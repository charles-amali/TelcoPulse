import sys
import datetime
import logging
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, count, col, from_json, trim, length
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.utils import AnalysisException
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame



def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


logger = setup_logger("GlueStreamingJob")



def initialize_glue_job() -> tuple:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])
    sc = SparkContext()
    sc.setLogLevel("INFO")
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args['JOB_NAME'], args)
    logger.info(f"Initialized job: {args['JOB_NAME']}")
    return args, glue_ctx, spark, job


def read_from_kinesis(glue_ctx: GlueContext) -> DataFrame:
    logger.info("Reading data from Kinesis stream...")

    spark = glue_ctx.spark_session


    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("operator", StringType(), True),
        StructField("network", StringType(), True),
        StructField("provider", StringType(), True),
        StructField("activity", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("signal", FloatType(), True),
        StructField("precision", FloatType(), True),
        StructField("status", StringType(), True)
    ])

    raw_df = spark.readStream.format("kinesis") \
        .option("streamName", "telco-network-metrics") \
        .option("endpointUrl", "https://kinesis.eu-west-1.amazonaws.com") \
        .option("region", "eu-west-1") \
        .option("startingPosition", "LATEST") \
        .load()

    logger.info("Kinesis stream read. Casting and parsing JSON...")

    json_df = raw_df.selectExpr("CAST(data AS STRING) as json_string")
    parsed_df = json_df.withColumn("parsed", from_json(col("json_string"), schema)).select("parsed.*")

    return parsed_df



def process_batch(batch_df, batch_id, glue_ctx: GlueContext, temp_dir: str, job_name: str):
    try:
        logger.info(f"Processing batch {batch_id} with {batch_df.count()} rows.")

        if batch_df.rdd.isEmpty():
            logger.warning(f"Batch {batch_id} is empty. Skipping.")
            return
        
        batch_df.printSchema()
        sample = batch_df.limit(10).collect()
        logger.info(f"Sample data from batch {batch_id}:\n{sample}")
        
        if "operator" not in batch_df.columns:
            logger.error(f"'operator' column not found in batch {batch_id}. Available columns: {batch_df.columns}")
            return
        
        batch_df.cache()
        logger.info(f"Processing batch {batch_id} with {batch_df.count()} rows.")
        
        cleaned_df = batch_df.dropna(subset=["operator", "postal_code", "signal", "precision", "status"]) \
        .filter((length(trim(col("postal_code"))) > 0))


        kpi_operator  = cleaned_df.groupBy("operator", "postal_code") \
          .agg(
            avg("signal").alias("avg_signal_strength"),
            avg("precision").alias("avg_precision")
        )

        status_count_per_postal = cleaned_df.groupBy("postal_code", "status").agg(
            count("status").alias("status_count")
        )

        def write_to_s3(df: DataFrame, table_name: str, now: datetime.datetime, glue_ctx: GlueContext, batch_id: int):
            s3_path = (
                f"s3://telco-pulse-135/processed-data/{table_name}/"
                f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
            )
            
            dynamic_frame = DynamicFrame.fromDF(df, glue_ctx, f"output_frame_{table_name}")
            
            glue_ctx.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="s3",
                format="glueparquet",
                connection_options={"path": s3_path, "partitionKeys": []},
                format_options={"compression": "snappy"},
                transformation_ctx=f"s3_output_{table_name}"
            )
            
            logger.info(f"Successfully wrote {table_name} for batch {batch_id} to {s3_path}")

        now = datetime.datetime.utcnow()
        write_to_s3(kpi_operator, "kpi-operator", now, glue_ctx, batch_id)
        write_to_s3(status_count_per_postal, "network-status", now, glue_ctx, batch_id)


    except AnalysisException as e:
        logger.error(f"Spark analysis error in batch {batch_id}: {e}")
    except Exception as ex:
        logger.error(f"Error processing batch {batch_id}: {str(ex)}", exc_info=True)



def main():
    args, glue_ctx, spark, job = initialize_glue_job()

    kinesis_stream = read_from_kinesis(glue_ctx)
    
    glue_ctx.forEachBatch(
        frame=kinesis_stream,
        batch_function=lambda df, batch_id: process_batch(df, batch_id, glue_ctx, args['TempDir'], args['JOB_NAME']),
        options={
            "windowSize": "30 seconds",
            "checkpointLocation": f"{args['TempDir']}/{args['JOB_NAME']}/checkpoints/"
        }
    )

    job.commit()
    logger.info("Glue job completed successfully.")


if __name__ == "__main__":
    main()




# aws ecr create-repository --repository-name telcopulse-dashboard

# aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 842676015206.dkr.ecr.eu-west-1.amazonaws.com

# docker tag telcopulse-dashboard:latest 842676015206.dkr.ecr.eu-west-1.amazonaws.com/dashboard/telcopulse-dashboard:latest
 
#  docker push 842676015206.dkr.ecr.eu-west-1.amazonaws.com/dashboard/telcopulse-dashboard:latest