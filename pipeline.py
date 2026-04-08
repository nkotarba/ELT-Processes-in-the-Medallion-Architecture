import os
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"


def create_spark_session():
    return SparkSession.builder \
        .appName("Medallion Pipeline") \
        .config(
            "spark.jars.packages",
            "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3,"
            "net.snowflake:snowflake-jdbc:3.13.30"
        ) \
        .getOrCreate()


def read_from_snowflake(spark):
    baseOptions = {
        "sfURL": "XFQDXXB-VL91923.snowflakecomputing.com",
        "sfUser": "NKOTARBA",
        "sfPassword": "xyz",
        "sfDatabase": "DATA_PROJECT",
        "sfWarehouse": "COMPUTE_WH"
    }

    readOptions = {**baseOptions, "sfSchema": "SILVER"}

    df = spark.read \
        .format("snowflake") \
        .options(**readOptions) \
        .option("dbtable", "data") \
        .load()

    return df, baseOptions


def transform(df):
    return df.groupBy("payment_type").count()


def write_to_snowflake(df, baseOptions):
    writeOptions = {**baseOptions, "sfSchema": "GOLD"}

    df.write \
        .format("snowflake") \
        .options(**writeOptions) \
        .option("dbtable", "GOLD_SPARK") \
        .mode("overwrite") \
        .save()


def main():
    spark = create_spark_session()
    df, baseOptions = read_from_snowflake(spark)
    df_transformed = transform(df)
    write_to_snowflake(df_transformed, baseOptions)


if __name__ == "__main__":
    main()