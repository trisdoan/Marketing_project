from pyspark.sql import SparkSession


def create_tbl(
        spark,
        path="s3a://src/delta",
        database: str = "marketing_db"
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"DROP TABLE IF EXISTS {database}.dim_customer")
    spark.sql(
        f"""
            CREATE TABLE {database}.dim_customer (
                id INT,
                customer_surrogate_id STRING,
                first_name STRING,
                last_name STRING,
                state_id STRING,
                datetime_created TIMESTAMP,
                datetime_updated TIMESTAMP,
                current boolean,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP,
                partition STRING
            ) USING DELTA LOCATION '{path}/dim_customer'
        """
    )
    spark.sql(f"DROP TABLE IF EXISTS {database}.fct_orders")
    spark.sql(
        f"""
            CREATE TABLE {database}.fct_orders (
                order_id STRING,
                customer_id INT,
                item_id STRING,
                item_name STRING,
                delivered_on TIMESTAMP,
                datetime_order_placed TIMESTAMP,
                customer_surrogate_id STRING,
                etl_inserted TIMESTAMP,
                partition STRING
            ) USING DELTA PARTITIONED BY (partition) LOCATION '{path}/fct_orders'
        """
    )


def drop_tbl(spark,
             database: str = "marketing_db"):
    spark.sql(f"DROP TABLE IF EXISTS {database}.dim_customer")
    spark.sql(f"DROP TABLE IF EXISTS {database}.fct_orders")
    spark.sql(f"DROP DATABASE IF EXISTS {database}")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("{database}_ddl")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .enableHiveSupport()
        .getOrCreate()
    )
    create_tbl(spark)
