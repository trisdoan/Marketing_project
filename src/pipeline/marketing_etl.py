import logging
import os
import random
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import great_expectations as gx
from delta.tables import DeltaTable
from faker import Faker
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.pipeline.standard_etl import StandardETL, DeltaDataSet
from src.datagenerator.fake_data import generate_data


class MarketingETL(StandardETL):
    def get_bronze_datasets(self, spark: SparkSession, **kwargs):
        customer_df, orders_df = generate_data(spark)
        return {
            'customer': DeltaDataSet(
                name='customer',
                cur_data=customer_df,
                primary_keys=['id', 'partition'],
                storage_path=f'{self.STORAGE_PATH}/customer',
                table_name='customer',
                data_type='delta',
                database=f'{self.DATABASE}',
                partition=kwargs.get('partition', self.DEFAULT_PARTITION),
                replace_partition=True,
            ),
            'orders': DeltaDataSet(
                name='orders',
                cur_data=orders_df,
                primary_keys=['id', 'partition'],
                storage_path=f'{self.STORAGE_PATH}/orders',
                table_name='orders',
                data_type='delta',
                database=f'{self.DATABASE}',
                partition=kwargs.get('partition', self.DEFAULT_PARTITION),
                replace_partition=True,
            ),
        }

    def get_dim_customer(self, customer: DeltaDataSet, spark: SparkSession, **kwargs):
        customer_df = customer.cur_data
        dim_customer = kwargs['dim_customer']

        customer_df = customer_df.withColumn(
            'customer_surrogate_id',
            expr('md5(concat(id, datetime_updated))')
        )
        # only get latest customer
        latest_dim_customer = dim_customer.where('current= true')
        customer_df_insert_new = (
            customer_df.join(
                latest_dim_customer,
                (customer_df.id == latest_dim_customer.id)
                & (
                        latest_dim_customer.datetime_updated < customer_datetime_updated
                ),
                'leftanti'
            ).select(
                customer_df.id,
                customer_df.customer_surrogate_id,
                customer_df.first_name,
                customer_df.last_name,
                customer_df.state_id,
                customer_df.datetime_created,
                customer_df.datetime_updated,
            ).withColumn('current', lit(True)).withColumn('valid_from', customer_df.datetime_updated).withColumn(
                'valid_to', lit('2099-01-01 12:00:00.0000'))
        )

        customer_df_insert_existing_ids = (
            customer_df.join(
                latest_dim_customer,
                (customer_df.id == latest_dim_customer.id)
                & (
                        latest_dim_customer.datetime_updated < customer_datetime_updated
                ),
            ).select(
                customer_df.id,
                customer_df.customer_surrogate_id,
                customer_df.first_name,
                customer_df.last_name,
                customer_df.state_id,
                customer_df.datetime_created,
                customer_df.datetime_updated,
            )
            .withColumn('current', lit(True))
            .withColumn('valid_from', customer_df.datetime_updated)
            .withColumn('valid_to', lit('2099-01-01 12:00:00.0000'))
        )
        # get rows to be updated
        customer_df_ids_update = (
            latest_dim_customer.join(
                customer_df,
                (latest_dim_customer.id == customer_df.id)
                & (
                        latest_dim_customer.datetime_updated
                        < customer_df.datetime_updated
                ),
            ).select(
                dim_customer_latest.id,
                dim_customer_latest.customer_surrogate_id,
                dim_customer_latest.first_name,
                dim_customer_latest.last_name,
                dim_customer_latest.state_id,
                dim_customer_latest.datetime_created,
                customer_df.datetime_updated,
                dim_customer_latest.valid_from,
            ).withColumn('current', lit(False)).withColumn('valid_to', customer_df.datetime_updated)
        )
        return customer_df_insert_new.uionByName(
            customer_df_insert_existing_ids
        ).unionByName(customer_df_ids_update)

    def get_fct_orders(self, input_datasets: Dict[str, DeltaDataSet], spark: SparkSession, **kwargs):
        dim_customer = input_datasets['dim_customer'].cur_data
        orders_df = input_datasets['orders'].cur_data

        dim_customer_cur_df = dim_customer.where('current=true')
        return orders_df.join(
            dim_customer_cur_df,
            orders_df.customer_id == dim_customer_cur_df.id,
            "left"
        ).select(
            orders_df.order_id,
            orders_df.customer_id,
            orders_df.item_id,
            orders_df.item_name,
            orders_df.delivered_on,
            orders_df.datetime_order_placed,
            dim_customer_curr_df.customer_surrogate_id,
        )

    def get_silver_datasets(self, input_datasets: Dict[str, DeltaDataSet], spark: SparkSession, **kwargs):
        self.check_required_inputs(input_datasets, ['customer', 'orders'])
        dim_customer_df = self.get_dim_customer(
            input_datasets['customer'],
            spark,
            dim_customer=spark.read.table(f'{self.DATABASE}.dim_customer'),
        )

        silver_datasets = {}
        silver_datasets['dim_customer'] = DeltaDataSet(
            name='dim_customer',
            cur_data=dim_customer_df,
            primary_keys=["customer_surrogate_id"],
            storage_path=f'{self.STORAGE_PATH}/dim_customer',
            table_name='dim_customer',
            data_type='delta',
            database=f'{self.DATABASE}',
            partition=kwargs.get('partition', self.DEFAULT_PARTITION),
        )
        self.publish_data(silver_datasets, spark)
        silver_datasets['dim_customer'].cur_data = spark.read.table(
            f'{self.DATABASE}.dim_customer'
        )
        silver_datasets['dim_customer'].skip_publish = True
        input_datasets['dim_customer'] = silver_datasets['dim_customer']

        silver_datasets['fct_orders'] = DeltaDataSet(
            name='fct_orders',
            cur_data=self.get_fct_orders(input_datasets, spark),
            primary_keys=["order_id"],
            storage_path=f'{self.STORAGE_PATH}/fct_orders',
            table_name='fct_orders',
            data_type='delta',
            database=f'{self.DATABASE}',
            partition=kwargs.get('partition', self.DEFAULT_PARTITION),
            replace_partition=True,
        )
        return silver_datasets

    def get_sales_mart(self, input_datasets: Dict[str, DeltaDataSet], spark: SparkSession, **kwarg):
        dim_customer = (
            input_datasets['dim_customer'].cur_data.where("current=true").select("customer_surrogate_id", "state_id")
        )
        fct_orders = (
            input_datasets["fct_orders"].cur_data
        )
        return (
            fct_orders.alias("fct_orders").join(
                dim_customer.alias("dim_customer"),
                fct_orders.customer_surrogate_id == dim_customer.customer_surrogate_id,
                "left"
            ).select(
                expr('to_date(fct_orders.delivered_on, "yyyy-dd-mm")').alias(
                    "deliver_date"
                ),
                col("dim_customer.state_id").alias("state_id"),
            ).groupBy("deliver_date", "state_id").count().withColumnRenamed("count", "num_orders")
        )

    def get_gold_datasets(self, input_datasets: Dict[str, DeltaDataSet], spark: SparkSession, **kwarg):
        self.check_required_inputs(
            input_datasets, ["dim_customer", "fct_orders"]
        )
        sales_mart_df = self.get_sales_mart(input_datasets)
        return {
            'sales_mart': DeltaDataSet(
                name='sales_mart',
                cur_data=sales_mart_df,
                primary_keys=['deliver_date', 'state_id', 'partition'],
                storage_path=f'{self.STORAGE_PATH}/sales_mart',
                table_name='sales_mart',
                data_type='delta',
                database=f'{self.DATABASE}',
                partition=kwargs.get('partition', self.DEFAULT_PARTITION),
                replace_partition=True,
            )
        }


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("marketing")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    sm = MarketingETL()
    partition = datetime.now().strftime(
        "%Y-%m-%d-%H-%M"
    )
    sm.run(spark, partition=partition)
    spark.stop()
