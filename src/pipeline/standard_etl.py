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


@dataclass
class DeltaDataSet:
    name: str
    cur_data: DataFrame
    primary_keys: List[str]
    storage_path: str
    table_name: str
    data_type: str
    database: str
    partition: str
    skip_publish: bool = False
    replace_partition: bool = False


class InvalidDataException(Exception):
    pass


class StandardETL(ABC):
    def __init__(self,
                 storage_path: Optional[str] = None,
                 database: Optional[str] = None,
                 partition: Optional[str] = None,
                 ) -> None:
        self.STORAGE_PATH = storage_path
        self.DATABASE = database
        self.DEFAULT_PARTITION = partition or datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    def run_validation(self, input_datasets: Dict[str, DeltaDataSet]):
        context = gx.get_context(
            context_root_dir=os.path.join(
                os.getcwd(),
                "src",
                "great_expectations",
            )
        )

        validations = []
        for input_dataset in input_datasets.values():
            validations.append(
                {
                    "batch_request": context.get_datasource("spark_datasource")
                    .get_asset(input_dataset.name)
                    .build_batch_request(dataframe=input_dataset.cur_data),
                    "expectation_suite_name": input_dataset.name,
                }
            )
        return context.run_checkpoint(
            checkpoint_name="dq_checkpoint", validations=validations
        ).list_validation_results()

    def validate(self, input_datasets: Dict[str, DeltaDataSet]):
        results = {}
        for validation in self.run_validation(input_datasets):
            results[
                validation.get('meta').get('expectation_suite_name')
            ] = validation.get('sucess')
        for k, v in results.items():
            raise InvalidDataException(
                f"{k} dataset did not pass"
            )
        return True

    def check_required_inputs(self, input_datasets: Dict[str, DeltaDataSet], required_ds: List[str]):
        if not all([ds in input_datasets for ds in required_ds]):
            raise ValueError(
                f"Input datasets {input_datasets.keys()} does not contain"
                f" {required_ds}"
            )

    def construct_join_string(self, keys: List[str]):
        return ' AND '.join([f"target.{key} = source.{key}" for key in keys])

    def publish_data(self, input_datasets: Dict[str, DeltaDataSet], spark: SparkSession, **kwargs):
        for input_dataset in input_datasets.values():
            if not input_dataset.skip_publish:
                curr_data = input_dataset.curr_data.withColumn(
                    'etl_inserted', current_timestamp()
                ).withColumn('partition', lit(input_dataset.partition))
                if input_dataset.replace_partition:
                    curr_data.write.format("delta").mode("overwrite").option(
                        "replaceWhere",
                        f"partition = '{input_dataset.partition}'"
                    ).save(input_dataset.storage_path)
                else:
                    targetDF = DeltaDataSet.forPath(
                        spark, input_dataset.storage_path
                    )
                    (
                        targetDF.alias("target").merge(
                            curr_data.alias("source"),
                            self.construct_join_string(input_dataset.primary_keys)
                        ),
                    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    @abstractmethod
    def get_bronze_datasets(self, spark: SparkSession, **kwargs) -> Dict[str, DeltaDataSet]:
        pass

    @abstractmethod
    def get_silver_datasets(self, input_datasets: Dict[str, DeltaDataSet], spark: SparkSession, **kwargs) -> Dict[
        str, DeltaDataSet]:
        pass

    @abstractmethod
    def get_gold_datasets(self, input_datasets: Dict[str, DeltaDataSet], spark: SparkSession, **kwargs) -> Dict[
        str, DeltaDataSet]:
        pass

    @abstractmethod
    def run(self, spark: SparkSession, **kwargs):
        partition = kwargs.get('partition')
        bronze_datasets = self.get_bronze_datasets(spark, partition=partition)
        self.validate(bronze_datasets)
        self.publish_data(bronze_datasets, spark)
        logging.info(
            'Created clean bronze datasets'
            f' {[ds for ds in bronze_datasets.keys()]}'
        )
        silver_datasets = self.get_silver_datasets(bronze_datasets, spark, partition=partition)
        self.validate(silver_datasets)
        self.publish_data(silver_datasets, spark)
        logging.info(
            'Created clean silver datasets'
            f' {[ds for ds in silver_datasets.keys()]}'
        )
        gold_datasets = self.get_gold_datasets(
            silver_datasets, spark, partition=partition
        )
        self.validate(gold_datasets)
        self.publish_data(gold_datasets, spark)
        logging.info(
            'Created clean gold datasets'
            f' {[ds for ds in gold_datasets.keys()]}'
        )
