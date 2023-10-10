from src.ddl.silver_tbl import create_tbl, drop_tbl
from src.pipeline.marketing_etl import MarketingETL

class TestSalesMarketing:
    def test_get_validate_publish_silver_datasets(self, spark):
        sm = MarketingETL("/tmp/delta", database="test_db")
        bronze_datasets = sm.get_bronze_datasets(
            spark, "YYYY-MM-DD_HH"
        )
        create_tbl(
            spark, path="/tmp/delta", database="test_db"
        )
        silver_datasets = sm.get_silver_datasets(bronze_datasets, spark)
        drop_tbl(spark, database="test_db")
        assert sorted(silver_datasets.keys()) == ['dim_customer', 'fct_orders']