from src.pipeline.marketing_etl import MarketingETL

class TestSalesMart:
    def test_get_bronze_datasets(self, spark):
        sm = MarketingETL()
        bronze_datasets = sm.get_bronze_datasets(
            spark,
            partition="YYYY-MM-DD_HH"
        )
        # check if it return customer_df and orders_df
        assert len(bronze_datasets) == 2

        # check if it returns in correct orders
        assert sorted(bronze_datasets.keys()) == ['customer', 'orders']


