"""Bronze layer: raw CSVs -> streaming tables via Auto Loader.

Everything lands as STRING exactly as received, plus lineage columns. No cleaning here:
if a downstream rule is wrong we can always rebuild silver from bronze.

In a pipeline, Auto Loader needs no schemaLocation or checkpointLocation: the pipeline
manages both for each streaming table.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Pipeline setting `raw_path` (Settings > Configuration). Falls back to the default below.
RAW_PATH = spark.conf.get("raw_path", "/Volumes/finance/raw/volume")

SOURCE_TABLES = [
    "customers", "accounts", "loans", "transactions", "branches", "addresses",
    "customer_types", "account_types", "account_statuses",
    "loan_statuses", "transaction_types",
]


def make_bronze(name: str):
    @dp.table(
        name=f"bronze_{name}",
        comment=f"Raw {name}.csv ingested with Auto Loader. All columns are strings.",
        table_properties={"quality": "bronze"},
    )
    def _bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "false")  # keep dirty values as text
            .load(f"{RAW_PATH}/{name}/")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )

    return _bronze


for _name in SOURCE_TABLES:
    make_bronze(_name)
