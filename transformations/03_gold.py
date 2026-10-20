"""Gold layer: star schema + rule-based risk scoring + dashboard aggregates.

NOTE: the dataset has NO fraud label (the readme mentions `is_fraud` but the file has no
such column). `gold_fct_transaction_risk` is therefore an anomaly/rules model, not a
supervised fraud detector. Thresholds are pipeline settings so they can be tuned.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window, functions as F

BURST_THRESHOLD = int(spark.conf.get("burst_threshold", "3"))          # txns per origin per day
LARGE_AMOUNT_PCTL = float(spark.conf.get("large_amount_percentile", "0.99"))


def _addr():
    return spark.read.table("silver_addresses").select("address_id", "city", "country")


# ------------------------------------------------------------------ dimensions
@dp.materialized_view(name="gold_dim_customer", comment="Customer dimension")
def gold_dim_customer():
    return (
        spark.read.table("silver_customers")
        .join(spark.read.table("silver_customer_types"), "customer_type_id", "left")
        .join(_addr(), "address_id", "left")
        .select(
            "customer_id", "first_name", "last_name", "date_of_birth",
            F.floor(F.months_between(F.current_date(), F.col("date_of_birth")) / 12).cast("int").alias("age"),
            "customer_type_id", "customer_type", "city", "country",
        )
    )


@dp.materialized_view(name="gold_dim_account", comment="Account dimension")
def gold_dim_account():
    return (
        spark.read.table("silver_accounts")
        .join(spark.read.table("silver_account_types"), "account_type_id", "left")
        .join(spark.read.table("silver_account_statuses"), "account_status_id", "left")
        .select(
            "account_id", "customer_id", "account_type_id", "account_type",
            "account_status_id", "account_status", "balance", "opening_date",
        )
    )


@dp.materialized_view(name="gold_dim_branch", comment="Branch dimension")
def gold_dim_branch():
    return (
        spark.read.table("silver_branches")
        .join(_addr(), "address_id", "left")
        .select("branch_id", "branch_name", "city", "country")
    )


@dp.materialized_view(name="gold_dim_transaction_type", comment="Transaction type dimension")
def gold_dim_transaction_type():
    return spark.read.table("silver_transaction_types")


@dp.materialized_view(name="gold_dim_date", comment="Calendar spanning the transaction and loan dates")
def gold_dim_date():
    txn = spark.read.table("silver_transactions").select(F.col("transaction_ts").cast("date").alias("d"))
    loans = spark.read.table("silver_loans").select(F.col("start_date").alias("d"))
    bounds = txn.unionByName(loans).where("d IS NOT NULL").agg(F.min("d").alias("lo"), F.max("d").alias("hi"))
    return bounds.select(F.explode(F.sequence("lo", "hi", F.expr("interval 1 day"))).alias("date")).select(
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMM").alias("month_name"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


# ------------------------------------------------------------------ facts
@dp.materialized_view(name="gold_fct_transactions", comment="One row per valid transaction")
def gold_fct_transactions():
    return spark.read.table("silver_transactions").select(
        "transaction_id", "account_origin_id", "account_destination_id", "transaction_type_id",
        "branch_id", "amount", "transaction_ts",
        F.col("transaction_ts").cast("date").alias("transaction_date"),
        F.hour("transaction_ts").alias("transaction_hour"),
    )


@dp.materialized_view(name="gold_fct_loans", comment="One row per loan with derived measures")
def gold_fct_loans():
    return (
        spark.read.table("silver_loans")
        .join(spark.read.table("silver_loan_statuses"), "loan_status_id", "left")
        .join(spark.read.table("silver_accounts").select("account_id", "customer_id"), "account_id", "left")
        .select(
            "loan_id", "account_id", "customer_id", "loan_status_id", "loan_status",
            "principal_amount", "interest_rate", "start_date", "est_end_date",
            F.datediff("est_end_date", "start_date").alias("term_days"),
            (F.col("loan_status_id") == 3).alias("is_overdue"),
        )
    )


# ------------------------------------------------------------------ risk rules
@dp.materialized_view(
    name="gold_fct_transaction_risk",
    comment="Rule-based anomaly score per transaction (no fraud label exists in the source)",
)
def gold_fct_transaction_risk():
    t = spark.read.table("silver_transactions")
    acc = spark.read.table("silver_accounts").select(
        F.col("account_id").alias("o_id"),
        F.col("account_status_id").alias("o_status"),
        F.col("opening_date").alias("o_open"),
    )
    p99 = t.groupBy("transaction_type_id").agg(F.percentile_approx("amount", LARGE_AMOUNT_PCTL).alias("p99"))
    day_count = F.count("*").over(Window.partitionBy("account_origin_id", F.col("transaction_ts").cast("date")))

    df = (
        t.join(acc, t.account_origin_id == acc.o_id, "left")
        .join(p99, "transaction_type_id", "left")
        .withColumn("r_large_amount", F.col("amount") >= F.col("p99"))
        .withColumn("r_self_transfer", F.col("account_origin_id") == F.col("account_destination_id"))
        .withColumn("r_inactive_origin", F.col("o_status").isin(2, 3))
        .withColumn("r_before_account_open", F.col("transaction_ts").cast("date") < F.col("o_open"))
        # missing dates would otherwise all land in one group and look like a burst
        .withColumn("r_origin_burst", F.when(F.col("transaction_ts").isNotNull(), day_count >= BURST_THRESHOLD))
        .withColumn("r_future_date", F.col("transaction_ts") > F.current_timestamp())
    )

    weights = {
        "r_large_amount": 3, "r_inactive_origin": 2, "r_self_transfer": 2, "r_future_date": 2,
        "r_before_account_open": 1,  # ~13% of rows fire on this synthetic data, so keep it weak
        "r_origin_burst": 1,
    }
    score = sum(F.coalesce(F.col(c), F.lit(False)).cast("int") * w for c, w in weights.items())
    df = df.withColumn("risk_score", score).withColumn(
        "risk_band",
        F.when(F.col("risk_score") >= 5, "high")
        .when(F.col("risk_score") >= 3, "medium")
        .when(F.col("risk_score") >= 1, "low")
        .otherwise("none"),
    )
    return df.select(
        "transaction_id", "account_origin_id", "account_destination_id", "branch_id",
        "transaction_type_id", "amount", "transaction_ts", *weights.keys(), "risk_score", "risk_band",
    )


# ------------------------------------------------------------------ aggregates for dashboards
@dp.materialized_view(name="gold_agg_daily_activity", comment="Daily volume, value and risk mix")
def gold_agg_daily_activity():
    r = spark.read.table("gold_fct_transaction_risk")
    return (
        r.where("transaction_ts IS NOT NULL")
        .groupBy(F.col("transaction_ts").cast("date").alias("date"), "transaction_type_id")
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount"), 2).alias("txn_value"),
            F.sum((F.col("risk_band") == "high").cast("int")).alias("high_risk_count"),
            F.sum(F.col("risk_band").isin("medium", "high").cast("int")).alias("medium_plus_count"),
        )
    )


@dp.materialized_view(name="gold_agg_branch_risk", comment="Risk profile by branch")
def gold_agg_branch_risk():
    r = spark.read.table("gold_fct_transaction_risk")
    return (
        r.groupBy("branch_id")
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount"), 2).alias("txn_value"),
            F.round(F.avg("risk_score"), 3).alias("avg_risk_score"),
            F.sum((F.col("risk_band") == "high").cast("int")).alias("high_risk_count"),
        )
        .join(spark.read.table("gold_dim_branch"), "branch_id", "left")
    )


@dp.materialized_view(name="gold_agg_loan_portfolio", comment="Loan book by status")
def gold_agg_loan_portfolio():
    return spark.read.table("gold_fct_loans").groupBy("loan_status").agg(
        F.count("*").alias("loan_count"),
        F.round(F.sum("principal_amount"), 2).alias("total_principal"),
        F.round(F.avg("interest_rate"), 4).alias("avg_interest_rate"),
        F.round(F.avg("term_days") / 365.25, 2).alias("avg_term_years"),
    )
