"""Silver layer: typed, de-duplicated, standardised, with data-quality expectations.

Same logic as the 03_silver notebook, wrapped as pipeline tables.

Policy
  * expect_all           -> soft rules: row kept, violations counted in the event log
  * expect_all_or_drop   -> hard rules: row dropped (transactions also go to quarantine)
"""
from pyspark import pipelines as dp
from pyspark.sql import Window, functions as F

# Dates arrive in mixed formats on purpose. First format that parses wins, so order matters.
TS_FORMATS = [
    "yyyy-MM-dd HH:mm:ss.SSSSSS", "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd",              # must come before yyyy-dd-MM
    "yyyy/MM/dd",
    "dd/MM/yyyy", "MM/dd/yyyy",
    "dd.MM.yyyy", "MM.dd.yyyy",
    "dd-MM-yyyy",
    "yyyy-dd-MM",              # last resort: swapped day/month
]


def parse_ts(c: str):
    # '2020-01-01T00:00:00' -> '2020-01-01 00:00:00', then try each format in order
    s = f"regexp_replace(trim(`{c}`), '(?<=[0-9])T(?=[0-9])', ' ')"
    return F.coalesce(*[F.expr(f"try_to_timestamp({s}, '{f}')") for f in TS_FORMATS])


def clean_text(c: str):
    """Trim; blank -> NULL."""
    return F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))


def to_int(c: str):
    # try_cast: serverless runs with ANSI on, so a plain cast would fail the whole update
    return F.expr(f"try_cast(trim(`{c}`) AS BIGINT)")


def to_num(c: str):
    return F.expr(f"try_cast(regexp_replace(trim(`{c}`), '[^0-9.\\\\-]', '') AS DOUBLE)")


def dedupe(df, key: str):
    w = Window.partitionBy(key).orderBy(F.col("_ingested_at").desc(), F.col("_source_file"))
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


# ---------------------------------------------------------------- lookups
LOOKUPS = [
    ("customer_types",    "CustomerTypeID",    "customer_type_id",    "TypeName",   "customer_type"),
    ("account_types",     "AccountTypeID",     "account_type_id",     "TypeName",   "account_type"),
    ("account_statuses",  "AccountStatusID",   "account_status_id",   "StatusName", "account_status"),
    ("loan_statuses",     "LoanStatusID",      "loan_status_id",      "StatusName", "loan_status"),
    ("transaction_types", "TransactionTypeID", "transaction_type_id", "TypeName",   "transaction_type"),
]


def make_lookup(name, src_id, id_, src_label, label):
    @dp.materialized_view(name=f"silver_{name}", comment=f"Cleaned {name} lookup")
    def _lookup():
        b = spark.read.table(f"bronze_{name}")
        df = b.select(
            to_int(src_id).alias(id_), clean_text(src_label).alias(label),
            "_ingested_at", "_source_file",
        ).where(f"{id_} IS NOT NULL")
        return dedupe(df, id_).drop("_ingested_at", "_source_file")

    return _lookup


for _spec in LOOKUPS:
    make_lookup(*_spec)


# ---------------------------------------------------------------- addresses
@dp.materialized_view(name="silver_addresses", comment="Cleaned addresses; country typos repaired")
@dp.expect_all({
    "country_present": "country IS NOT NULL",
    "country_is_us": "country IS NULL OR country = 'United States'",
    "city_present": "city IS NOT NULL",
})
@dp.expect_all_or_drop({"address_id_present": "address_id IS NOT NULL"})
def silver_addresses():
    b = spark.read.table("bronze_addresses")
    df = b.select(
        to_int("AddressID").alias("address_id"),
        clean_text("Street").alias("street"),
        clean_text("City").alias("city"),
        clean_text("Country").alias("country_raw"),
        "_ingested_at", "_source_file",
    ).where("address_id IS NOT NULL")
    # 'Unitd States', 'United StateR', ... are within 2 edits of the real value
    df = df.withColumn(
        "country",
        F.when(F.levenshtein("country_raw", F.lit("United States")) <= 2, F.lit("United States"))
        .otherwise(F.col("country_raw")),
    )
    return dedupe(df, "address_id").drop("country_raw", "_ingested_at", "_source_file")


# ---------------------------------------------------------------- branches
@dp.materialized_view(name="silver_branches", comment="Cleaned branches")
@dp.expect_all_or_drop({"branch_id_present": "branch_id IS NOT NULL"})
def silver_branches():
    b = spark.read.table("bronze_branches")
    df = b.select(
        to_int("BranchID").alias("branch_id"),
        clean_text("BranchName").alias("branch_name"),
        to_int("AddressID").alias("address_id"),
        "_ingested_at", "_source_file",
    ).where("branch_id IS NOT NULL")
    return dedupe(df, "branch_id").drop("_ingested_at", "_source_file")


# ---------------------------------------------------------------- customers
@dp.materialized_view(name="silver_customers", comment="Cleaned, de-duplicated customers")
@dp.expect_all({
    "has_first_name": "first_name IS NOT NULL",
    "has_last_name": "last_name IS NOT NULL",
    "dob_present": "date_of_birth IS NOT NULL",
    "dob_plausible": "date_of_birth IS NULL OR (date_of_birth < current_date() AND date_of_birth > DATE'1900-01-01')",
    "customer_type_known": "customer_type_id IN (1, 2, 3)",
})
@dp.expect_all_or_drop({"customer_id_present": "customer_id IS NOT NULL"})
def silver_customers():
    b = spark.read.table("bronze_customers")
    df = b.select(
        to_int("CustomerID").alias("customer_id"),
        F.initcap(clean_text("FirstName")).alias("first_name"),
        F.initcap(clean_text("LastName")).alias("last_name"),
        parse_ts("DateOfBirth").cast("date").alias("date_of_birth"),
        to_int("AddressID").alias("address_id"),
        to_int("CustomerTypeID").alias("customer_type_id"),
        "_ingested_at", "_source_file",
    ).where("customer_id IS NOT NULL")
    return dedupe(df, "customer_id").drop("_ingested_at", "_source_file")


# ---------------------------------------------------------------- accounts
@dp.materialized_view(name="silver_accounts", comment="Cleaned, de-duplicated accounts")
@dp.expect_all({
    "opening_date_present": "opening_date IS NOT NULL",
    "opening_not_future": "opening_date IS NULL OR opening_date <= current_date()",
    "balance_present": "balance IS NOT NULL",
    "account_type_known": "account_type_id IN (1, 2, 3, 4, 5)",
    "account_status_known": "account_status_id IN (1, 2, 3)",
})
@dp.expect_all_or_drop({"account_id_present": "account_id IS NOT NULL"})
def silver_accounts():
    b = spark.read.table("bronze_accounts")
    df = b.select(
        to_int("AccountID").alias("account_id"),
        to_int("CustomerID").alias("customer_id"),
        to_int("AccountTypeID").alias("account_type_id"),
        to_int("AccountStatusID").alias("account_status_id"),
        to_num("Balance").alias("balance"),
        parse_ts("OpeningDate").cast("date").alias("opening_date"),
        "_ingested_at", "_source_file",
    ).where("account_id IS NOT NULL")
    return dedupe(df, "account_id").drop("_ingested_at", "_source_file")


# ---------------------------------------------------------------- loans
@dp.materialized_view(name="silver_loans", comment="Cleaned, de-duplicated loans")
@dp.expect_all({
    "start_present": "start_date IS NOT NULL",
    "end_present": "est_end_date IS NOT NULL",
    "start_not_future": "start_date IS NULL OR start_date <= current_date()",
    "end_after_start": "start_date IS NULL OR est_end_date IS NULL OR est_end_date >= start_date",
    "interest_rate_sane": "interest_rate IS NULL OR interest_rate BETWEEN 0 AND 1",
    "loan_status_known": "loan_status_id IN (1, 2, 3)",
})
@dp.expect_all_or_drop({
    "loan_id_present": "loan_id IS NOT NULL",
    "principal_positive": "principal_amount > 0",
})
def silver_loans():
    b = spark.read.table("bronze_loans")
    df = b.select(
        to_int("LoanID").alias("loan_id"),
        to_int("AccountID").alias("account_id"),
        to_int("LoanStatusID").alias("loan_status_id"),
        to_num("PrincipalAmount").alias("principal_amount"),
        to_num("InterestRate").alias("interest_rate"),
        parse_ts("StartDate").cast("date").alias("start_date"),
        parse_ts("EstimatedEndDate").cast("date").alias("est_end_date"),
        "_ingested_at", "_source_file",
    ).where("loan_id IS NOT NULL")
    return dedupe(df, "loan_id").drop("_ingested_at", "_source_file")


# ---------------------------------------------------------------- transactions
TXN_HARD_RULES = {
    "transaction_id_present": "transaction_id IS NOT NULL",
    "amount_positive": "amount IS NOT NULL AND amount > 0",
    "type_known": "transaction_type_id IN (1, 2, 3, 4)",
    "origin_present": "account_origin_id IS NOT NULL",
}
TXN_SOFT_RULES = {
    "date_present": "transaction_ts IS NOT NULL",
    "date_not_future": "transaction_ts IS NULL OR transaction_ts <= current_timestamp()",
    "branch_present": "branch_id IS NOT NULL",
    "destination_present": "account_destination_id IS NOT NULL",
}


def _txn_base():
    b = spark.read.table("bronze_transactions")
    return b.select(
        to_int("TransactionID").alias("transaction_id"),
        to_int("AccountOriginID").alias("account_origin_id"),
        to_int("AccountDestinationID").alias("account_destination_id"),
        to_int("TransactionTypeID").alias("transaction_type_id"),
        to_num("Amount").alias("amount"),
        parse_ts("TransactionDate").alias("transaction_ts"),
        to_int("BranchID").alias("branch_id"),
        clean_text("Description").alias("description"),
        "_ingested_at", "_source_file",
    )


@dp.materialized_view(name="silver_transactions", comment="Valid, de-duplicated transactions")
@dp.expect_all(TXN_SOFT_RULES)
@dp.expect_all_or_drop(TXN_HARD_RULES)
def silver_transactions():
    return dedupe(_txn_base().where("transaction_id IS NOT NULL"), "transaction_id").drop(
        "_ingested_at", "_source_file"
    )


@dp.materialized_view(
    name="silver_transactions_quarantine",
    comment="Rows that broke a hard rule, with the reason, for review instead of silent loss",
)
def silver_transactions_quarantine():
    # NULL rule result counts as a failure, matching how expectations treat it
    failed = [
        F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(name))
        for name, rule in TXN_HARD_RULES.items()
    ]
    return (
        _txn_base()
        .withColumn("failed_rules", F.array_compact(F.array(*failed)))
        .where(F.size("failed_rules") > 0)
        .drop("_ingested_at")
    )
