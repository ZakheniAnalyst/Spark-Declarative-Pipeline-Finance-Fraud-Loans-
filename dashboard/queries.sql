-- Datasets behind the "Finance Risk Overview" AI/BI dashboard

-- kpis
SELECT count(*) AS transactions, round(sum(amount), 0) AS total_value,
       sum(CASE WHEN risk_band = 'high' THEN 1 ELSE 0 END) AS high_risk,
       sum(CASE WHEN risk_band IN ('medium','high') THEN 1 ELSE 0 END) AS medium_plus
FROM finance.lakeflow.gold_fct_transaction_risk;

-- monthly
SELECT date_trunc('month', date) AS month,
       sum(txn_count) AS transactions, sum(high_risk_count) AS high_risk
FROM finance.lakeflow.gold_agg_daily_activity GROUP BY 1 ORDER BY 1;

-- by_type
SELECT t.transaction_type, sum(a.txn_count) AS transactions
FROM finance.lakeflow.gold_agg_daily_activity a
JOIN finance.lakeflow.gold_dim_transaction_type t USING (transaction_type_id)
GROUP BY 1;

-- rule_hits
SELECT stack(6,
  'large_amount', sum(CAST(r_large_amount AS INT)),
  'self_transfer', sum(CAST(r_self_transfer AS INT)),
  'inactive_origin', sum(CAST(r_inactive_origin AS INT)),
  'before_account_open', sum(CAST(r_before_account_open AS INT)),
  'origin_burst', sum(CAST(r_origin_burst AS INT)),
  'future_date', sum(CAST(r_future_date AS INT))
) AS (rule, hits)
FROM finance.lakeflow.gold_fct_transaction_risk;

-- branches
SELECT * FROM finance.lakeflow.gold_agg_branch_risk ORDER BY avg_risk_score DESC LIMIT 10;

-- loans
SELECT * FROM finance.lakeflow.gold_agg_loan_portfolio;
