{{
    config(
        materialized='incremental',
        unique_key='subscription_month_sk',
        incremental_strategy='merge',
        dist='subscriber_id',
        sort=['activity_month', 'brand_code', 'cohort_month'],
        contract={'enforced': true},
        tags=['adl', 'subscriptions', 'mrr', 'commercial']
    )
}}

/*
    Mart: fct_subscription_mrr_retention
    Domain: Subscription Data Product (894K+ Active Subscriptions | £220K+ Monthly Recurring Revenue)
    Grain: One row per subscriber per calendar month (subscriber_id + activity_month)
    Purpose: Tracks Monthly Recurring Revenue (MRR), net revenue retention, churn movements
             (New, Expansion, Contraction, Churned, Reactivated), and LTV cohorts.
*/

WITH monthly_ledger AS (
    SELECT
        subscriber_id,
        subscription_plan_id,
        brand_code,
        CAST(DATE_TRUNC('month', billing_date) AS DATE) AS activity_month,
        CAST(DATE_TRUNC('month', subscription_start_date) AS DATE) AS cohort_month,
        SUM(net_billed_amount_gbp) AS current_mrr_gbp,
        MAX(billing_status) AS billing_status,
        MAX(updated_at) AS updated_at
    FROM {{ ref('stg_subscription_billing_events') }}
    {% if is_incremental() %}
        WHERE billing_date >= DATEADD(month, -2, DATE_TRUNC('month', CURRENT_DATE))
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5
),

with_prior_month AS (
    SELECT
        MD5(subscriber_id || '|' || CAST(activity_month AS VARCHAR)) AS subscription_month_sk,
        subscriber_id,
        subscription_plan_id,
        brand_code,
        activity_month,
        cohort_month,
        DATEDIFF(month, cohort_month, activity_month) AS cohort_tenure_months,
        current_mrr_gbp,
        COALESCE(
            LAG(current_mrr_gbp) OVER (
                PARTITION BY subscriber_id
                ORDER BY activity_month
            ),
            0.00
        ) AS prior_mrr_gbp,
        billing_status
    FROM monthly_ledger
)

SELECT
    subscription_month_sk,
    subscriber_id,
    subscription_plan_id,
    brand_code,
    activity_month,
    cohort_month,
    cohort_tenure_months,
    current_mrr_gbp,
    prior_mrr_gbp,
    ROUND(current_mrr_gbp - prior_mrr_gbp, 2) AS net_mrr_movement_gbp,
    CASE
        WHEN cohort_tenure_months = 0 AND current_mrr_gbp > 0 THEN 'NEW'
        WHEN prior_mrr_gbp = 0 AND current_mrr_gbp > 0 AND cohort_tenure_months > 0 THEN 'REACTIVATED'
        WHEN current_mrr_gbp > prior_mrr_gbp AND prior_mrr_gbp > 0 THEN 'EXPANSION'
        WHEN current_mrr_gbp < prior_mrr_gbp AND current_mrr_gbp > 0 THEN 'CONTRACTION'
        WHEN current_mrr_gbp = 0 AND prior_mrr_gbp > 0 THEN 'CHURNED'
        ELSE 'RETAINED'
    END AS mrr_movement_type,
    (current_mrr_gbp > 0) AS is_active_subscriber,
    CURRENT_TIMESTAMP AS _dbt_loaded_at
FROM with_prior_month
