WITH stg_stripe__payments AS (
  SELECT
    *
  FROM {{ ref('rapid_onboarding_exemplar', 'stg_stripe__payments') }}
), aggregation AS (
  SELECT
    PAYMENT_METHOD,
    STATUS,
    SUM(AMOUNT) AS sum_AMOUNT
  FROM stg_stripe__payments
  GROUP BY
    PAYMENT_METHOD,
    STATUS
), untitled_2_sql AS (
  SELECT
    *
  FROM aggregation
)
SELECT
  *
FROM untitled_2_sql