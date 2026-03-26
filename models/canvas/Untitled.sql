WITH stg_jaffle_shop__orders AS (
  SELECT
    *
  FROM {{ ref('rapid_onboarding_exemplar', 'stg_jaffle_shop__orders') }}
), stg_jaffle_shop__customers AS (
  SELECT
    *
  FROM {{ ref('rapid_onboarding_exemplar', 'stg_jaffle_shop__customers') }}
), join_1 AS (
  SELECT
    *
  FROM stg_jaffle_shop__orders
  JOIN stg_jaffle_shop__customers
    USING (CUSTOMER_ID)
), formula_1 AS (
  SELECT
    *,
    FIRST_NAME || ' ' || LAST_NAME AS CUSTOMER_FULL_NAME
  FROM join_1
), formula AS (
  SELECT
    *,
    {{ dbt }}
  FROM formula_1
), untitled_sql AS (
  SELECT
    *
  FROM formula
)
SELECT
  *
FROM untitled_sql