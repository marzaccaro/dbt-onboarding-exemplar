WITH stg_jaffle_shop__orders AS (
  SELECT
    *
  FROM {{ ref('rapid_onboarding_exemplar', 'stg_jaffle_shop__orders') }}
), stg_jaffle_shop__customers AS (
  SELECT
    *
  FROM {{ ref('rapid_onboarding_exemplar', 'stg_jaffle_shop__customers') }}
), stg_stripe__payments AS (
  SELECT
    *
  FROM {{ ref('stg_stripe__payments') }}
), rename_1 AS (
  SELECT
    ORDER_ID AS O_ORDER_ID,
    CUSTOMER_ID AS O_CUSTOMER_ID,
    STATUS AS O_STATUS,
    *
    EXCLUDE (ORDER_ID, CUSTOMER_ID, STATUS)
  FROM stg_jaffle_shop__orders
), rename_2 AS (
  SELECT
    CUSTOMER_ID AS C_CUSTOMER_ID,
    *
    EXCLUDE (CUSTOMER_ID)
  FROM stg_jaffle_shop__customers
), rename_3 AS (
  SELECT
    ORDER_ID AS P_ORDER_ID,
    STATUS AS P_STATUS,
    *
    EXCLUDE (ORDER_ID, STATUS)
  FROM stg_stripe__payments
), join_1 AS (
  SELECT
    *
  FROM rename_1
  LEFT JOIN rename_2
    ON rename_1.O_CUSTOMER_ID = rename_2.C_CUSTOMER_ID
), join_2 AS (
  SELECT
    *
  FROM join_1
  LEFT JOIN rename_3
    ON join_1.O_ORDER_ID = rename_3.P_ORDER_ID
), rename_4 AS (
  SELECT
    O_ORDER_ID AS ORDER_ID,
    O_CUSTOMER_ID AS CUSTOMER_ID,
    ORDER_DATE,
    O_STATUS AS STATUS,
    _ETL_LOADED_AT,
    FIRST_NAME,
    LAST_NAME,
    PAYMENT_METHOD,
    AMOUNT
  FROM join_2
), formula_1 AS (
  SELECT
    *,
    FIRST_NAME || ' ' || LAST_NAME AS CUSTOMER_FULL_NAME
  FROM rename_4
), formula_2 AS (
  SELECT
    *,
    CASE WHEN PAYMENT_METHOD = 'credit_card' THEN AMOUNT * 0.05 ELSE 0 END AS FEES
  FROM formula_1
), final_model_sql AS (
  SELECT
    *
  FROM formula_2
)
SELECT
  *
FROM final_model_sql