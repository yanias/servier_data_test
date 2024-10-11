WITH
  TRANSCATION AS (
  SELECT
    "01/01/2020" date,
    "1234" order_id,
    "999" client_id,
    "490756" prod_id,
    "50" prod_price,
    1 prod_qty
  UNION ALL
  SELECT
    "01/01/2020",
    "1234",
    "999",
    "389728",
    "3,56",
    4
  UNION ALL
  SELECT
    "01/01/2020",
    "3456",
    "845",
    "490756",
    "50",
    2
  UNION ALL
  SELECT
    "01/01/2020",
    "3456",
    "845",
    "549380",
    "300",
    1
  UNION ALL
  SELECT
    "01/01/2020",
    "3456",
    "845",
    "293718",
    "10",
    6 ),
  PRODUCT_NOMMENCLATURE AS (
  SELECT
    "490756" product_id,
    "MEUBLE" product_type,
    "Chaise" product_name
  UNION ALL
  SELECT
    "389728",
    "DECO",
    "Boule de Noël"
  UNION ALL
  SELECT
    "549380",
    "MEUBLE",
    "Canapé"
  UNION ALL
  SELECT
    "293718",
    "DECO",
    "Mug" ),
  Transfo_transaction AS(
  SELECT
    PARSE_DATE('%m/%d/%Y',date) AS date,
    order_id,
    client_id,
    prod_id,
    CAST(REPLACE(prod_price,",",".") AS FLOAT64) prod_price,
    prod_qty
  FROM
    TRANSCATION )
SELECT
  FORMAT_DATE("%m/%d/%Y",date) date,
  CAST(SUM(prod_price*prod_qty)*1000 AS INT64) AS Ventes
FROM
  Transfo_transaction
  -- to be added for test 1 but on the the exhaustive table
  --  WHERE date BETWEEN "2019-01-01" AND "2019-12-31"

GROUP BY
  ALL
ORDER BY
  1