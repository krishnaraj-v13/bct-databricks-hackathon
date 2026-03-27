 CREATE OR REFRESH STREAMING TABLE primeins.gold.fact_sales
SELECT distinct * FROM
(SELECT 
  -- Keys
  s.sales_id,
  s.car_id,
  s.Region                                            AS region,
  s.State                                             AS state,
  s.City                                              AS city,

  -- Date keys → join to dim_date
  CAST(date_format(
    TO_TIMESTAMP(s.ad_placed_on, 'dd-MM-yyyy HH:mm'),
    'yyyyMMdd'
  ) AS INT)                                           AS ad_placed_date_key,

  CAST(date_format(
    TO_TIMESTAMP(
      CASE WHEN s.sold_on = 'NULL' THEN NULL ELSE s.sold_on END,
      'dd-MM-yyyy HH:mm'
    ), 'yyyyMMdd'
  ) AS INT)                                           AS sold_date_key,

  -- Seller attributes
  s.seller_type,
  s.owner,

  -- Measures
  s.original_selling_price,
  s.original_selling_price                            AS net_selling_price,

  CASE WHEN s.sold_on = 'NULL'
       THEN FALSE ELSE TRUE END                       AS is_sold,

  DATEDIFF(
    TO_TIMESTAMP(
      CASE WHEN s.sold_on = 'NULL' THEN NULL ELSE s.sold_on END,
      'dd-MM-yyyy HH:mm'
    ),
    TO_TIMESTAMP(s.ad_placed_on, 'dd-MM-yyyy HH:mm')
  )                                                   AS days_to_sell,

  CURRENT_TIMESTAMP()                                 AS dw_insert_timestamp

FROM STREAM(primeins.silver.sales) s
);
