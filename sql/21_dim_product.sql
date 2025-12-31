WITH latest_prod AS (
  SELECT DISTINCT ON (payload->>'product_id')
    payload->>'product_id'        AS product_id,
    payload->>'category'          AS category,
    payload->>'brand'             AS brand,
    NULLIF(payload->>'price','')::int AS price,
    (payload->>'event_time')::timestamptz AS asof_time
  FROM raw.user_events
  WHERE load_id = %s
    AND payload ? 'product_id'
  ORDER BY
    payload->>'product_id',
    (payload->>'event_time')::timestamptz DESC
)
INSERT INTO marts.dim_product (
  product_id, category, brand, price, updated_at
)
SELECT
  product_id,
  category,
  brand,
  price,
  now()
FROM latest_prod
ON CONFLICT (product_id)
DO UPDATE SET
  category   = EXCLUDED.category,
  brand      = EXCLUDED.brand,
  price      = EXCLUDED.price,
  updated_at = now();
