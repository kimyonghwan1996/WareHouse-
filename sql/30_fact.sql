INSERT INTO marts.fact_user_events (
  event_id, event_time, event_type, session_id,
  user_key, product_key,
  quantity, revenue, referrer, payment
)
SELECT
  e.event_id,
  e.event_time,
  e.event_type,
  e.session_id,
  u.user_key,
  p.product_key,
  e.quantity,
  e.revenue,
  e.referrer,
  e.payment
FROM staging.user_events e
JOIN marts.dim_product p
  ON p.product_id = e.product_id
JOIN marts.dim_user u
  ON u.user_id = e.user_id
 AND u.valid_from <= e.event_time
 AND (u.valid_to  > e.event_time OR u.valid_to IS NULL)
WHERE e.load_id = {{ params.load_id }}
ON CONFLICT (event_id) DO NOTHING;
