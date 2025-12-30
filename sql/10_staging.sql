INSERT INTO staging.user_events (
  event_id, event_time, event_type, session_id, user_id, device, country,
  product_id, category, price, quantity, revenue, referrer, payment, load_id
)
SELECT
  (payload->>'event_id')::uuid                           AS event_id,
  (payload->>'event_time')::timestamptz                  AS event_time,
  payload->>'event_type'                       AS event_type,
  (payload->>'session_id')::uuid                         AS session_id,
  payload->>'user_id'                                    AS user_id,
  payload->>'device'                                     AS device,
  payload->>'country'                                    AS country,
  payload->>'product_id'                                 AS product_id,
  payload->>'category'                                   AS category,
  NULLIF(payload->>'price','')::int                      AS price,
  NULLIF(payload->>'quantity','')::int                   AS quantity,
  COALESCE(NULLIF(payload->>'revenue','')::int, 0)       AS revenue,
  payload->>'referrer'                                   AS referrer,
  payload->>'payment'                                    AS payment,
  load_id
FROM raw.user_events
WHERE load_id = '{{ params.load_id }}'
ON CONFLICT (event_id) DO NOTHING;
