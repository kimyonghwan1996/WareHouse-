-- staging null count
SELECT COUNT(*) AS null_cnt
FROM staging.user_events
WHERE load_id = %s
  AND (user_id IS NULL OR product_id IS NULL OR event_time IS NULL OR event_type IS NULL);

-- fact ratio
SELECT
  COUNT(*) FILTER (WHERE f.event_id IS NOT NULL)::numeric
  / NULLIF(COUNT(*), 0)::numeric AS fact_ratio
FROM staging.user_events s
LEFT JOIN marts.fact_user_events f
  ON s.event_id = f.event_id
WHERE s.load_id = %s;
