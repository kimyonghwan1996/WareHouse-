WITH latest_user_attr AS (
  SELECT DISTINCT ON (user_id)
    user_id,
    device,
    country,
    event_time AS asof_time
  FROM staging.user_events
  WHERE load_id = %s
  ORDER BY user_id, event_time DESC
),
changed AS (
  SELECT
    l.user_id,
    l.device,
    l.country,
    l.asof_time,
    d.user_key
  FROM latest_user_attr l
  LEFT JOIN marts.dim_user d
    ON d.user_id = l.user_id
   AND d.is_current = true
  WHERE d.user_key IS NULL
     OR d.device  <> l.device
     OR d.country <> l.country
),
closed AS (
  -- 1️⃣ 기존 current 닫기
  UPDATE marts.dim_user d
  SET valid_to = c.asof_time,
      is_current = false
  FROM changed c
  WHERE d.user_key = c.user_key
  RETURNING d.user_key
)
-- 2️⃣ 새 current 열기
INSERT INTO marts.dim_user (
  user_id, device, country,
  valid_from, valid_to, is_current
)
SELECT
  user_id,
  device,
  country,
  asof_time AS valid_from,
  NULL::timestamptz AS valid_to,
  true AS is_current
FROM changed;
