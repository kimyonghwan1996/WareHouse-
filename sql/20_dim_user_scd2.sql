-- 1) 이번 배치에서 등장한 유저의 "최신 속성" 뽑기
WITH latest_user_attr AS (
  SELECT DISTINCT ON (user_id)
    user_id,
    device,
    country,
    event_time AS asof_time
  FROM staging.user_events
  WHERE load_id = {{ params.load_id }}
  ORDER BY user_id, event_time DESC
),

-- 2) 현재 dim_user의 current와 비교해서 변경된 유저만 추림
changed AS (
  SELECT
    l.user_id, l.device, l.country, l.asof_time,
    d.user_key, d.device AS cur_device, d.country AS cur_country
  FROM latest_user_attr l
  LEFT JOIN marts.dim_user d
    ON d.user_id = l.user_id AND d.is_current = true
  WHERE d.user_key IS NULL
     OR d.device <> l.device
     OR d.country <> l.country
)

-- 3) 기존 current 닫기
UPDATE marts.dim_user d
SET valid_to = c.asof_time,
    is_current = false
FROM changed c
WHERE d.user_key = c.user_key;

-- 4) 새 current 열기
INSERT INTO marts.dim_user (user_id, device, country, valid_from, valid_to, is_current)
SELECT
  user_id, device, country,
  asof_time AS valid_from,
  NULL::timestamptz AS valid_to,
  true AS is_current
FROM changed;
