-- (1) staging null 체크
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM staging.user_events
    WHERE load_id = {{ params.load_id }}
      AND (user_id IS NULL OR product_id IS NULL OR event_time IS NULL OR event_type IS NULL)
  ) THEN
    RAISE EXCEPTION 'Data quality failed: staging.user_events has null critical fields';
  END IF;
END $$;

-- (2) fact rowcount 체크 (staging 대비 너무 적으면 실패)
DO $$
DECLARE
  stg_cnt bigint;
  fact_cnt bigint;
BEGIN
  SELECT COUNT(*) INTO stg_cnt FROM staging.user_events WHERE load_id = {{ params.load_id }};
  SELECT COUNT(*) INTO fact_cnt
  FROM marts.fact_user_events f
  JOIN staging.user_events s ON s.event_id = f.event_id
  WHERE s.load_id = {{ params.load_id }};

  IF stg_cnt > 0 AND fact_cnt::numeric / stg_cnt::numeric < 0.90 THEN
    RAISE EXCEPTION 'Data quality failed: fact load ratio < 0.90 (stg=% fact=%)', stg_cnt, fact_cnt;
  END IF;
END $$;
