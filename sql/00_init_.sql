CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- 1) raw: JSONL 그대로 적재
CREATE TABLE IF NOT EXISTS raw.user_events (
  load_id        uuid        NOT NULL,
  line_no        bigint      NOT NULL,
  payload        jsonb       NOT NULL,
  ingested_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (load_id, line_no)
);

-- 2) staging: 타입 정리된 이벤트
CREATE TABLE IF NOT EXISTS staging.user_events (
  event_id     uuid        NOT NULL,
  event_time   timestamptz NOT NULL,
  event_type   text        NOT NULL,
  session_id   uuid        NOT NULL,
  user_id      text        NOT NULL,
  device       text        NOT NULL,
  country      text        NOT NULL,
  product_id   text        NOT NULL,
  category     text,
  price        integer,
  quantity     integer,
  revenue      integer,
  referrer     text,
  payment      text,
  load_id      uuid        NOT NULL,
  PRIMARY KEY (event_id)
);

CREATE INDEX IF NOT EXISTS ix_stg_user_events_time ON staging.user_events(event_time);
CREATE INDEX IF NOT EXISTS ix_stg_user_events_user ON staging.user_events(user_id);
CREATE INDEX IF NOT EXISTS ix_stg_user_events_prod ON staging.user_events(product_id);

-- 3) marts: SCD2 dim_user
CREATE TABLE IF NOT EXISTS marts.dim_user (
  user_key     bigserial   PRIMARY KEY,
  user_id      text        NOT NULL,
  device       text        NOT NULL,
  country      text        NOT NULL,
  valid_from   timestamptz NOT NULL,
  valid_to     timestamptz,
  is_current   boolean     NOT NULL DEFAULT true,
  UNIQUE (user_id, valid_from)
);
CREATE INDEX IF NOT EXISTS ix_dim_user_current ON marts.dim_user(user_id, is_current);

-- 4) marts: dim_product (Type1로 단순화: 최신 속성 유지)
CREATE TABLE IF NOT EXISTS marts.dim_product (
  product_key  bigserial PRIMARY KEY,
  product_id   text      NOT NULL UNIQUE,
  category     text,
  brand        text,
  price        integer,
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- 5) marts: fact
CREATE TABLE IF NOT EXISTS marts.fact_user_events (
  event_id     uuid        PRIMARY KEY,
  event_time   timestamptz NOT NULL,
  event_type   text        NOT NULL,
  session_id   uuid        NOT NULL,
  user_key     bigint      NOT NULL REFERENCES marts.dim_user(user_key),
  product_key  bigint      NOT NULL REFERENCES marts.dim_product(product_key),
  quantity     integer,
  revenue      integer,
  referrer     text,
  payment      text
);

CREATE INDEX IF NOT EXISTS ix_fact_time ON marts.fact_user_events(event_time);
