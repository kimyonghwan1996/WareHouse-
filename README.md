# WareHouse — User Behavior Log Data Warehouse

간단요약
- 웹/앱의 유저 행동 로그를 생성 → 수집 → 웨어하우스 적재 → 분석 가능한 Dim/Fact 모델로 가공하는 로컬 데이터 웨어하우스 파이프라인입니다.
- Docker 환경에서 Airflow + Postgres(Redshift 스타일 SQL)로 재현 가능하도록 구성되어 있습니다.
- 핵심 포인트: Raw → Staging → Marts 계층 분리, SCD Type 2 사용자 차원, 배치 파이프라인의 idempotency 보장.

---

## 주요 특징
- Raw(JSONL) → Staging → Marts(Dim/Fact) ELT 흐름  
- 사용자 차원은 SCD Type 2로 변경 이력 관리  
- Docker Compose로 로컬에서 별도 설치 없이 실행 가능  
- Airflow로 오케스트레이션, Postgres(웨어하우스 역할) 사용

---

## 전체 아키텍처 (요약)
Python Log Generator (JSONL)

↓

raw schema (jsonb 저장)

↓

staging (타입/정제)

↓

marts (dim/fact)

↓

analytics / KPI

오케스트레이션: Airflow  
웨어하우스: Postgres (Redshift 호환 SQL 설계)  
실행·배포: Docker Compose

---

## 프로젝트 구조
- docker-compose.yml
- dags/
  - dwh_pipeline.py
- sql/
  - 00_init.sql         — 스키마 및 테이블 생성
  - 10_staging.sql      — Raw → Staging 변환
  - 20_dim_user_scd2.sql— dim_user SCD Type 2 처리
  - 21_dim_product.sql  — dim_product upsert
  - 30_fact.sql         — fact 테이블 적재
  - 90_quality_checks.sql— 데이터 품질 검사
- scripts/
  - generate_user_behavior_logs.py — 로그 생성기
- data/raw/events.jsonl — 예시 원시 로그

---

## 데이터 소스: User Behavior Logs
이 프로젝트는 Python 스크립트로 현실적인 유저 행동 로그(JSONL)를 생성합니다.

이벤트 타입
- page_view
- add_to_cart
- purchase

로그 특징
- 세션 기반 이벤트 흐름
- 시간대별 트래픽 패턴 반영
- 구매 전환율 약 2~5%
- 유저별 device / country 고정(변경 가능성 고려)

예시 로그(JSON)
```json
{
  "event_time": "2025-01-10T12:30:15Z",
  "event_type": "purchase",
  "user_id": "u000123",
  "session_id": "uuid",
  "product_id": "p0045",
  "device": "mobile",
  "country": "KR",
  "price": 29000,
  "quantity": 1,
  "revenue": 29000
}
```

---

## 데이터 모델 (요약)

Fact table
- marts.fact_user_events
  - event_id (PK)
  - event_time
  - event_type
  - session_id
  - user_key (FK -> dim_user.user_key)
  - product_key (FK -> dim_product.product_key)
  - quantity
  - revenue
  - ... (원시 필드 보존용 jsonb 등)

Dimension tables
- marts.dim_user (SCD Type 2)
  - user_key (PK)
  - user_id
  - device
  - country
  - valid_from
  - valid_to
  - is_current
- marts.dim_product
  - product_key (PK)
  - product_id
  - category
  - brand
  - price

SCD 정책
- user의 device 또는 country 변경 시 기존 row의 valid_to를 종료 처리하고 새로운 row를 추가하여 is_current를 갱신합니다.

Idempotency
- event_id(또는 고유 식별자)를 기준으로 중복 적재 방지 로직 적용

---

## ETL / ELT 파이프라인 (Airflow DAG)
DAG: dwh_raw_to_marts
1. Python 로그 생성기 실행 (scripts/generate_user_behavior_logs.py)
2. JSONL → raw.user_events (raw 스키마에 jsonb로 적재)
3. Raw → Staging (데이터 타입 변환, null/정합성 체크)
4. dim_user: SCD Type 2 처리
5. dim_product: Upsert (존재하면 업데이트/혹은 무시)
6. fact_user_events 적재 (staging 대비 비율 체크 포함)
7. 데이터 품질 검사 (검수 실패 시 DAG 중단)

---

## 데이터 품질 체크 예시
- 필수 컬럼 null 체크 (staging 핵심 컬럼)
- staging 대비 fact 적재 비율 ≥ 90%
- 중복 event_id 존재 여부 검사

SQL 예시 (품질 체크)
```sql
-- 필수 컬럼 null 체크
SELECT COUNT(*) FROM staging.user_events WHERE event_time IS NULL OR event_type IS NULL;

-- fact 적재 비율
WITH s AS (SELECT COUNT(*) cnt FROM staging.user_events),
     f AS (SELECT COUNT(*) cnt FROM marts.fact_user_events)
SELECT f.cnt::float / s.cnt AS load_ratio FROM s, f;
```

---

## 실행 방법 (로컬)
1) Docker Compose 시작
```bash
docker compose up -d
```

2) Airflow UI 접속
- URL: http://localhost:8080
- ID / PW: admin / admin

3) (최초 1회) Airflow Connection 설정
- Airflow UI → Admin → Connections
  - Conn Id: warehouse_pg
  - Conn Type: Postgres
  - Host: warehouse
  - Schema: dwh
  - Login: dwh
  - Password: dwh
  - Port: 5432

4) DAG 트리거
- dwh_raw_to_marts → Trigger DAG

---

## 결과 확인용 쿼리 예시
```sql
-- 이벤트 분포
SELECT event_type, COUNT(*) FROM marts.fact_user_events GROUP BY 1 ORDER BY 2 DESC;

-- 현재 활성 유저 수 (is_current 기준)
SELECT COUNT(*) FROM marts.dim_user WHERE is_current = true;

-- 총 매출
SELECT SUM(revenue) FROM marts.fact_user_events;
```

---

## 강조한 엔지니어링 포인트
- Raw / Staging / Mart 계층 명확한 분리
- SCD Type 2 기반 차원 모델링 (사용자 변경 이력 보존)
- 이벤트 시점 기준 dimension join
- 배치 파이프라인의 idempotency (event_id 기준 중복 방지)
- Docker 기반 재현 가능한 로컬 개발 환경

---

## 향후 개선 아이디어
- dbt 도입으로 모델 관리 및 테스트 자동화
- 날짜 기준 fact 파티셔닝 (성능 개선)
- Late-arriving data 처리 로직 추가
- 비용/성능 최적화 실험 (인덱스/분할/클러스터링 등)
- 모니터링/알림(데이터 품질 실패 시 Slack/Email)

---

## 기술 스택
- Python (로그 생성 및 ETL 로직)
- Apache Airflow (오케스트레이션)
- Postgres (Redshift 호환 SQL 설계)
- Docker / Docker Compose
