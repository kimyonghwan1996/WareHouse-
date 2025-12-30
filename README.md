# WareHouse

# User Behavior Log Data Warehouse (Local Docker Project)

## 📌 프로젝트 개요
이 프로젝트는 **웹/앱 서비스의 유저 행동 로그를 수집하여 데이터 웨어하우스로 적재하고,
분석 가능한 Fact/Dimension 모델로 가공하는 데이터 엔지니어링 파이프라인**입니다.

- Raw(JSONL) → Staging → Marts(Dim/Fact)
- **SCD Type 2**를 적용한 사용자 차원 테이블
- **Docker + Airflow + Postgres(Redshift 계열 SQL)** 기반
- 로컬 환경에서 **추가 설치 없이 Docker만으로 실행 가능**

---

## 🏗️ 전체 아키텍처

[Python Log Generator]
↓ (JSONL)
[Raw Schema (jsonb)]
↓
[Staging Schema]
↓
[Dim / Fact (Marts)]
↓
[Analytics / KPI Query]

yaml
코드 복사

- Orchestration: **Airflow**
- Warehouse: **Postgres (Redshift 호환 SQL 설계)**
- Execution: **Docker Compose**

---

## 📂 프로젝트 구조

de-dwh-project/
├─ docker-compose.yml
├─ dags/
│ └─ dwh_pipeline.py
├─ sql/
│ ├─ 00_init.sql # 스키마 및 테이블 생성
│ ├─ 10_staging.sql # Raw → Staging
│ ├─ 20_dim_user_scd2.sql # SCD Type 2 (User)
│ ├─ 21_dim_product.sql # Product Dimension
│ ├─ 30_fact.sql # Fact Table 적재
│ └─ 90_quality_checks.sql # 데이터 품질 체크
├─ scripts/
│ └─ generate_user_behavior_logs.py
└─ data/
└─ raw/
└─ events.jsonl

yaml
코드 복사

---

## 🧪 데이터 소스 (User Behavior Logs)

### 이벤트 타입
- `page_view`
- `add_to_cart`
- `purchase`

### 로그 특징
- Python으로 직접 생성한 **현실적인 유저 행동 로그**
- 세션 기반 이벤트 흐름
- 시간대별 트래픽 패턴 반영
- 구매 전환율 약 **2~5%**
- 유저별 device / country 고정

### 예시 로그
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
🗄️ 데이터 모델링
⭐ Fact Table
marts.fact_user_events

event_id (PK)

event_time

event_type

session_id

user_key (FK)

product_key (FK)

quantity

revenue

📐 Dimension Tables
dim_user (SCD Type 2)
user_key (PK)

user_id

device

country

valid_from

valid_to

is_current

➡️ 유저의 device/country 변경 시 기존 row 종료 후 신규 row 생성

dim_product
product_key (PK)

product_id

category

brand

price

🔄 ETL / ELT 파이프라인 (Airflow DAG)
DAG: dwh_raw_to_marts
Python 로그 생성기 실행

JSONL → raw.user_events 적재

Raw → Staging 타입 변환

dim_user SCD Type 2 처리

dim_product upsert

Fact 테이블 적재

데이터 품질 검사

✅ 데이터 품질 체크
staging 테이블의 핵심 컬럼 null 여부 검증

staging 대비 fact 적재 비율 90% 이상 확인

실패 시 DAG 즉시 중단

🚀 실행 방법
1️⃣ Docker 실행
bash
코드 복사
docker compose up -d
2️⃣ Airflow 접속
URL: http://localhost:8080

ID / PW: admin / admin

3️⃣ Connection 설정 (최초 1회)
Airflow UI → Admin → Connections

Conn Id: warehouse_pg

Conn Type: Postgres

Host: warehouse

Schema: dwh

Login: dwh

Password: dwh

Port: 5432

4️⃣ DAG 실행
dwh_raw_to_marts → Trigger DAG

🔍 결과 확인 예시
sql
코드 복사
-- 이벤트 분포
SELECT event_type, COUNT(*)
FROM marts.fact_user_events
GROUP BY 1
ORDER BY 2 DESC;

-- 현재 활성 유저 수
SELECT COUNT(*) FROM marts.dim_user WHERE is_current = true;

-- 총 매출
SELECT SUM(revenue) FROM marts.fact_user_events;
🎯 프로젝트에서 강조한 엔지니어링 포인트
Raw / Staging / Mart 계층 분리

SCD Type 2 기반 차원 모델링

이벤트 시점 기준 dimension join

배치 파이프라인 idempotency (event_id 기준 중복 방지)

Docker 기반 재현 가능한 로컬 환경

🧠 면접용 한 줄 요약
“웹 서비스의 유저 행동 로그를 직접 생성해 Raw부터 Mart까지 데이터 웨어하우스를 구축했고,
Airflow로 배치 파이프라인을 구성했습니다.
사용자 속성 변경을 관리하기 위해 SCD Type 2를 적용했으며,
분석가가 바로 KPI를 조회할 수 있는 Fact/Dimension 모델을 설계했습니다.”

🔧 향후 개선 아이디어
dbt 도입 및 테스트 추가

날짜 기준 fact 파티셔닝

Late-arriving data 처리

비용/성능 최적화 실험

📎 기술 스택
Python

Airflow

Postgres (Redshift-style SQL)

Docker / Docker Compose
