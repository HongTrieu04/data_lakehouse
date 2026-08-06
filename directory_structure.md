```
data-lakehouse/
├── infra/
│   ├── docker-compose.yml              # MinIO, Postgres-Airflow, Postgres-Hive, Hive-Metastore, Spark Master/Worker, Airflow Webserver/Scheduler
│   ├── docker-compose.override.yml     # Override dev local (ports, local mounts)
│   ├── minio/
│   │   └── init-buckets.sh             # Script khởi tạo buckets: landing, bronze, silver, gold
│   ├── hive-metastore/
│   │   ├── Dockerfile
│   │   └── conf/
│   │       └── metastore-site.xml      # Cấu hình JDBC connection đến Postgres Hive
│   ├── hive-server2/                   # (Tuỳ chọn) nếu muốn query SQL trực tiếp qua JDBC Beeline
│   │   ├── Dockerfile
│   │   └── conf/
│   │       └── hive-site.xml
│   ├── spark/
│   │   ├── Dockerfile                  # Image Spark custom + Apache Iceberg + AWS S3A JARs
│   │   └── conf/
│   │       ├── spark-defaults.conf     # Iceberg catalog configuration + S3A MinIO credentials
│   │       └── log4j2.properties
│   └── airflow/
│       ├── Dockerfile                  # Image Airflow custom + Apache Spark Client / PySpark + Requirements
│       └── conf/
│           └── airflow.cfg             # LocalExecutor + Postgres SQLAlchemy connection
│
├── airflow/
│   ├── dags/
│   │   └── track2_loan/
│   │       └── dag_track2_loan_standalone.py  # Orchestrate Phase 0 -> Phase 5
│   ├── plugins/
│   └── requirements.txt                # Python libraries cho Airflow container
│
├── spark_jobs/
│   ├── common/
│   │   ├── spark_session.py            # Singleton SparkSession builder với Iceberg + MinIO config
│   │   ├── io_utils.py                 # Reader/Writer helper cho Parquet/Iceberg
│   │   └── recon_utils.py              # Reconciliation verification helpers (row count, sum, minus test)
│   ├── landing/
│   │   └── track2_loan_contract/
│   │       └── landing_loan_sources.py # Extract 21 source tables -> MinIO Landing
│   ├── bronze/
│   │   └── track2_loan_contract/       # 21 Bronze jobs (1:1 với source + audit cols)
│   │       ├── bz_t24_ld_loans_and_deposits.py
│   │       ├── bz_t24_customer.py
│   │       ├── bz_flexbo_pgb_ldtb_contract_master.py
│   │       └── ...
│   ├── silver/
│   │   └── track2_loan_contract/
│   │       ├── satellite/              # [BỔ SUNG] 6 Bảng vệ tinh Phase 1 (SCD2/Snapshot)
│   │       │   ├── ar_bal.py
│   │       │   ├── ar_rate_hist.py
│   │       │   ├── ar_dlq_smy.py
│   │       │   ├── ou.py
│   │       │   ├── exg_rate.py
│   │       │   └── ast_ar_int_smy.py
│   │       ├── temp1_technical/        # Phase 4 (Task 9)
│   │       │   └── intf_loan_ar.py
│   │       └── temp2_business/         # Phase 2 (Task 10) & Phase 3 (Task 11)
│   │           ├── loan_ar.py
│   │           └── loan_ar_prfl.py
│   └── gold/
│       └── track2_loan_contract/       # Phase 5 (Task 12)
│           └── dim_loan_ar.py
│
├── tests/
│   └── track2_loan_contract/
│       ├── unit/                       # Pytest unit tests cho spark_session & io_utils
│       └── recon/                      # Verification scripts cho Bronze, Silver, Gold
│
├── configs/
│   ├── env/
│   │   ├── local.yaml                  # Cấu hình URLs, Endpoints, Bucket Names
│   │   └── connections.yaml
│   └── schemas/
│       ├── bronze/
│       ├── silver/
│       └── gold/
│
├── .env                                # Chứa credentials thực (MINIO_ROOT_USER, POSTGRES_PASSWORD...)
├── .env.sample                         # Template file .env
└── README.md
```