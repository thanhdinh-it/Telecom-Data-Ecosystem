# Troubleshooting Guide

> Cập nhật file này ngay khi gặp lỗi mới. Đây là nội dung kể chuyện hay trong phỏng vấn.

---

## Docker & Infrastructure

### HDFS Namenode không start

**Triệu chứng:** Container restart liên tục, log hiện `java.io.IOException: Incompatible clusterIDs`

**Nguyên nhân:** Volume cũ còn dữ liệu từ cluster ID khác.

**Fix:**
```bash
docker-compose down
docker volume rm de_namenode-data de_datanode-data
docker-compose up namenode datanode -d
```

**Workaround nếu vẫn lỗi:** Chuyển sang MinIO (S3-compatible):
```yaml
minio:
  image: minio/minio
  command: server /data --console-address ":9001"
  ports:
    - "9000:9000"
    - "9001:9001"
  environment:
    MINIO_ROOT_USER: minioadmin
    MINIO_ROOT_PASSWORD: minioadmin
```

---

### Airflow "No module named 'airflow'"

**Nguyên nhân:** Airflow chưa init DB hoặc `airflow-init` container chưa chạy xong.

**Fix:**
```bash
docker-compose logs airflow-init  # xem log init
docker-compose run --rm airflow-init  # chạy lại init thủ công
```

---

### Kafka KRaft "No cluster ID found"

**Triệu chứng:** Kafka không start, log hiện `ERROR Not found: clusterID`

**Nguyên nhân:** Volume kafka-data cũ conflict với CLUSTER_ID mới.

**Fix:**
```bash
docker-compose down kafka
docker volume rm de_kafka-data
docker-compose up kafka -d
```

---

## PySpark

### `java.lang.OutOfMemoryError: GC overhead limit exceeded`

**Nguyên nhân:** Spark Worker không đủ RAM cho job.

**Fix:**
1. Giảm data partitions: thêm `.coalesce(4)` trước heavy operations
2. Giảm `spark.sql.shuffle.partitions` (default 200 → 20):
```python
spark.conf.set("spark.sql.shuffle.partitions", "20")
```
3. Bật Adaptive Query Execution:
```python
.config("spark.sql.adaptive.enabled", "true")
```

---

### Kafka-Spark connector `ClassNotFoundException`

**Triệu chứng:** `org.apache.spark.SparkException: Failed to find data source: kafka`

**Fix:** Phải thêm `--packages` khi spark-submit:
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  job_fraud_detection.py
```

Hoặc thêm vào SparkSession:
```python
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
```

---

### Timestamp parse fail trong PySpark

**Triệu chứng:** `to_timestamp()` trả về null cho dữ liệu hợp lệ.

**Nguyên nhân:** PySpark 3.x mặc định strict mode cho timestamp parsing.

**Fix:** Thêm legacy mode:
```python
.config("spark.sql.legacy.timeParserPolicy", "LEGACY")
```

---

## Kafka Producer

### `NoBrokersAvailable` khi chạy producer từ host

**Nguyên nhân:** Producer trỏ đến `kafka:9092` nhưng đang chạy ngoài Docker network.

**Fix:** Dùng external listener `localhost:29092`:
```python
producer = KafkaProducer(bootstrap_servers=["localhost:29092"])
```

---

## dbt

### `Relation "analytics.stg_customers" does not exist`

**Nguyên nhân:** Chạy `dbt run --select marts` nhưng staging views chưa tồn tại.

**Fix:** Luôn chạy theo thứ tự layer:
```bash
dbt run --select staging
dbt run --select intermediate
dbt run --select marts
```
Hoặc chạy tất cả: `dbt run`

---

### `dbt test` fail với `surrogate_key`

**Nguyên nhân:** Thiếu package `dbt_utils`.

**Fix:**
```bash
dbt deps  # install packages từ packages.yml
dbt run
dbt test
```

---

## PostgreSQL

### `FATAL: database "airflow_db" does not exist`

**Nguyên nhân:** init.sql chưa chạy vì volume đã tồn tại từ trước (Docker không re-run init script).

**Fix:**
```bash
docker-compose down postgres
docker volume rm de_postgres-data
docker-compose up postgres -d
# Đợi init.sql chạy tự động
docker logs postgres  # verify: "database system is ready to accept connections"
```

---

## Performance Issues

### Consumer lag tăng dần (Kafka backpressure)

**Triệu chứng:** `kafka-consumer-groups --describe` hiện lag tăng liên tục.

**Nguyên nhân 1:** Spark job xử lý chậm hơn producer gửi.
**Fix:** Tăng trigger interval hoặc giảm producer rate:
```python
.trigger(processingTime="60 seconds")  # từ 30s lên 60s
```

**Nguyên nhân 2:** `write_fraud_to_postgres` với `batch_df.count()` gọi 2 lần (count + write).
**Fix:** Cache batch_df:
```python
def write_fraud_to_postgres(batch_df, batch_id):
    batch_df.cache()
    count = batch_df.count()
    if count > 0:
        batch_df.write.jdbc(...)
    batch_df.unpersist()
```
