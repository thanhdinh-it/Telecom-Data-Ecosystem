# Architecture Decision Records

> Ghi lại các quyết định kỹ thuật quan trọng trong quá trình xây dựng hệ thống.  
> Sử dụng format: **Context → Decision → Rationale → Trade-offs**

---

## ADR-001: Chọn Parquet thay vì CSV trên HDFS

**Ngày:** 2024-06-17  
**Trạng thái:** Accepted  
**Người quyết định:** DE Portfolio Team

### Bối cảnh
Cần chọn format lưu trữ cho output của PySpark Batch job trên HDFS processed zone.  
Dữ liệu gồm: daily CDR aggregation (~500K records/ngày → ~15M records/tháng).

### Quyết định
Dùng **Apache Parquet** với Snappy compression.

### Lý do
| Tiêu chí | CSV | Parquet |
|---|---|---|
| Compression | Không tự compress | ~60-80% nhỏ hơn CSV |
| Schema | Không lưu schema | Lưu schema trong file |
| Read performance | Đọc toàn bộ row | Columnar: chỉ đọc cột cần |
| Predicate pushdown | Không hỗ trợ | Spark bỏ qua row groups không match |
| Debug | Dễ đọc bằng text editor | Cần công cụ (parquet-tools) |

**Số liệu thực tế:** CDR 30 ngày (~15M rows) trên Parquet/Snappy: ~120MB vs CSV: ~450MB.

### Trade-offs
- ❌ Không đọc được bằng text editor thông thường
- ❌ Khó debug trực tiếp hơn CSV
- ✅ Đọc nhanh hơn 3-5x khi Spark chỉ cần 3-4 cột
- ✅ Schema được giữ nguyên qua các lần đọc

---

## ADR-002: Dùng KRaft mode cho Kafka (không có Zookeeper)

**Ngày:** 2024-06-17  
**Trạng thái:** Accepted

### Bối cảnh
Cần Kafka cho real-time streaming pipeline. Traditional Kafka requires Zookeeper để quản lý metadata cluster.  
Machine constraints: 16GB RAM tổng.

### Quyết định
Dùng **Kafka với KRaft mode** (Kafka Raft Metadata) — không cần Zookeeper.

### Lý do
- **RAM budget:** Zookeeper tiêu thụ thêm ~512MB RAM — không đủ budget trên máy cá nhân
- **Simplicity:** Ít container hơn (không cần zookeeper service), ít điểm lỗi hơn
- **Future direction:** KRaft là roadmap chính thức của Apache Kafka từ v3.3+, Zookeeper đang bị deprecated
- **Single broker setup:** KRaft phù hợp cho single-node dev/portfolio setup

### Trade-offs
- ❌ KRaft còn tương đối mới — một số edge cases chưa được kiểm chứng kỹ ở production
- ❌ Một số công cụ quản lý Kafka cũ (Kafka Manager, v.v.) chưa hỗ trợ KRaft hoàn toàn
- ✅ Tiết kiệm ~512MB RAM
- ✅ Đơn giản hóa cấu hình Docker Compose

---

## ADR-003: Dùng dbt thay vì thuần SQL cho Transformation Layer

**Ngày:** 2024-06-24  
**Trạng thái:** Accepted

### Bối cảnh
Cần transform dữ liệu từ HDFS processed zone → PostgreSQL analytics schema để phục vụ dashboard và ML.

### Quyết định
Dùng **dbt-postgres** thay vì viết SQL scripts thuần.

### Lý do
- **Testing built-in:** dbt test (unique, not_null, accepted_values) chạy tự động — không cần viết assertion riêng
- **Lineage graph:** dbt docs generate tạo data lineage graph — visualize được dependency giữa các models
- **Modularity:** Mỗi model là 1 file SQL, dễ maintain và review
- **Jinja templating:** `{{ ref('stg_customers') }}`, `{{ source('raw', 'cdr') }}` → compile-time dependency resolution
- **Materialization flexibility:** Staging dùng View (nhanh, không persist), Marts dùng Table (cache cho dashboard)
- **Industry standard:** dbt là công cụ phổ biến nhất cho analytics engineering — portfolio value cao

### Trade-offs
- ❌ Learning curve ban đầu (Jinja, project structure)
- ❌ Thêm 1 dependency (dbt-postgres)
- ✅ Test coverage cho data models tự động
- ✅ Documentation được generate từ code
- ✅ Có thể integrate vào Airflow DAG dễ dàng (`BashOperator: dbt run`)

---

## ADR-004: Dùng foreachBatch thay vì JDBC Streaming Sink

**Ngày:** 2024-07-08  
**Trạng thái:** Accepted

### Bối cảnh
PySpark Structured Streaming cần ghi fraud alerts vào PostgreSQL.  
JDBC DataSourceV2 không hỗ trợ streaming write mode trực tiếp.

### Quyết định
Dùng **`writeStream.foreachBatch()`** với JDBC batch write bên trong.

### Lý do
- JDBC sink trong PySpark Streaming chỉ hỗ trợ `append` mode với micro-batch thông qua foreachBatch
- foreachBatch cho phép dùng toàn bộ DataFrame API (JDBC, custom transformations) trong mỗi micro-batch
- Có thể kiểm tra `batch_df.count() > 0` trước khi ghi → tránh tạo empty transactions

### Trade-offs
- ❌ Không phải "true streaming" — vẫn là micro-batch (trigger every 30s)
- ❌ Nếu foreachBatch fail, có thể duplicate writes (at-least-once guarantee)
- ✅ Đơn giản hơn custom sink
- ✅ JDBC connection pooling có thể cấu hình trong properties
- ✅ Dễ debug vì mỗi batch được log riêng

**Mitigation cho duplicate:** Dùng `INSERT ... ON CONFLICT DO NOTHING` hoặc thêm unique constraint trên (caller_id, window_start, window_end).
