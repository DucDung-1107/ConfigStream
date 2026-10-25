# ConfigStream — mô tả dự án và kiến trúc phương pháp

## 1. Dự án làm về gì?

ConfigStream là một nền tảng Big Data dùng để lưu trữ, so sánh và phân tích lịch sử thay đổi cấu hình theo thời gian. Đối tượng trung tâm là **asset** — thực thể sở hữu cấu hình, chẳng hạn thiết bị mạng, máy chủ NGINX/Linux, ứng dụng hoặc tài nguyên Kubernetes.

Mục tiêu của dự án không chỉ là trả lời “cấu hình đã thay đổi gì”, mà còn phải truy vết được:

- thay đổi xảy ra ở phiên bản nào và tại thời điểm nào;
- thay đổi thuộc asset nào, kể cả khi dữ liệu đến không đúng thứ tự;
- thay đổi có lặp lại, có khoảng phiên bản bị thiếu hay có cảnh báo rủi ro nào;
- lịch sử có thể được phát lại, kiểm tra tính toàn vẹn và phục hồi sau lỗi hay không.

Vì vậy, ConfigStream là một **data-pipeline evaluation platform** chứ không phải framework tự động cấu hình mạng hoặc hệ thống frontend-first. Network configuration là ví dụ chính; mô hình dữ liệu được thiết kế mở rộng cho nhiều loại asset.

## 2. Bài toán và đóng góp kỹ thuật

Hệ thống giải quyết bài toán tiếp nhận các snapshot cấu hình và event thay đổi, sau đó biến chúng thành lịch sử có cấu trúc, diff và alert có thể kiểm chứng. Các đóng góp/kết quả chính được thể hiện trong mã nguồn và tài liệu hiện có gồm:

1. **Versioned data contracts:** các schema Pydantic cho `Asset`, `ConfigSnapshot`, `ChangeEvent`, `ConfigDiff` và `Alert`, kèm kiểm tra kiểu, timestamp UTC, hash SHA-256 và tính hợp lệ của content/content URI.
2. **Deterministic workload generation:** bộ sinh dữ liệu có seed, version theo từng asset, mutation theo loại asset và đầu ra JSONL/Kafka tái lập được.
3. **Event-time stateful processing:** Kafka và Spark Structured Streaming thực hiện parse/validate, dedup theo identity, watermark, state theo `asset_id`, ghép các phiên bản kề nhau và xử lý được dữ liệu đến không theo thứ tự.
4. **Conservative normalization and diff:** chuẩn hóa text có kiểm soát, che một số khóa nhạy cảm, tính normalized hash và tạo diff dòng có ID xác định.
5. **Durable history and rebuildable serving:** MinIO/Parquet là lịch sử có commit manifest và là nguồn sự thật; Elasticsearch chỉ là projection metadata có thể rebuild; FastAPI cung cấp API read-only.
6. **Reproducible evaluation:** workload tổng hợp, lịch sử Git quan sát được, TerraDS, RPKI, RIS và DNS được giữ các semantics khác nhau; kết quả được đánh giá bằng oracle, invariant, replay/recovery và benchmark có giới hạn.

## 3. Kiến trúc tổng thể

```text
Nguồn dữ liệu / Generator / Git histories
                  |
                  v
        Kafka topics (asset/event/snapshot)
                  |
                  v
  Spark Structured Streaming (native state)
  parse -> validate -> watermark/dedup
        -> state theo asset/version
        -> normalize -> adjacent diff -> YAML rules
                  |
       +----------+-----------+
       |                      |
       v                      v
 MinIO / Parquet        Kafka config.alerts
 (authoritative lake)  (alert delivery)
       |
       v
 Spark batch analytics
       |
       v
 Elasticsearch metadata projection
       |
       v
 FastAPI read-only API + dashboard
```

### 3.1. Tầng schema và identity

Mỗi asset có `asset_id` ổn định. `ConfigSnapshot` có `snapshot_id`, `version`, `event_time`, `ingest_time`, raw SHA-256 và content hoặc content URI. `ChangeEvent` mô tả sự kiện độc lập với snapshot; hai record có thể liên hệ bằng metadata nhưng không có bảo đảm atomic/cross-topic ordering. Tất cả record dùng `schema_version` và được định tuyến bằng Kafka key là `asset_id` khi cần bảo toàn thứ tự trong partition.

### 3.2. Tầng ingestion và messaging

Generator hoặc collector tạo `Asset`, `ChangeEvent`, `ConfigSnapshot` và gửi vào các topic `config.assets`, `config.events`, `config.snapshots`. Kafka đóng vai trò message backbone và replay log, không phải application database. Ordering chỉ được bảo đảm trong một partition của một topic; hệ thống không giả định event luôn đến trước snapshot.

### 3.3. Tầng streaming processing

Spark đọc trực tiếp `config.snapshots`. SQL parsing/validation tách record hợp lệ và không hợp lệ; record lỗi được phát thành safe diagnostics, không ghi configuration body vào log. Record hợp lệ đi qua event-time watermark và dedup theo identity, sau đó được group theo `asset_id`.

State native của Spark giữ bounded version buffer, event-time timer, eviction floor và các edge flags. State chọn previous/current theo cùng asset và version adjacency, không theo row đến trước đó, global arrival order hoặc timestamp sort. Sau đó hệ thống gọi lại pure Python processing để normalize, diff và áp dụng các rule YAML để sinh `Alert`.

### 3.4. Tầng normalization và diff

Chính sách normalization v1 xử lý newline, trailing spaces, blank lines, một số generated-comment convention, separator đơn giản và các khóa bí mật như `password`, `token`, `api_key`. Raw content/hash không bị thay đổi. Diff là line-based deterministic diff bằng `SequenceMatcher`; `changed` là phần giao của các block thay thế, không phải một bộ đếm độc lập với `added` và `removed`. Secret-only change có thể không xuất hiện trong normalized diff, vì vậy raw history vẫn cần được bảo vệ và lưu giữ.

### 3.5. Tầng storage, analytics và serving

Luồng lake-first lưu raw bytes/provenance và processed typed outputs vào Parquet theo partition thời gian, query và batch; commit manifest giúp readers bỏ qua file chưa hoàn tất. Spark batch đọc Parquet để tính frequency, severity, history và anomaly analytics. Elasticsearch chứa metadata projection an toàn, deterministic document IDs và có thể reindex từ lake. FastAPI chỉ đọc projection này, dùng cursor phân trang ổn định và không trả configuration body trong các route thông thường.

## 4. Phương pháp đánh giá

Repository phân biệt ba lớp bằng chứng: (i) observed public file histories, (ii) calibrated/semi-synthetic controlled histories và (iii) stress/replay workloads. Ở các phase nghiên cứu sau, Terraform, RPKI, DNS và RIS có vai trò thời gian khác nhau; chúng không được gộp thành một loại “configuration change rate”.

Phương pháp đánh giá gồm sáu bước:

1. thu thập, đóng băng nguồn, hash, scope và license;
2. gán identity có kiểu cho version, observation, transition hoặc occurrence;
3. giữ đúng semantics về completeness, uncertainty, ordering và multiplicity;
4. publish immutable history, provenance, manifest và recovery/accounting;
5. chỉ suy ra analytics phù hợp với dữ liệu, không tự gán causal label;
6. so sánh với oracle/baseline đơn giản bằng exact identity, invariant, cost và negative findings.

Các benchmark hiện có là đánh giá cục bộ có kiểm soát. Một số matched processing pilot đã hoàn tất trong giới hạn được đăng ký; các full-rate/paced RIS comparisons và các tuyên bố về distributed scalability vẫn được ghi rõ là chưa thực hiện hoặc ngoài phạm vi. Do đó không nên diễn giải kết quả hiện tại thành production throughput, HA, exactly-once end-to-end hoặc tần suất thay đổi tự nhiên của thiết bị.

## 5. Cấu trúc mã nguồn chính

| Thư mục | Vai trò |
|---|---|
| `schemas/` | Model và contract cho asset, snapshot, event, diff, alert và temporal records |
| `generator/` | Sinh workload cấu hình deterministic |
| `messaging/` | Kafka producer, topic, serialization và delivery |
| `processing/` | State transition, normalization, diff, rules và temporal logic |
| `spark/` | Spark streaming, stateful processing, sinks và analytics runtime |
| `lake/` | MinIO/S3-compatible data lake và Parquet commit/read |
| `search/`, `api/` | Elasticsearch projection và FastAPI read-only service |
| `analytics/` | Batch analytics và publication |
| `workloads/`, `benchmarks/` | Dataset/workload preparation, evaluation và reports |
| `frontend/` | Dashboard đọc các aggregate/projection đã công bố |
| `tests/` | Unit, Spark, integration, API và Kubernetes acceptance tests |
| `docs/` | Architecture, data model, phase acceptance, methodology và limitations |

## 6. Trạng thái và giới hạn cần nêu trong báo cáo

- Kafka local hiện là single-broker development topology, replication factor 1, loopback plaintext; không phải HA production.
- Kafka delivery và alert sink vẫn có thể at-least-once; deterministic IDs không tự tạo exactly-once end-to-end.
- Spark chạy local trong nhiều acceptance/benchmark; các số đo không chứng minh distributed-cluster scalability.
- Elasticsearch là derived/rebuildable projection, không phải source of truth.
- Dataset observed có scope, coverage và uncertainty riêng; Git history là proxy cho configuration evolution, không phải deployment frequency.
- High input rates là accelerated replay/stress của lịch sử tích lũy, không phải tốc độ thay đổi tự nhiên trên từng thiết bị.

