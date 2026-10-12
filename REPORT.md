# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Đoàn Anh Quân |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 202602803 |
| GitHub | https://github.com/DAQuan-VinAI/K4-L3L4-Track4-DataPipeline4ComputerVision |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-04868 |

## Kết quả cuối

- `dataset_version` của bạn: `72d089019416`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 720 / 174 / 126 (tổng 1020; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt giữa hai nguồn nhìn thấy ở bronze (2 dòng đầu mỗi bảng):

1. **Tên cột khác hẳn**: A dùng `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer`; B dùng `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering`.
2. **Đơn vị tốc độ**: A là m/s (`1.213`), B là km/h (`55.11`).
3. **Kiểu timestamp**: A là chuỗi ISO UTC (`2025-10-07T08:00:00.014Z`), B là epoch mili giây (`1759933801509`).
4. **Bàn đạp**: A là tỉ lệ 0..1 (`gas = 0.028`), B là phần trăm (`throttle_pct = 2.0`).
5. **Đồng hồ**: `bronze.video_files` ghi `telemetry_clock_offset_ms` = 0 cho drive_a và 1500 cho drive_b, tức đồng hồ thiết bị B chạy lệch 1,5 giây so với video.

Giữ mỗi nguồn một bảng và để mọi cột dạng chữ vì bronze là bản sao nguyên trạng của nguồn: nếu gộp và ép kiểu ngay thì phải quyết định sớm về đơn vị, múi giờ, ô trống, và một dòng không ép được sẽ bị mất hoặc làm hỏng cả lần nạp. Để nguyên thì khi luật làm sạch sai, mình sửa ở silver rồi chạy lại từ bronze mà không cần xin lại dữ liệu gốc. Bronze của tôi có 3021 dòng (A) và 3234 dòng (B), đúng bằng số dòng CSV, và `bronze.raw_telemetry_a` chỉ có một commit (version 0, 3021 dòng) dù đã nạp hai lần.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Hai model staging (`stg_telemetry_a.sql`, `stg_telemetry_b.sql`) đổi tên cột về chung một bộ, đổi tốc độ của B từ km/h sang m/s, đổi epoch ms thành timestamp UTC và **trừ 1500 ms** độ lệch đồng hồ khai báo trong `sources.json`.

Bằng chứng đã trừ đúng: dòng thô đầu tiên của B là `1759933801509` ms = 14:30:01.509 UTC; trong `silver_telemetry` thời điểm bắt đầu của drive_b là `2025-10-08 14:30:00.009`, khớp giờ bắt đầu video `14:30:00` (verify: "first sample within 0.5 s of video start"). Tốc độ tối đa sau khi đổi đơn vị là 71,3 km/h (A) và 100,2 km/h (B), đều hợp lý.

| drive | duplicate | speed_missing | speed_out_of_range | tổng loại | sạch | nhận về |
|---|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 | 2968 | 3021 |
| drive_b | 20 | 30 | 3 | 53 | 3181 | 3234 |

Dòng lỗi vào `silver_telemetry_rejects` kèm `reject_reason` thay vì bị xoá để phép cộng luôn khớp (2968 + 53 = 3021; 3181 + 53 = 3234), để sau này tra được vì sao một dòng bị loại, và để nếu luật sai (ví dụ ngưỡng tốc độ quá chặt) thì còn dữ liệu mà khôi phục.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` (khoảng trống giữa hai mẫu > 1000 ms) là `warn` vì cảm biến mất tín hiệu là khiếm khuyết đã biết của dữ liệu thật: nó không làm sai các dòng còn lại, và bước sau đã tự xử lý bằng cờ `sync_lost` (7 khung mỗi drive mất đồng bộ bị loại). Dừng cả pipeline vì chuyện này là quá tay. Lần chạy của tôi test này báo WARN với 2 kết quả. Ngược lại `in_range` trên `speed_mps` (0..70 m/s) là `error` vì một tốc độ vô lý là dữ liệu **sai**: nếu lọt xuống gold nó thành nhãn sai cho mô hình.

Thí nghiệm `break_gate.json`: nới `max_speed_mps` từ 70 lên 1000 thì `dbt test` cho `PASS=5 WARN=1 ERROR=1`, test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng** (đúng bằng 3 + 3 dòng `speed_out_of_range` của hai drive, lúc này đã lọt vào silver). Khôi phục luật gốc thì về `PASS=6 WARN=1 ERROR=0`.

Điều này cho thấy cổng chất lượng là lớp kiểm tra **độc lập với luật làm sạch**: khi ai đó nới nhầm luật ở tầng biến đổi, test vẫn chặn được và các task phía sau trong DAG không chạy, nên dữ liệu xấu không tới gold.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Theo `idempotency.json` (trip `trip-00003`, 60 khung): bảng `silver.frames` có **1246 dòng** trước, và vẫn **1246 dòng** sau hai lần chạy lại partition đó; dấu vân tay nội dung giữ nguyên `cb6118dbb0b8` (`unchanged: true`). Với cách ghi sai kiểu `append`, chạy 2 lần cho **120 dòng** cho một trip chỉ có 60 khung, tức nhân đôi.

Delta `overwrite` theo predicate `trip_id = '...'` an toàn hơn "xoá rồi ghi lại" vì việc thay partition là **một commit nguyên tử** trong transaction log: người đọc chỉ thấy phiên bản cũ hoặc phiên bản mới, không bao giờ thấy trạng thái trống ở giữa. Nếu tiến trình sập giữa chừng thì commit không được ghi và bảng vẫn ở phiên bản cũ. Với xoá rồi ghi lại, sập sau bước xoá là mất dữ liệu của trip đó; và nếu chạy lại nhiều lần thì cách ghi theo predicate vẫn cho cùng một kết quả, còn các partition khác (25 trip còn lại) không bị đụng tới.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 60 | 8 |
| 2 | 149 | 52 |
| 6 | 363 | 244 |

**Đen** là khung có độ sáng trung bình dưới 12 (`dark_luma`): camera hỏng hoặc bị che, không có thông tin, nên bị **loại** (A: 6 khung, B: 4 khung). **Tối** là độ sáng từ 12 đến dưới 40 (`dark_soft`): cảnh thật nhưng thiếu sáng (đường hầm, bóng râm), nên chỉ **gắn cờ và giữ lại** (A: 4, B: 61). Tôi đồng ý: nếu bỏ hết khung tối thì mô hình không bao giờ thấy điều kiện thiếu sáng, trong khi đó là lúc perception dễ sai nhất. Điểm tôi còn băn khoăn là hai ngưỡng 12 và 40 là số cố định, chưa chắc hợp với camera khác.

Theo `threshold.json` (bảng trên, tổng 602 khung ở A và 644 ở B): ngưỡng 0 chỉ bắt khung trùng tuyệt đối (60 và 8); ngưỡng 2 bắt 149 và 52; ngưỡng 6 loại tới 363 khung của A (60%) và 244 của B (38%). Tôi chọn giữ **ngưỡng 2**: ngưỡng 0 bỏ sót khung gần như y hệt (nhiễu nén làm lệch 1–2 bit), còn từ 4 trở lên số khung bị loại tăng vọt (A: 270 ở ngưỡng 4), nhiều khả năng đã loại cả khung khác nhau thật.

Ngưỡng phụ thuộc tốc độ xe: lấy mẫu 2 khung/giây, xe chạy chậm hoặc đứng yên thì hai khung liền nhau gần giống nhau nên dễ bị coi là trùng. Drive A chậm hơn (tốc độ trung bình các split 19–44 km/h so với 50–74 km/h ở B) và có đoạn camera đứng hình, nên ở cùng ngưỡng 2 A có 149 khung trùng còn B chỉ 52.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
