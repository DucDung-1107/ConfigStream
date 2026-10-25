# Ghi chú thuyết trình — Checkpoint 26/10/2026

Slide tiếng Anh; ghi chú tiếng Việt. Tổng thời lượng dự kiến: khoảng 8 phút 50 giây. Chỉnh nội dung gốc trong `main.tex` rồi cập nhật bản ghi chú này.

## 1. ConfigStream — Group 4

Khoảng 25 giây. Chào thầy và các bạn. Nhóm 4 trình bày ConfigStream, hệ thống lưu trữ và phân tích thay đổi cấu hình. Buổi checkpoint này tập trung vào bài toán, dữ liệu, kiến trúc và những phần nhóm đã triển khai được. Chưa trình bày như một sản phẩm đã hoàn thiện hoặc đã chứng minh khả năng mở rộng.

## 2. The problem: configuration changes over time

Khoảng 45 giây. Mở đầu bằng ví dụ cụ thể: cùng một router nhưng phiên bản sau tắt logging và bật telnet. Đây là dữ liệu minh hoạ do nhóm tạo, không phải sự cố thực tế. Nếu chỉ giữ file hiện tại, ta không biết thay đổi xảy ra lúc nào và khác phiên bản trước ra sao. Nhóm muốn lưu lịch sử và chỉ ra phần thay đổi thay vì chỉ giữ trạng thái cuối cùng.

## 3. What ConfigStream provides

Khoảng 35 giây. Giải thích ba việc hệ thống làm: lưu phiên bản đã được chấp nhận, so sánh các phiên bản và tra cứu lịch sử cùng cảnh báo theo rule. Asset là đối tượng như router hoặc một file cấu hình. ID của asset giúp nối lịch sử lại với nhau. Cảnh báo là kết quả rule, không đồng nghĩa với một sự cố bảo mật đã được xác nhận.

## 4. Late arrivals should not create the wrong diff

Khoảng 55 giây. Chỉ vào hàng trên: dữ liệu có thể đến theo thứ tự 1, 3, 2. Nếu so sánh theo thứ tự đến, ta sẽ tạo cặp 1 sang 3 và bỏ qua phiên bản 2. Hàng dưới là cách nhóm xử lý: giữ các phiên bản theo asset và chỉ tạo diff giữa các phiên bản liền kề. Nếu 2 đến khi state còn giữ được 1 và 3 thì tạo đủ hai cặp đúng. State có giới hạn lưu giữ, nên không hứa sửa được mọi dữ liệu đến muộn vô hạn.

## 5. System architecture

Khoảng 65 giây. Đi theo mũi tên: snapshot đi qua Kafka, Spark chuẩn hoá và so sánh phiên bản, rồi ghi lịch sử vào MinIO dưới dạng Parquet. Nhánh bên dưới ghi metadata vào Elasticsearch, FastAPI cung cấp API và dashboard hiển thị. Lịch sử Parquet là dữ liệu gốc để đọc lại; search là lớp phục vụ truy vấn. Trong code, lake được ghi trước search. Sơ đồ rút gọn đường cấu hình; các bộ dữ liệu RPKI, RIS, DNS có pipeline riêng và không bị ép qua bộ so sánh cấu hình này. Nếu thầy hỏi analytics batch, nó đọc Parquet đã commit.

## 6. Data is ready for development and replay

Khoảng 50 giây. Nhóm có hai loại dữ liệu. Bộ tạo dữ liệu giúp chủ động kiểm tra thứ tự đảo, dữ liệu lặp và các thay đổi biết trước. TerraDS là lịch sử mã hạ tầng từ Git: 6.505 lịch sử file, 10.188 phiên bản và 3.683 cặp liền kề được chấp nhận. Không gọi đây là 10.188 lần triển khai ngoài thực tế. Dữ liệu lớn không đưa hết vào Git; repo giữ nguồn gốc, manifest và kết quả. Nếu được hỏi thêm, nhóm còn có dữ liệu RPKI, RIS và DNS, nhưng hôm nay tập trung đường xử lý cấu hình để dễ theo dõi.

## 7. Implemented: differences and rule alerts

Khoảng 45 giây. Quay lại ví dụ đầu: output chỉ ra các dòng bị bỏ và thêm. Rule mặc định báo logging bị tắt ở mức high và telnet được bật ở mức critical. Đây là rule minh hoạ đã có trong code, không phải mô hình AI dự đoán. Chuẩn hoá xử lý các khác biệt văn bản được hỗ trợ; không phải parser hiểu mọi ý nghĩa cấu hình. Việc gửi lại cùng snapshot cũng không được tạo thêm thay đổi logic mới.

## 8. Executable code is available in Git

Khoảng 50 giây. Mở link repo nếu có mạng hoặc chuyển terminal đã chuẩn bị. Hai lệnh trên chạy demo xử lý thuần Python, không cần dựng Kafka hay MinIO. Không dùng demo nhỏ này để nói cả stack đang online. Nhóm vừa kiểm tra 178 test của state logic, diff và rule. CI của revision mã nguồn 3980ce1 đã đạt trước khi thêm slide. Nếu thầy muốn xem thực thi, chạy make processing-demo; phần output chính chính là ví dụ vừa giải thích.

## 9. A complete local replay has been recorded

Khoảng 40 giây. Đây là kết quả replay đã được ghi nhận trong repo, không phải lần chạy mới trong buổi thuyết trình. 10.188 snapshot qua đường tích hợp và cho 3.683 diff liền kề. Khoảng 29,4 MB là tổng Parquet raw và processed đã commit. Chỉ kết luận đường tích hợp chạy được trên phạm vi local đã thử; không kết luận Spark nhanh hơn baseline hay hệ thống đã đạt quy mô production. Cấu hình replay lịch sử có retention riêng, khác mặc định demo online.

## 10. Dashboard preview

Khoảng 40 giây. Đây là ảnh giao diện thật được mở từ frontend hiện tại. Trang overview tóm tắt dữ liệu đã được chấp nhận ở các miền. Phần lịch sử cấu hình có API để tra cứu theo asset và thời gian khi backend chạy. Các con số trên ảnh thuộc những đơn vị khác nhau, không cộng lại thành một chỉ số tổng. Ảnh này không chứng minh backend đang chạy trực tiếp trong buổi checkpoint.

## 11. Checkpoint readiness

Khoảng 40 giây. Trả lời trực tiếp ba yêu cầu thầy đưa ra. Git đã có code chạy được; dữ liệu và manifest đã chuẩn bị; kiến trúc đã có implementation và minh chứng local. Chỉ vào đúng slide trước tương ứng mỗi hàng. Chưa gọi đây là bài hoàn thành cuối kỳ; vẫn cần củng cố demo tích hợp, đo đạc và cách trình bày kết quả.

## 12. Next milestone

Khoảng 40 giây. Ba việc nhóm dự kiến ưu tiên tiếp: diễn tập demo tích hợp từ đầu, mở rộng đánh giá với đơn vị đo rõ ràng và chạy lại kiểm thử phục hồi trên revision mới nhất. Không nói những việc dự kiến này đã hoàn thành. Bằng chứng hiện có chủ yếu local hoặc single-node; chưa chứng minh HA hay horizontal scalability. Kết thúc và mời thầy góp ý về phạm vi dữ liệu và ưu tiên cho milestone tiếp theo.
