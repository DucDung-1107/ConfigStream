# Ghi chú thuyết trình — Checkpoint 26/10/2026

Slide tiếng Anh; ghi chú tiếng Việt. Tổng thời lượng dự kiến: khoảng 10 phút. Chỉnh nội dung gốc trong `main.tex` rồi cập nhật bản ghi chú này.

## 1. ConfigStream — Group 4

Khoảng 25 giây. Chào thầy và các bạn. Nhóm 4 trình bày ConfigStream, hệ thống lưu trữ và phân tích thay đổi cấu hình. Buổi checkpoint này tập trung vào bài toán, dữ liệu, kiến trúc và những phần nhóm đã triển khai được. Chưa trình bày như một sản phẩm đã hoàn thiện hoặc đã chứng minh khả năng mở rộng.

## 2. The problem: configuration changes over time

Khoảng 45 giây. Mở đầu bằng ví dụ cụ thể: cùng một router nhưng phiên bản sau tắt logging và bật telnet. Đây là dữ liệu minh hoạ do nhóm tạo, không phải sự cố thực tế. Nếu chỉ giữ file hiện tại, ta không biết thay đổi xảy ra lúc nào và khác phiên bản trước ra sao. Nhóm muốn lưu lịch sử và chỉ ra phần thay đổi thay vì chỉ giữ trạng thái cuối cùng.

## 3. What ConfigStream provides

Khoảng 35 giây. Giải thích ba việc hệ thống làm: lưu phiên bản đã được chấp nhận, so sánh các phiên bản và tra cứu lịch sử cùng cảnh báo theo rule. Asset là đối tượng như router hoặc một file cấu hình. ID của asset giúp nối lịch sử lại với nhau. Cảnh báo là kết quả rule, không đồng nghĩa với một sự cố bảo mật đã được xác nhận.

## 4. Four datasets, four meanings of change

Khoảng 65 giây. Nhóm dùng bốn loại dữ liệu có ý nghĩa thời gian khác nhau. Terraform từ TerraDS là các phiên bản file mã hạ tầng trong Git: so sánh để biết dòng nào đổi, không coi commit là lần triển khai. RPKI là tập bản ghi uỷ quyền định tuyến quan sát theo ngày; so sánh thành viên giữa hai ngày đầy đủ, không suy ra chính xác thao tác của quản trị viên. RIS ghi nhận sự kiện BGP, ví dụ quảng bá hoặc rút tuyến; hai sự kiện giống nội dung vẫn có thể là hai lần xảy ra khác nhau. DNS là phép đo truy vấn và phản hồi: thấy tên miền trả về địa chỉ nào tại thời điểm quan sát, không coi đó là toàn bộ trạng thái DNS. Ví dụ trên slide chỉ minh hoạ cách hiểu. Điểm chung là cần lưu lịch sử; cách định nghĩa thay đổi phải riêng cho từng loại.

## 5. Configuration example: handling late arrivals

Khoảng 55 giây. Chỉ vào hàng trên: dữ liệu có thể đến theo thứ tự 1, 3, 2. Nếu so sánh theo thứ tự đến, ta sẽ tạo cặp 1 sang 3 và bỏ qua phiên bản 2. Hàng dưới là cách nhóm xử lý: giữ các phiên bản theo asset và chỉ tạo diff giữa các phiên bản liền kề. Nếu 2 đến khi state còn giữ được 1 và 3 thì tạo đủ hai cặp đúng. State có giới hạn lưu giữ, nên không hứa sửa được mọi dữ liệu đến muộn vô hạn.

## 6. System architecture: four separate processing paths

Khoảng 60 giây. Bốn hàng là bốn đường xử lý đã có trong repo, không phải tất cả đều đi qua bộ diff cấu hình. Hàng Terraform giữ logic phiên bản và rule. RPKI xử lý tập thành viên của các ngày đầy đủ và thay đổi giữa chúng. RIS giữ các lần xảy ra sự kiện, chỉ hoà giải bản gửi lại cùng danh tính nguồn; không xoá sự kiện chỉ vì nội dung giống nhau. DNS giữ phép đo và nhóm kết quả có thông tin nguồn để so sánh mô tả. Các luồng dùng chung công nghệ Kafka, Spark và lưu Parquet trên MinIO nhưng có schema, topic và metadata commit riêng. Elasticsearch và FastAPI phục vụ đường cấu hình; dashboard hiện có bản tổng hợp bằng chứng của cả bốn miền, không phải API live chung cho mọi miền. Dữ liệu thực ở đây được phát lại từ các nguồn đã cố định.

## 7. Prepared data: accepted scope for each dataset

Khoảng 55 giây. Đây là phạm vi đã được kiểm chứng trong các lần chạy ghi nhận, không phải tổng kích thước mọi nguồn ngoài Internet và không phải lần chạy mới hôm nay. Terraform có 10.188 phiên bản của 6.505 lịch sử file thuộc 375 repo. RPKI có 14 ngày đầy đủ trong phạm vi ripencc.tal; số 14 là số ngày quan sát, không phải số bản ghi bên trong. RIS có khoảng 4,73 triệu lần xảy ra sự kiện trong một giờ ở collector rrc00. DNS có khoảng 2,23 triệu dòng phép đo từ hai đối tượng nguồn .li đã chọn. Không cộng bốn con số vì đơn vị khác nhau. Nhóm còn có generator để chủ động kiểm thử dữ liệu lặp và đến muộn. Dữ liệu lớn lưu ngoài Git; repo giữ manifest và bằng chứng kết quả.

## 8. Implemented: differences and rule alerts

Khoảng 45 giây. Quay lại ví dụ đầu: output chỉ ra các dòng bị bỏ và thêm. Rule mặc định báo logging bị tắt ở mức high và telnet được bật ở mức critical. Đây là rule minh hoạ đã có trong code, không phải mô hình AI dự đoán. Chuẩn hoá xử lý các khác biệt văn bản được hỗ trợ; không phải parser hiểu mọi ý nghĩa cấu hình. Việc gửi lại cùng snapshot cũng không được tạo thêm thay đổi logic mới.

## 9. Executable code is available in Git

Khoảng 50 giây. Mở link repo nếu có mạng hoặc chuyển terminal đã chuẩn bị. Hai lệnh trên chạy demo xử lý thuần Python, không cần dựng Kafka hay MinIO. Không dùng demo nhỏ này để nói cả stack đang online. Nhóm vừa kiểm tra 178 test của state logic, diff và rule. CI của revision mã nguồn 3980ce1 đã đạt trước khi thêm slide. Nếu thầy muốn xem thực thi, chạy make processing-demo; phần output chính chính là ví dụ vừa giải thích.

## 10. A complete local replay has been recorded

Khoảng 40 giây. Đây là kết quả replay đã được ghi nhận trong repo, không phải lần chạy mới trong buổi thuyết trình. 10.188 snapshot qua đường tích hợp và cho 3.683 diff liền kề. Khoảng 29,4 MB là tổng Parquet raw và processed đã commit. Chỉ kết luận đường tích hợp chạy được trên phạm vi local đã thử; không kết luận Spark nhanh hơn baseline hay hệ thống đã đạt quy mô production. Cấu hình replay lịch sử có retention riêng, khác mặc định demo online.

## 11. Dashboard preview

Khoảng 40 giây. Đây là ảnh giao diện thật được mở từ frontend hiện tại. Bốn thẻ phía trên tương ứng Terraform, RPKI, RIS và DNS vừa giới thiệu. Biểu đồ bên dưới là số sự kiện RIS theo thời gian nguồn, không phải throughput xử lý của Spark. Phần lịch sử cấu hình có API để tra cứu theo asset và thời gian khi backend chạy. Các con số trên ảnh thuộc những đơn vị khác nhau, không cộng lại thành một chỉ số tổng. Ảnh này không chứng minh backend đang chạy trực tiếp trong buổi checkpoint.

## 12. Checkpoint readiness

Khoảng 40 giây. Trả lời trực tiếp ba yêu cầu thầy đưa ra. Git đã có code chạy được; dữ liệu và manifest đã chuẩn bị; kiến trúc đã có implementation và minh chứng local. Chỉ vào đúng slide trước tương ứng mỗi hàng. Chưa gọi đây là bài hoàn thành cuối kỳ; vẫn cần củng cố demo tích hợp, đo đạc và cách trình bày kết quả.

## 13. Next milestone

Khoảng 40 giây. Ba việc nhóm dự kiến ưu tiên tiếp: diễn tập demo tích hợp từ đầu, mở rộng đánh giá với đơn vị đo rõ ràng và chạy lại kiểm thử phục hồi trên revision mới nhất. Không nói những việc dự kiến này đã hoàn thành. Bằng chứng hiện có chủ yếu local hoặc single-node; chưa chứng minh HA hay horizontal scalability. Kết thúc và mời thầy góp ý về phạm vi dữ liệu và ưu tiên cho milestone tiếp theo.
