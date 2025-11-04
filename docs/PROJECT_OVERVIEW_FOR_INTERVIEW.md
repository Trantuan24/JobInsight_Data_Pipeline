# TỔNG QUAN DỰ ÁN DÀNH CHO PHỎNG VẤN

## Kiến trúc
Dự án JobInsight_Data_Pipeline được xây dựng dựa trên kiến trúc ETL (Extract, Transform, Load). Kiến trúc này bao gồm các thành phần chính sau:

- **Nguồn dữ liệu**: Dữ liệu được lấy từ nhiều nguồn khác nhau như API, cơ sở dữ liệu và file CSV.
- **Quá trình ETL**: Sử dụng Apache Airflow để lập lịch và theo dõi các tác vụ ETL. Nó xử lý dữ liệu, chuyển đổi định dạng và loại bỏ dữ liệu không hợp lệ.
- **Lưu trữ dữ liệu**: Dữ liệu được lưu trữ trong cơ sở dữ liệu PostgreSQL và được tối ưu hóa cho các truy vấn nhanh chóng.
- **Trực quan hóa**: Sử dụng Tableau hoặc một công cụ tương tự để tạo bảng điều khiển trực quan cho người dùng cuối.

## Công nghệ
Dự án sử dụng một loạt các công nghệ hiện đại:

- **Ngôn ngữ lập trình**: Python cho quá trình ETL và Bash cho các script tự động hóa.
- **Công cụ ETL**: Apache Airflow.
- **Cơ sở dữ liệu**: PostgreSQL.
- **Trực quan hóa**: Tableau.
- **Môi trường triển khai**: Docker để container hóa các thành phần của ứng dụng.

## Chỉ số hiệu suất
Một số chỉ số hiệu suất quan trọng được theo dõi trong dự án:

- Thời gian xử lý dữ liệu: Thời gian cần thiết để hoàn thành một tác vụ ETL.
- Độ tin cậy của dữ liệu: Tỉ lệ phần trăm dữ liệu chính xác và đầy đủ sau khi xử lý.
- Tần suất cập nhật dữ liệu: Bao nhiêu lần dữ liệu được cập nhật trong một khoảng thời gian.

## Câu trả lời mẫu
- **Câu hỏi: Bạn đã sử dụng công cụ nào cho quy trình ETL và lý do tại sao?**  
  Trả lời: Tôi đã sử dụng Apache Airflow vì nó cho phép tôi lập lịch và theo dõi các tác vụ một cách linh hoạt. Nó cũng có giao diện trực quan giúp dễ dàng quản lý quy trình.

- **Câu hỏi: Bạn làm thế nào để đảm bảo dữ liệu bạn xử lý là chính xác?**  
  Trả lời: Tôi thực hiện kiểm tra dữ liệu trong quá trình ETL, bao gồm việc xác thực định dạng và kiểm tra tính hợp lệ của dữ liệu trước khi lưu trữ. Tôi cũng duy trì nhật ký chi tiết để theo dõi bất kỳ vấn đề nào có thể phát sinh.

- **Câu hỏi: Bạn đã tối ưu hóa hiệu suất như thế nào trong dự án của mình?**  
  Trả lời: Tôi đã sử dụng chỉ số hiệu suất để xác định các nút cổ chai trong quy trình ETL và thực hiện các phương pháp như lập chỉ mục cơ sở dữ liệu và điều chỉnh các truy vấn SQL để cải thiện hiệu suất.