# Reflection — Top 5 Lakehouse Anti-Patterns

**Anti-pattern dễ vướng nhất: Small Files**

Trong hầu hết các dự án có pipeline streaming hoặc micro-batch - đặc biệt là các hệ thống log LLM observability như NB4 - dữ liệu được ghi liên tục theo từng batch nhỏ. Mỗi commit là một file, đúng về kỹ thuật, nhưng càng nhiều commit là càng nhiều file, sau một khoảng thời gian ngắn đã có thể lên hàng ngàn file.

NB2 và NB6 đo trực tiếp hậu quả: 200 commit -> 200 file -> chi phí GET tăng tuyến tính theo số file, không theo dung lượng dữ liệu. Nguy hiểm là không có lỗi nào xảy ra do vẫn đúng về mặt kỹ thuật. Vì thế mà hệ thống vẫn chạy, nhưng sẽ chậm dần và tốn tiền hơn.

Anti-pattern này dễ vướng vì giải pháp (OPTIMIZE/compaction) không phải là một tính năng của pipeline, mà là một **job bảo trì riêng biệt** cần được lên lịch chủ động. Nếu không có cron job compaction từ đầu, technical debt này sẽ âm thầm tích lũy cho đến khi hoá đơn storage hoặc query timeout buộc team phải xử lý, lúc này đã là quá muộn do chi phí cao.

*Nguyễn Thành An — K3-01017 — Track 2 Day 18*
