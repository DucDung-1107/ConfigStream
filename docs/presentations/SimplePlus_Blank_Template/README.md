# SimplePlus — template trống

- Sửa `main.tex`: tên bài, tác giả, section và nội dung frame.
- Giữ 4 file `.sty` cùng thư mục với `main.tex`.
- Đặt ảnh trong `figs/`.
- Tỉ lệ 16:9, tiêu đề in đậm, slide tên section tự động ở giữa.
- Các ví dụ bullet, ảnh và citation nằm trong comment, không hiện trên slide.

Biên dịch với pdfLaTeX (Overleaf hoặc TeX Live/MacTeX):

```sh
latexmk -pdf main.tex
```

`main.pdf` là bản xem trước đã biên dịch. Theme SimplePlus của Pin-Yen Huang; thông tin tác giả và giấy phép được giữ trong các file `.sty`.
