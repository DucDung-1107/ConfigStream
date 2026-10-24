import fs from "node:fs/promises";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const outputDir = "D:/ConfigStream/doc/report";
const outputPath = `${outputDir}/configstream_presentation_scripts.xlsx`;
const previewPath = "D:/ConfigStream/tmp_xlsx_builder/configstream_presentation_scripts.png";
await fs.mkdir(outputDir, { recursive: true });

const rows = [
  ["Slide", "English presentation script", "Vietnamese presentation script"],
  ["1", "This project asks how to reconstruct configuration history without confusing arrival time with version time. The v1, v2, and v3 labels are only a small example of the integer version field used by the system.", "Dự án này đặt câu hỏi: làm thế nào để tái dựng lịch sử cấu hình mà không nhầm thời điểm nhận dữ liệu với thứ tự phiên bản. Các nhãn v1, v2 và v3 chỉ là ví dụ nhỏ cho trường version dạng số nguyên trong hệ thống."],
  ["2", "A valid record can arrive late. If we compare records in arrival order, the sequence v1, v3, v2 may create the false pair v1 to v3. The method instead accepts only adjacent producer versions: v1 to v2 and v2 to v3.", "Một bản ghi hợp lệ có thể đến muộn. Nếu so sánh theo thứ tự nhận, chuỗi v1, v3, v2 có thể tạo ra cặp sai v1 đến v3. Phương pháp chỉ chấp nhận các phiên bản kế tiếp nhau: v1 đến v2 và v2 đến v3."],
  ["3", "This matters because one wrong pair can become a false diff, a false alert, or an audit problem. The same risk appears when a predecessor is missing, a duplicate is received, or a projection is updated before the authoritative history is committed.", "Điều này quan trọng vì một cặp sai có thể tạo ra diff giả, cảnh báo giả hoặc khó kiểm toán. Rủi ro tương tự xuất hiện khi thiếu phiên bản trước, nhận bản ghi trùng, hoặc projection được cập nhật trước khi lịch sử chính thức được commit."],
  ["4", "The checkpoint answers three questions: Can we reconstruct valid adjacent states under disorder and duplicates? Can committed history stay independent from rebuildable views? And can we compare workloads fairly when their evidence units are different?", "Checkpoint trả lời ba câu hỏi: Hệ thống có tái dựng được các trạng thái kế tiếp khi dữ liệu lộn xộn và bị trùng không? Lịch sử đã commit có độc lập với các view có thể dựng lại không? Và có thể so sánh công bằng các workload có đơn vị bằng chứng khác nhau không?"],
  ["5", "The method has one common structure: each source enters through its own claim gate, becomes a typed temporal record with identity and provenance, and reaches one authoritative commit boundary. Analytics and serving views are downstream projections that can be rebuilt.", "Phương pháp có một cấu trúc chung: mỗi nguồn đi qua claim gate riêng, trở thành temporal record có kiểu, identity và provenance, rồi đi tới một ranh giới commit chính thức. Analytics và serving views là các projection phía sau và có thể dựng lại."],
  ["6", "For configuration history, the buffer waits for a missing predecessor. With arrival v1, v3, v2, it stores v1 and v3 but emits nothing. When v2 arrives, the sorted buffer is v1, v2, v3, so it emits v1 to v2 and v2 to v3. A retry is a duplicate; a missing version becomes a gap diagnostic, not an invented transition.", "Với lịch sử cấu hình, buffer chờ phiên bản trước còn thiếu. Khi nhận v1, v3, v2, hệ thống giữ v1 và v3 nhưng chưa phát cặp nào. Khi v2 đến, buffer được sắp thành v1, v2, v3 nên phát hai cặp v1 đến v2 và v2 đến v3. Bản ghi gửi lại là duplicate; phiên bản bị thiếu được ghi là gap, không được tự tạo thành transition."],
  ["7", "The temporal rule is workload-specific. RPKI compares a complete coordinate inventory from 1 to N. RIS keeps repeated peer-prefix occurrences as separate evidence. DNS keeps coverage and vantage explicit, even when a value is unknown. All three produce typed records with identity and provenance, but they do not share one universal gate.", "Quy tắc temporal phụ thuộc từng workload. RPKI chỉ so sánh khi inventory tọa độ từ 1 đến N đầy đủ. RIS giữ các occurrence peer-prefix lặp lại như các bằng chứng riêng. DNS giữ rõ coverage và vantage, kể cả khi giá trị chưa biết. Cả ba đều tạo typed record có identity và provenance, nhưng không dùng chung một gate duy nhất."],
  ["8", "Publication separates authoritative history from serving views. Records are written to Parquet, checked through a manifest with file inventory, row counts, SHA-256, and provenance, and only then committed. If search or the API fails, it is rebuilt from committed Parquet. This does not claim one distributed exactly-once transaction.", "Publication tách lịch sử chính thức khỏi các serving view. Record được ghi vào Parquet, kiểm tra qua manifest gồm danh sách file, số dòng, SHA-256 và provenance, rồi mới commit. Nếu search hoặc API lỗi, view được dựng lại từ Parquet đã commit. Quy trình này không đồng nghĩa với một giao dịch distributed exactly-once."],
  ["9", "The evaluation uses the evidence unit appropriate to each workload: file version for TerraDS, canonical row or member for RPKI, peer-prefix occurrence for RIS, and measurement or grouped row for DNS. We should not pool rates across these units.", "Đánh giá dùng đơn vị bằng chứng phù hợp với từng workload: file version cho TerraDS, canonical row hoặc member cho RPKI, peer-prefix occurrence cho RIS, và measurement hoặc grouped row cho DNS. Không nên gộp các rate có mẫu số khác nhau."],
  ["10", "In the TerraDS replay, 10,188 snapshots produced 3,683 adjacent pairs, with 68.742 seconds for completion including drain. The run reported zero alerts and zero diagnostics under the stated configuration. This is a bounded replay result, not a universal capacity claim.", "Trong replay TerraDS, 10.188 snapshot tạo ra 3.683 cặp kế tiếp, với thời gian hoàn tất gồm drain là 68,742 giây. Run này ghi nhận 0 alert và 0 diagnostic trong cấu hình đã nêu. Đây là kết quả của một replay có phạm vi rõ ràng, không phải claim về năng lực chung."],
  ["11", "The matched-processing campaign used 27 trials: nine configurations and three fresh-process repetitions. The normalized rows matched across the compared modes. These timings cover the normalized core only; they do not include every production parsing, finalization, or paced-replay cost.", "Chiến dịch matched-processing gồm 27 trial: chín cấu hình và ba lần chạy process mới cho mỗi cấu hình. Các normalized row khớp giữa các mode được so sánh. Các timing này chỉ đo normalized core, chưa bao gồm toàn bộ chi phí parsing production, finalization hoặc paced replay."],
  ["12", "The RPKI layout comparison measured 47,756,423 bytes for R1, 3,555,516 for R2, and 6,954,890 for R3. Relative to R1, R2 reduced storage by 92.55 percent and R3 by 85.44 percent for the tested query set.", "So sánh layout RPKI đo được 47.756.423 byte cho R1, 3.555.516 byte cho R2 và 6.954.890 byte cho R3. So với R1, R2 giảm 92,55 phần trăm và R3 giảm 85,44 phần trăm trong query set đã thử nghiệm."],
  ["13", "The current evidence supports tested reconstruction, typed records, an authoritative Parquet boundary, and rebuildable views. It does not yet verify paced RIS replay, end-to-end recovery after interrupted publication, repeated fault recovery, or multi-node scale. We therefore do not claim high availability, cluster-wide scalability, or distributed exactly-once processing.", "Bằng chứng hiện có hỗ trợ reconstruction đã thử nghiệm, typed record, ranh giới Parquet chính thức và các view có thể dựng lại. Nó chưa kiểm chứng paced RIS replay, recovery end-to-end sau khi publication bị gián đoạn, recovery qua nhiều lỗi liên tiếp, hoặc scale nhiều node. Vì vậy chưa claim high availability, cluster-wide scalability hay distributed exactly-once processing."],
  ["14", "The next steps answer specific evidence gaps. First freeze the evidence pack. Then run the paced RIS replay. Next execute projection and checkpoint fault tests. Finally update the paper and slides only from accepted artifacts. The main lesson is simple: trustworthy history requires identity, order, provenance, and recovery.", "Các bước tiếp theo trả lời từng khoảng trống bằng chứng cụ thể. Trước hết cố định evidence pack. Sau đó chạy paced RIS replay. Tiếp theo kiểm thử lỗi ở projection và checkpoint. Cuối cùng chỉ cập nhật paper và slide từ artifact đã được chấp nhận. Bài học chính là: lịch sử đáng tin cần identity, order, provenance và recovery."],
];

const workbook = Workbook.create();
const sheet = workbook.worksheets.add("Presentation scripts");
sheet.showGridLines = false;
sheet.tabColor = "#1F4E78";
sheet.getRange("A1:C15").values = rows;

const all = sheet.getRange("A1:C15");
all.format.font = { name: "Arial", size: 11, color: "#1F2937" };
all.format.verticalAlignment = "center";
all.format.wrapText = true;
all.format.borders = { preset: "inside", style: "thin", color: "#D9E2F3" };

const header = sheet.getRange("A1:C1");
header.format = {
  fill: "#1F4E78",
  font: { name: "Arial", size: 11, bold: true, color: "#FFFFFF" },
  verticalAlignment: "center",
  horizontalAlignment: "center",
  wrapText: true,
  borders: { preset: "all", style: "thin", color: "#FFFFFF" },
};
header.format.rowHeight = 30;

sheet.getRange("A2:A15").format = {
  fill: "#EAF2F8",
  font: { name: "Arial", size: 11, bold: true, color: "#1F4E78" },
  horizontalAlignment: "center",
  verticalAlignment: "center",
  wrapText: false,
};

for (let row = 2; row <= 15; row++) {
  sheet.getRange(`B${row}:C${row}`).format.fill = row % 2 === 0 ? "#FFFFFF" : "#F7FAFC";
  sheet.getRange(`A${row}:C${row}`).format.rowHeight = [7, 8, 13].includes(row) ? 94 : 72;
}

sheet.getRange("A1:A15").format.columnWidth = 10;
sheet.getRange("B1:B15").format.columnWidth = 78;
sheet.getRange("C1:C15").format.columnWidth = 78;
sheet.freezePanes.freezeRows(1);
const table = sheet.tables.add("A1:C15", true, "PresentationScripts");
table.style = "TableStyleMedium2";
table.showFilterButton = true;

workbook.recalculate();
const inspect = await workbook.inspect({ kind: "table", range: "Presentation scripts!A1:C15", include: "values", tableMaxRows: 15, tableMaxCols: 3, tableMaxCellChars: 120 });
console.log(inspect.ndjson);
const preview = await workbook.render({ sheetName: "Presentation scripts", range: "A1:C15", scale: 1, format: "png" });
await fs.writeFile(previewPath, new Uint8Array(await preview.arrayBuffer()));
const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(outputPath);
console.log(`SAVED ${outputPath}`);
