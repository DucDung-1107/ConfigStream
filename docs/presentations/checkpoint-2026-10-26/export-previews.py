"""Synchronize rehearsal notes and contact sheets after building the LaTeX deck.

Requires PyMuPDF: python -m pip install pymupdf
"""

import re
from pathlib import Path

import pymupdf

ROOT = Path(__file__).resolve().parent


def contact_sheet(deck, indices, output):
    columns, tile_width, tile_height = 3, 400, 246
    sheet = pymupdf.open()
    page = sheet.new_page(
        width=columns * tile_width,
        height=((len(indices) + columns - 1) // columns) * tile_height,
    )
    for slot, index in enumerate(indices):
        x = (slot % columns) * tile_width
        y = (slot // columns) * tile_height
        page.insert_text((x + 8, y + 15), f"PDF page {index + 1}", fontsize=9)
        page.show_pdf_page(pymupdf.Rect(x + 6, y + 21, x + 394, y + 239), deck, index)
    page.get_pixmap().save(ROOT / output)
    sheet.close()


def main():
    source = (ROOT / "main.tex").read_text()
    appendix_start = source.index(r"\appendix")
    frames = list(re.finditer(r"\\begin\{frame\}(.*?)\\end\{frame\}", source, re.S))
    main_count = sum(frame.start() < appendix_start for frame in frames)
    sections, seconds = [], 0
    for index, frame in enumerate(frames):
        body = frame.group(1)
        title = re.match(r"(?:\[[^\]]*\])?\{([^{}]*)\}", body)
        title = title.group(1) if title else "ConfigStream — Group 4"
        note = re.search(r"\\vnote\{(.*?)\}\s*$", body, re.S)
        if note is None:
            raise ValueError(f"Missing notes on PDF page {index + 1}")
        note_text = note.group(1).strip()
        if index < main_count:
            timing = re.match(r"Khoảng (\d+) giây\.", note_text)
            if timing is None:
                raise ValueError(f"Missing timing on main slide {index + 1}")
            seconds += int(timing.group(1))
            label = str(index + 1)
        else:
            label = f"A{index - main_count + 1}"
        sections.append(f"## {label}. {title}\n\n{note_text}\n")
    header = (
        "# Ghi chú thuyết trình — Checkpoint 26/10/2026\n\n"
        f"{main_count} slide chính tiếng Anh; "
        f"{len(frames) - main_count} slide phụ cho phần hỏi đáp. "
        f"Ghi chú tiếng Việt. Thời lượng phần chính: khoảng {seconds // 60} phút "
        f"{seconds % 60} giây.\n\n"
        "Nguồn: `main.tex`. Sau khi sửa và build, chạy `python export-previews.py` "
        "để đồng bộ ghi chú và ảnh xem trước.\n\n"
    )
    with pymupdf.open(ROOT / "main.pdf") as deck:
        if len(deck) != len(frames):
            raise ValueError("PDF page count differs from source; rebuild first")
        with pymupdf.open(ROOT / "speaker-notes.pdf") as notes:
            if len(notes) != len(frames):
                raise ValueError("Presenter PDF is stale; rebuild first")
        contact_sheet(deck, list(range(main_count)), "preview.png")
        contact_sheet(deck, list(range(main_count, len(deck))), "appendix-preview.png")
    (ROOT / "speaker-notes.md").write_text(header + "\n".join(sections))
    print(f"Exported {main_count} main + {len(frames) - main_count} appendix slides; {seconds}s")


if __name__ == "__main__":
    main()
