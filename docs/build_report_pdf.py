#!/usr/bin/env python3
"""Build the Project 1/4 analysis PDF from its Markdown source."""

from __future__ import annotations

import argparse
import html
import re
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen.canvas import Canvas
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    KeepTogether,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)


ROOT = Path(__file__).resolve().parent
FONT = Path("/mnt/c/Windows/Fonts/simhei.ttf")
FOOTER_TEXT = "TinyKV 项目分析报告"


def inline_markup(text: str) -> str:
    escaped = html.escape(text)
    escaped = re.sub(r"`([^`]+)`", r'<font color="#7A2430">\1</font>', escaped)
    escaped = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", escaped)
    return escaped


class NumberedCanvas(Canvas):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_pages = []

    def showPage(self):
        self._saved_pages.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        total = len(self._saved_pages)
        for state in self._saved_pages:
            self.__dict__.update(state)
            self.setFont("SimHei", 8)
            self.setFillColor(colors.HexColor("#666666"))
            self.drawString(18 * mm, 10 * mm, FOOTER_TEXT)
            self.drawRightString(192 * mm, 10 * mm, f"第 {self._pageNumber} / {total} 页")
            super().showPage()
        super().save()


def make_styles():
    base = getSampleStyleSheet()
    common = dict(fontName="SimHei", textColor=colors.HexColor("#202124"))
    return {
        "title": ParagraphStyle(
            "TitleCN", parent=base["Title"], fontSize=22, leading=31,
            alignment=TA_CENTER, spaceAfter=16, **common
        ),
        "h1": ParagraphStyle(
            "H1CN", parent=base["Heading1"], fontSize=16, leading=23,
            textColor=colors.HexColor("#163A5F"), spaceBefore=13, spaceAfter=8,
            keepWithNext=True, fontName="SimHei"
        ),
        "h2": ParagraphStyle(
            "H2CN", parent=base["Heading2"], fontSize=13, leading=19,
            textColor=colors.HexColor("#245B86"), spaceBefore=10, spaceAfter=6,
            keepWithNext=True, fontName="SimHei"
        ),
        "h3": ParagraphStyle(
            "H3CN", parent=base["Heading3"], fontSize=11, leading=17,
            textColor=colors.HexColor("#2D648B"), spaceBefore=7, spaceAfter=4,
            keepWithNext=True, fontName="SimHei"
        ),
        "body": ParagraphStyle(
            "BodyCN", parent=base["BodyText"], fontSize=9.4, leading=15,
            alignment=TA_JUSTIFY, wordWrap="CJK", spaceAfter=5, **common
        ),
        "bullet": ParagraphStyle(
            "BulletCN", parent=base["BodyText"], fontSize=9.2, leading=14.5,
            leftIndent=14, firstLineIndent=-8, wordWrap="CJK", spaceAfter=3,
            bulletIndent=4, **common
        ),
        "table_head": ParagraphStyle(
            "TableHeadCN", parent=base["BodyText"], fontSize=8.4, leading=12,
            alignment=TA_CENTER, wordWrap="CJK", textColor=colors.white,
            fontName="SimHei"
        ),
        "table": ParagraphStyle(
            "TableCN", parent=base["BodyText"], fontSize=8.1, leading=12.2,
            wordWrap="CJK", textColor=colors.HexColor("#202124"),
            fontName="SimHei"
        ),
        "meta": ParagraphStyle(
            "MetaCN", parent=base["BodyText"], fontSize=9.5, leading=15,
            alignment=TA_CENTER, textColor=colors.HexColor("#666666"),
            fontName="SimHei", spaceAfter=15
        ),
    }


def table_widths(column_count: int):
    usable = A4[0] - 36 * mm
    if column_count == 3:
        return [14 * mm, 48 * mm, usable - 62 * mm]
    if column_count == 4:
        return [54 * mm, 16 * mm, 16 * mm, usable - 86 * mm]
    return [usable / column_count] * column_count


def make_table(rows, styles):
    rendered = []
    for row_index, row in enumerate(rows):
        style = styles["table_head"] if row_index == 0 else styles["table"]
        rendered.append([Paragraph(inline_markup(cell), style) for cell in row])
    table = Table(
        rendered,
        colWidths=table_widths(len(rows[0])),
        repeatRows=1,
        hAlign="LEFT",
    )
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#245B86")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#B9C5CF")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.white, colors.HexColor("#F3F7FA")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return table


def parse_markdown(text: str, styles):
    lines = text.splitlines()
    story = []
    paragraph = []
    table_rows = []

    def flush_paragraph():
        nonlocal paragraph
        if paragraph:
            story.append(Paragraph(inline_markup(" ".join(paragraph)), styles["body"]))
            paragraph = []

    def flush_table():
        nonlocal table_rows
        if table_rows:
            story.append(make_table(table_rows, styles))
            story.append(Spacer(1, 5))
            table_rows = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("|") and stripped.endswith("|"):
            flush_paragraph()
            cells = [cell.strip() for cell in stripped.strip("|").split("|")]
            if all(re.fullmatch(r":?-{3,}:?", cell) for cell in cells):
                continue
            table_rows.append(cells)
            continue

        flush_table()
        if not stripped:
            flush_paragraph()
            continue
        if stripped.startswith("# "):
            flush_paragraph()
            story.append(Spacer(1, 31 * mm))
            story.append(Paragraph(inline_markup(stripped[2:]), styles["title"]))
        elif stripped.startswith("## "):
            flush_paragraph()
            story.append(Paragraph(inline_markup(stripped[3:]), styles["h1"]))
        elif stripped.startswith("### "):
            flush_paragraph()
            story.append(Paragraph(inline_markup(stripped[4:]), styles["h2"]))
        elif stripped.startswith("#### "):
            flush_paragraph()
            story.append(Paragraph(inline_markup(stripped[5:]), styles["h3"]))
        elif stripped.startswith("- "):
            flush_paragraph()
            story.append(Paragraph("· " + inline_markup(stripped[2:]), styles["bullet"]))
        elif re.match(r"^\d+\.\s", stripped):
            flush_paragraph()
            number, content = stripped.split(".", 1)
            story.append(Paragraph(
                f"{number}. {inline_markup(content.strip())}", styles["bullet"]
            ))
        elif stripped.startswith("生成日期："):
            flush_paragraph()
            story.append(Paragraph(inline_markup(stripped), styles["meta"]))
        else:
            paragraph.append(stripped)

    flush_paragraph()
    flush_table()
    return story


def build(source: Path, output: Path):
    global FOOTER_TEXT
    if not FONT.exists():
        raise FileNotFoundError(f"Chinese font not found: {FONT}")
    pdfmetrics.registerFont(TTFont("SimHei", str(FONT)))
    styles = make_styles()
    markdown = source.read_text(encoding="utf-8")
    title = markdown.splitlines()[0].removeprefix("# ").strip()
    project_match = re.search(r"Project \d+ 与 Project \d+", title)
    if project_match:
        FOOTER_TEXT = f"TinyKV {project_match.group(0).replace(' 与 ', ' / ')} 分析报告"
    doc = BaseDocTemplate(
        str(output),
        pagesize=A4,
        rightMargin=18 * mm,
        leftMargin=18 * mm,
        topMargin=17 * mm,
        bottomMargin=17 * mm,
        title=title,
        author="Codex",
        subject="TinyKV Project 1 / Project 4 change and test analysis",
    )
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="normal")
    doc.addPageTemplates([PageTemplate(id="report", frames=[frame])])
    story = parse_markdown(markdown, styles)
    doc.build(story, canvasmaker=NumberedCanvas)
    print(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "source",
        nargs="?",
        type=Path,
        default=ROOT / "project1_project4_change_report.md",
    )
    parser.add_argument("-o", "--output", type=Path)
    args = parser.parse_args()
    output = args.output or args.source.with_suffix(".pdf")
    build(args.source.resolve(), output.resolve())
