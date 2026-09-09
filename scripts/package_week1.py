"""Build paginated Week 1 PDFs and a vector architecture diagram.

Requires reportlab, pypdf, pdf2image and Poppler in the generation environment.
This is a documentation tool, not a platform runtime dependency.
"""

import argparse
import os
import re
import sys
import textwrap
from pathlib import Path
from xml.sax.saxutils import escape

from pdf2image import convert_from_path
from pypdf import PdfReader
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.pdfgen import canvas
from reportlab.platypus import (
    KeepTogether,
    PageBreak,
    Paragraph,
    Preformatted,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.storage.report import render

OUT = ROOT / "reports/week1"
QA = ROOT / "tmp/pdfs/week1"
styles = getSampleStyleSheet()
styles.add(
    ParagraphStyle(
        name="BodyCustom", fontName="Helvetica", fontSize=10, leading=14, spaceAfter=8
    )
)
styles.add(
    ParagraphStyle(name="CellCustom", fontName="Helvetica", fontSize=8.4, leading=11)
)
styles["Title"].fontName = "Helvetica-Bold"
styles["Title"].fontSize = 21
styles["Title"].leading = 25
styles["Title"].textColor = colors.black
styles["Title"].alignment = 0
styles["Title"].spaceAfter = 15
for name, size in [("Heading1", 14), ("Heading2", 11)]:
    styles[name].fontSize = size
    styles[name].leading = size + 4
    styles[name].textColor = colors.black
    styles[name].spaceBefore = 10
    styles[name].spaceAfter = 7
    styles[name].keepWithNext = True


def para(text, style="BodyCustom"):
    text = text.removeprefix("> ")
    for old, new in {
        "–": "-",
        "—": "-",
        "×": "x",
        "→": "->",
        "≤": "<=",
        "≥": ">=",
    }.items():
        text = text.replace(old, new)
    text = escape(text).replace("`", "")
    text = text.replace("&lt;br&gt;", "<br/>").replace("&lt;br/&gt;", "<br/>")
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"<i>\1</i>", text)
    return Paragraph(text, styles[style])


def footer(c, doc):
    c.setFont("Helvetica", 8)
    c.setFillColor(colors.HexColor("#556270"))
    c.drawString(43, 25, "Urban Data Platform  |  Week 1")
    c.drawRightString(A4[0] - 43, 25, str(doc.page))


def build(md, target):
    styles["BodyCustom"].fontSize = 9.5 if target.stem == "benchmark_report" else 10
    styles["BodyCustom"].leading = 12.5 if target.stem == "benchmark_report" else 14
    styles["BodyCustom"].spaceAfter = 7 if target.stem == "benchmark_report" else 8
    if target.stem == "task_answers":
        styles["BodyCustom"].fontSize = 9.7
        styles["BodyCustom"].leading = 13.3
        styles["BodyCustom"].spaceAfter = 7
    lines = md.read_text(encoding="utf-8").splitlines()
    story = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue
        if line in ("---", "> [!NOTE]"):
            i += 1
            continue
        if line.startswith("```"):
            code = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                code_line = (
                    lines[i]
                    .replace("├──", "+--")
                    .replace("└──", "+--")
                    .replace("│", "|")
                )
                code.extend(
                    textwrap.wrap(code_line, width=100, replace_whitespace=False)
                    or [""]
                )
                i += 1
            story.append(
                Preformatted(
                    "\n".join(code),
                    ParagraphStyle(
                        "Code", fontName="Courier", fontSize=8, leading=10, spaceAfter=8
                    ),
                )
            )
            i += 1
            continue
        if line == "<!-- PAGEBREAK -->":
            if target.stem != "task_answers":
                story.append(PageBreak())
            i += 1
            continue
        if line.startswith("|"):
            rows = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                cells = [s.strip() for s in lines[i].strip().strip("|").split("|")]
                if not all(re.fullmatch(r"[-: ]+", s) for s in cells):
                    rows.append(cells)
                i += 1
            n = len(rows[0])
            width = A4[0] - 86
            if n == 3:
                widths = [width * 0.22, width * 0.39, width * 0.39]
            elif n == 4:
                widths = [width * 0.37] + [width * 0.21] * 3
            elif n == 5:
                widths = [width * 0.26] + [width * 0.185] * 4
            else:
                widths = [width / n] * n
            data = [[para(s, "CellCustom") for s in row] for row in rows]
            t = Table(data, colWidths=widths, repeatRows=1, hAlign="LEFT")
            t.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#DCE5ED")),
                        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#D9D9D9")),
                        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 7),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
                        ("TOPPADDING", (0, 0), (-1, -1), 7),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                    ]
                )
            )
            group = [t]
            if (
                story
                and isinstance(story[-1], Paragraph)
                and story[-1].style.name in ("Heading1", "Heading2")
            ):
                group.insert(0, story.pop())
            story.extend([KeepTogether(group), Spacer(1, 10)])
            continue
        if line.startswith("# "):
            story.append(para(line[2:], "Title"))
        elif line.startswith("## "):
            story.append(para(line[3:], "Heading1"))
        elif line.startswith(("### ", "#### ")):
            story.append(para(line.lstrip("# "), "Heading2"))
        elif line.startswith("- ") or re.match(r"\d+\. ", line):
            story.append(para(line))
        else:
            chunk = [line]
            while (
                i + 1 < len(lines)
                and lines[i + 1].strip()
                and not lines[i + 1]
                .strip()
                .startswith(("#", "|", "<!--", "```", "- ", "---", ">"))
                and not re.match(r"\d+\. ", lines[i + 1].strip())
            ):
                i += 1
                chunk.append(lines[i].strip())
            story.append(para(" ".join(chunk)))
        i += 1
    SimpleDocTemplate(
        str(target),
        pagesize=A4,
        rightMargin=43,
        leftMargin=43,
        topMargin=39,
        bottomMargin=43,
        title=lines[0].lstrip("# "),
        author="",
    ).build(story, onFirstPage=footer, onLaterPages=footer)


def diagram():
    path = OUT / "architecture.pdf"
    w, h = landscape(A4)
    c = canvas.Canvas(str(path), pagesize=(w, h))
    c.setTitle("Week 1 Urban Data Platform Architecture")
    c.setFont("Helvetica-Bold", 21)
    c.drawString(35, h - 42, "Week 1 Urban Data Platform Architecture")
    c.setFont("Helvetica", 10)
    c.drawString(
        35,
        h - 62,
        "Current YAML configuration  |  Spark batch processing  |  Delta tables addressed by path",
    )

    def box(x, y, bw, bh, title, lines, color="#EEF3F7"):
        c.setFillColor(colors.HexColor(color))
        c.setStrokeColor(colors.HexColor("#8799AA"))
        c.roundRect(x, y, bw, bh, 6, fill=1, stroke=1)
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(x + 11, y + bh - 20, title)
        c.setFont("Helvetica", 9)
        for j, line in enumerate(lines):
            c.drawString(x + 11, y + bh - 37 - j * 13, line)

    def arrow(x, y, xx, yy, dashed=False):
        c.setStrokeColor(colors.HexColor("#465F73"))
        c.setLineWidth(1.2)
        c.setDash(4, 3) if dashed else c.setDash()
        c.line(x, y, xx, yy)
        c.setDash()
        import math

        a = math.atan2(yy - y, xx - x)
        for d in [-0.5, 0.5]:
            c.line(xx, yy, xx - 7 * math.cos(a + d), yy - 7 * math.sin(a + d))

    box(
        35,
        370,
        175,
        120,
        "Original files in data/",
        [
            "Taxi trips: Parquet / Q1 2024",
            "Weather: hourly CSV",
            "Air quality: national EPA CSV",
            "Taxi zones: lookup CSV",
        ],
    )
    box(
        245,
        370,
        255,
        120,
        "Reusable ingestion",
        [
            "Read and check required columns",
            "Rename / cast / scope / normalize",
            "Validate and deduplicate",
            "Assert input row accounting",
        ],
    )
    box(
        540,
        395,
        266,
        95,
        "Bronze Delta snapshots",
        [
            "raw_taxi_trips / raw_weather",
            "raw_air_quality / raw_taxi_zones",
            "Unpartitioned; overwrite",
        ],
    )
    box(
        245,
        195,
        255,
        125,
        "Silver Delta sources",
        [
            "clean_taxi_trips: local year + month",
            "clean_weather: unpartitioned",
            "clean_air_quality: unpartitioned",
            "clean_taxi_zones: unpartitioned",
        ],
    )
    box(
        35,
        195,
        175,
        125,
        "Configuration and audit",
        [
            "YAML: paths, keys, rules, types",
            "Timezones, partitions, version",
            "Delta ingestion metadata",
            "Per-dataset Delta quarantine",
            "JSON execution summaries",
        ],
    )
    box(
        540,
        195,
        266,
        150,
        "Integration and publication",
        [
            "Broadcast endpoint zone / UTC weather joins",
            "Air: site means -> two parallel left joins",
            "Borough-hour -> pm25_borough (nullable)",
            "City-hour -> pm25_citywide (nullable)",
            "Retain fallback pm25 and source separately",
            "Assert trip IDs; integrated Delta: year + month",
        ],
    )
    box(
        245,
        47,
        561,
        98,
        "Storage benchmark",
        [
            "Independent raw-to-clean taxi ingestion for each layout: unpartitioned / daily / monthly",
            "Required queries: trips per borough; duration per day; fare per borough",
            "Fresh Delta layout paths; JSON timings, sizes, file counts and physical plans",
        ],
    )
    arrow(210, 430, 245, 430)
    arrow(500, 439, 540, 439)
    arrow(370, 370, 370, 320)
    arrow(500, 263, 540, 263)
    arrow(245, 262, 210, 262, True)
    arrow(123, 320, 123, 344, True)
    arrow(123, 344, 275, 344, True)
    arrow(275, 344, 275, 370, True)
    arrow(371, 195, 371, 145)
    c.setFont("Helvetica", 8)
    c.drawString(378, 165, "Zone lookup")
    c.setStrokeColor(colors.HexColor("#465F73"))
    c.line(35, 400, 21, 400)
    c.line(21, 400, 21, 95)
    arrow(21, 95, 245, 95)
    c.setFont("Helvetica", 9)
    c.drawString(46, 105, "Raw taxi Parquet")
    c.setFont("Helvetica", 8)
    c.drawString(
        35,
        29,
        "Solid arrows: data/results. Dashed arrows: configuration/audit. Benchmark rereads original taxi files; it does not time integrated input.",
    )
    c.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--metrics",
        required=True,
        type=Path,
        help="Successful raw-taxi full-run JSON to package",
    )
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    QA.mkdir(parents=True, exist_ok=True)
    render(args.metrics, OUT / "benchmark_report.md")
    evidence = OUT / "evidence" / args.metrics.name
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_bytes(args.metrics.read_bytes())
    for source, name in [
        (OUT / "design_report.md", "design_report"),
        (OUT / "benchmark_report.md", "benchmark_report"),
        (OUT / "task_answers.md", "task_answers"),
    ]:
        build(source, OUT / (name + ".pdf"))
    diagram()
    for pdf in OUT.glob("*.pdf"):
        doc = PdfReader(pdf)
        folder = QA / pdf.stem
        folder.mkdir(exist_ok=True)
        pages = convert_from_path(
            str(pdf), dpi=105, poppler_path=os.environ.get("POPPLER_BIN")
        )
        for i, page in enumerate(pages):
            page.save(str(folder / f"page-{i + 1}.png"))
        print(f"{pdf.name}: {len(doc.pages)} pages")
        if pdf.stem == "architecture":
            convert_from_path(
                str(pdf), dpi=150, poppler_path=os.environ.get("POPPLER_BIN")
            )[0].save(str(OUT / "architecture.png"))


if __name__ == "__main__":
    main()
