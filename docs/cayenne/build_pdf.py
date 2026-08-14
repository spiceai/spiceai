#!/usr/bin/env python3
"""Render cayenne.md -> styled HTML (with locally rendered mermaid SVGs) -> PDF.

Diagrams render through mermaid-cli (`mmdc`), pinned in package.json and
installed with `npm ci` in this directory. Rendering therefore depends on
nothing but the checkout: no network call is made, so a build cannot fail for
reasons outside this repository, and the same commit renders the same diagrams
every time.
"""
import json, re, shutil, subprocess, sys, tempfile
from pathlib import Path
import markdown as md
import os

# Run from this script's directory so every relative path (cayenne.md, the
# referenced SVG assets, the outputs, and WeasyPrint's base_url=".") resolves
# regardless of the caller's working directory.
os.chdir(os.path.dirname(os.path.abspath(__file__)))

SRC = "cayenne.md"
HTML_OUT = "cayenne.html"
PDF_OUT = "Cayenne.pdf"

# Init prefix forces native SVG <text> labels (WeasyPrint can't render foreignObject)
# plus a clean indigo/slate theme that works across flowchart / sequence / ER diagrams.
INIT = (
    '%%{init: {'
    '"htmlLabels": false, '
    '"flowchart": {"htmlLabels": false, "curve": "basis", "nodeSpacing": 45, "rankSpacing": 50}, '
    '"securityLevel": "loose", '
    '"theme": "base", '
    '"themeVariables": {'
    '"fontFamily": "Helvetica, Arial, sans-serif", '
    '"fontSize": "15px", '
    '"primaryColor": "#ffffff", '
    '"primaryBorderColor": "#312e81", '
    '"primaryTextColor": "#0f172a", '
    '"lineColor": "#1e293b", '
    '"secondaryColor": "#e2e8f0", '
    '"tertiaryColor": "#f8fafc", '
    '"actorBkg": "#ffffff", '
    '"actorBorder": "#312e81", '
    '"actorTextColor": "#0f172a", '
    '"signalColor": "#1e293b", '
    '"signalTextColor": "#0f172a", '
    '"labelBoxBkgColor": "#e2e8f0", '
    '"labelBoxBorderColor": "#475569", '
    '"noteBkgColor": "#e2e8f0", '
    '"noteBorderColor": "#475569", '
    '"clusterBkg": "#f8fafc", '
    '"clusterBorder": "#6366f1"'
    '}}}%%\n'
)


# High-contrast override stylesheet injected into every SVG. Class-only selectors
# + !important so it beats mermaid's #container-scoped defaults regardless of the
# generated id, across flowchart / sequence / ER diagrams. This is what makes the
# lines dark and thick, the borders bold, and the subgraph outlines visible.
OVERRIDE_CSS = """
/* connectors — near-black, thick: reads strongly in grayscale */
.edgePath .path, .flowchart-link { stroke:#1e293b !important; stroke-width:2px !important; }
.marker, marker path, .arrowheadPath, .arrowMarkerPath { fill:#1e293b !important; stroke:#1e293b !important; }
/* flowchart nodes — WHITE fill + very dark SOLID border (max luminance vs everything else) */
.node rect, .node circle, .node ellipse, .node polygon, .node path {
  fill:#ffffff !important; stroke:#312e81 !important; stroke-width:2px !important; }
.nodeLabel, .node .label text, .node .label, .node .nodeLabel, .node span, .node p {
  fill:#0f172a !important; color:#0f172a !important; font-weight:500 !important; }
/* subgraph clusters — faint fill + DASHED border: distinguished from solid node borders
   by line STYLE, which survives grayscale where hue would not */
.cluster rect { fill:#f8fafc !important; stroke:#6366f1 !important; stroke-width:1.75px !important;
  stroke-dasharray:7 4 !important; rx:6 !important; ry:6 !important; }
.cluster-label text, .cluster text, .cluster-label span, .cluster span, .cluster-label p, .cluster-label .nodeLabel {
  fill:#1e293b !important; color:#1e293b !important; font-weight:700 !important; }
/* edge labels — opaque white so they read on top of dark lines */
.edgeLabel, .edgeLabel p { background-color:#ffffff !important; color:#0f172a !important; fill:#0f172a !important; }
.edgeLabel rect { fill:#ffffff !important; opacity:1 !important; }
.edgeLabel foreignObject, .label foreignObject { overflow:visible !important; }
/* sequence: actors match nodes (white + dark solid border) */
.actor { fill:#ffffff !important; stroke:#312e81 !important; stroke-width:2px !important; }
text.actor, text.actor > tspan { fill:#0f172a !important; font-weight:600 !important; stroke:none !important; }
.actor-line { stroke:#94a3b8 !important; stroke-width:1px !important; }
.messageLine0, .messageLine1 { stroke:#1e293b !important; stroke-width:1.75px !important; }
.messageText { fill:#0f172a !important; stroke:none !important; font-weight:500 !important; }
line.loopLine, .loopLine { stroke:#64748b !important; stroke-width:1.5px !important; }
.labelBox { fill:#e2e8f0 !important; stroke:#475569 !important; stroke-width:1.5px !important; }
.labelText, .labelText > tspan, .loopText, .loopText > tspan { fill:#1e293b !important; font-weight:600 !important; stroke:none !important; }
/* notes — neutral GRAY fill (clearly lower luminance than white nodes) + solid dark border */
.note { fill:#e2e8f0 !important; stroke:#475569 !important; stroke-width:1.5px !important; }
.noteText, .noteText > tspan { fill:#1e293b !important; stroke:none !important; font-weight:500 !important; }
/* activation bars — mid gray fill so they show on the lifeline */
rect.activation0, rect.activation1, rect.activation2 { fill:#cbd5e1 !important; stroke:#475569 !important; stroke-width:1px !important; }
/* ER (if present) */
.er.entityBox { fill:#ffffff !important; stroke:#312e81 !important; stroke-width:2px !important; }
.er.attributeBoxOdd { fill:#ffffff !important; stroke:#94a3b8 !important; }
.er.attributeBoxEven { fill:#e2e8f0 !important; stroke:#94a3b8 !important; }
.er.relationshipLine { stroke:#1e293b !important; stroke-width:1.75px !important; fill:none !important; }
.er.entityLabel, .entityLabel { fill:#0f172a !important; font-weight:700 !important; }
.er.relationshipLabel { fill:#1e293b !important; font-weight:500 !important; }
"""


def find_mmdc() -> list[str]:
    """The mermaid-cli command, preferring this directory's pinned install.

    `npm ci` here puts the pinned binary in `node_modules/.bin`, which is what
    both CI and a local build should use — a global `mmdc` on PATH is whatever
    version that machine happens to carry, and mermaid reflows diagrams between
    releases. PATH is the fallback so someone with a global install isn't forced
    to run `npm ci`, at the cost of matching CI's output exactly.
    """
    local = Path("node_modules/.bin/mmdc")
    if local.is_file():
        return [str(local)]
    found = shutil.which("mmdc")
    if found:
        print(f"warning: using {found}; run 'npm ci' here to render with the pinned version.",
              file=sys.stderr, flush=True)
        return [found]
    raise RuntimeError(
        "mermaid-cli (mmdc) not found. Run 'npm ci' in docs/cayenne to install the "
        "pinned version (it downloads a headless browser on first install)."
    )


def render_diagrams(sources: list[str]) -> list[str]:
    """Render every mermaid block to a styled SVG, in one mermaid-cli run.

    mermaid-cli renders in a headless browser, so each invocation pays a browser
    launch. Feeding it one markdown file holding every diagram pays that once
    rather than 21 times; it writes `<stem>-1.svg`, `<stem>-2.svg`, ... in
    document order, which is the order the blocks were appended in.
    """
    if not sources:
        return []
    cmd = find_mmdc()
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        # A per-diagram `%%{init}%%` in the source wins; INIT supplies the shared
        # theme for every block that carries none.
        doc = "\n\n".join(
            "```mermaid\n" + ("" if s.lstrip().startswith("%%{init") else INIT) + s + "\n```"
            for s in sources
        )
        src = tmp / "diagrams.md"
        src.write_text(doc, encoding="utf-8")
        # `--no-sandbox` because CI runners commonly cannot use the browser
        # sandbox; the only input is this repository's own diagram source.
        puppeteer_cfg = tmp / "puppeteer.json"
        puppeteer_cfg.write_text(json.dumps({"args": ["--no-sandbox", "--disable-dev-shm-usage"]}),
                                 encoding="utf-8")
        out = tmp / "rendered.md"
        print(f"Rendering {len(sources)} mermaid diagrams with {cmd[0]}...", flush=True)
        proc = subprocess.run(
            cmd + ["-i", str(src), "-o", str(out), "-p", str(puppeteer_cfg), "--quiet"],
            capture_output=True, text=True, check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"mermaid-cli failed (exit {proc.returncode}).\n"
                f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
            )
        svgs = []
        for i in range(1, len(sources) + 1):
            path = out.with_name(f"{out.stem}-{i}.svg")
            if not path.is_file():
                raise RuntimeError(
                    f"mermaid-cli reported success but produced no SVG for diagram {i} "
                    f"({path.name}). stderr:\n{proc.stderr}"
                )
            svgs.append(style_svg(path.read_text(encoding="utf-8")))
    print(" done.", flush=True)
    return svgs


def style_svg(svg: str) -> str:
    """Make a rendered SVG scale to its column and read well in print."""
    svg = re.sub(r"<svg ", '<svg preserveAspectRatio="xMidYMid meet" ', svg, count=1)
    # one merged style: scale to container AND let labels draw outside the box.
    # native-SVG text (htmlLabels off) is under-measured by mermaid, so the
    # computed viewBox can be a hair too tight; overflow:visible + a padded
    # viewBox stops the right/bottom edges from clipping.
    if re.search(r'style="max-width:[^"]*"', svg):
        svg = re.sub(r'style="max-width:[^"]*"', 'style="max-width:100%;overflow:visible"', svg, count=1)
    else:
        svg = re.sub(r"(<svg )", r'\1style="max-width:100%;overflow:visible" ', svg, count=1)
    m = re.search(r'viewBox="([\-\d.]+ [\-\d.]+ [\-\d.]+ [\-\d.]+)"', svg)
    if m:
        x, y, w, h = map(float, m.group(1).split())
        svg = svg.replace(
            f'viewBox="{m.group(1)}"',
            f'viewBox="{x-6} {y-6} {w + w*0.06 + 18} {h + h*0.03 + 12}"', 1)
    # inject high-contrast overrides (placed after mermaid's own <style> so it wins)
    if "</style>" in svg:
        svg = svg.replace("</style>", "</style><style>" + OVERRIDE_CSS + "</style>", 1)
    else:
        svg = re.sub(r"(<svg[^>]*>)", r"\1<style>" + OVERRIDE_CSS + "</style>", svg, count=1)
    return svg


with open(SRC, encoding="utf-8") as f:
    text = f.read()

# Extract mermaid fenced blocks
blocks = []
def grab(m):
    blocks.append(m.group(1))
    return f"\n@@MERMAID{len(blocks)-1}@@\n"
text = re.sub(r"```mermaid\n(.*?)\n```", grab, text, flags=re.DOTALL)

svgs = render_diagrams(blocks)

# Markdown -> HTML
body = md.markdown(
    text,
    extensions=["tables", "fenced_code", "sane_lists", "attr_list", "md_in_html", "toc"],
    extension_configs={"toc": {"permalink": False}},
)

# Substitute diagrams (markdown wraps the placeholder paragraph in <p>...</p>)
for i, svg in enumerate(svgs):
    fig = f'<figure class="diagram">{svg}</figure>'
    body = body.replace(f"<p>@@MERMAID{i}@@</p>", fig)
    body = body.replace(f"@@MERMAID{i}@@", fig)

CSS = """
@page {
  size: A4; margin: 18mm 16mm 20mm 16mm;
  @bottom-center { content: "Cayenne — Spice.ai's CDC acceleration engine"; font-size: 8pt; color: #94a3b8; }
  @bottom-right  { content: counter(page) " / " counter(pages); font-size: 8pt; color: #94a3b8; }
}
@page :first { @bottom-center { content: ""; } @bottom-right { content: ""; } }
html { -weasy-hyphens: auto; }
body { font-family: "Helvetica Neue", Helvetica, Arial, sans-serif; font-size: 10pt;
  line-height: 1.5; color: #1e293b; }
h1 { font-size: 21pt; color: #312e81; border-bottom: 3px solid #6366f1; padding-bottom: 5px;
  margin-top: 30px; page-break-before: always; page-break-after: avoid; }
h1:first-of-type { page-break-before: avoid; }
h2 { font-size: 15pt; color: #4338ca; margin-top: 22px; page-break-after: avoid;
  border-bottom: 1px solid #e0e7ff; padding-bottom: 3px; }
h3 { font-size: 12pt; color: #4f46e5; margin-top: 16px; page-break-after: avoid; }
h4 { font-size: 10.5pt; color: #475569; margin-top: 12px; page-break-after: avoid; }
p { margin: 7px 0; }
a { color: #4f46e5; text-decoration: none; }
strong { color: #312e81; }
code { font-family: "DejaVu Sans Mono", "SF Mono", Consolas, monospace; font-size: 8.6pt;
  background: #f1f5f9; padding: 1px 4px; border-radius: 3px; color: #be123c; }
pre { background: #0f172a; color: #e2e8f0; padding: 11px 13px; border-radius: 7px;
  font-size: 8.2pt; line-height: 1.42; overflow-x: auto; page-break-inside: avoid;
  border: 1px solid #1e293b; }
pre code { background: none; color: #e2e8f0; padding: 0; font-size: 8.2pt; }
.codehilite { background: #0f172a; border-radius: 7px; page-break-inside: avoid; }
.codehilite pre { margin: 0; }
table { border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 8.6pt;
  page-break-inside: avoid; }
th { background: #4338ca; color: #fff; text-align: left; padding: 6px 9px; font-weight: 600; }
td { border: 1px solid #e2e8f0; padding: 5px 9px; vertical-align: top; }
tr:nth-child(even) td { background: #f8fafc; }
blockquote { border-left: 4px solid #818cf8; background: #eef2ff; margin: 12px 0;
  padding: 8px 14px; color: #3730a3; border-radius: 0 6px 6px 0; }
blockquote p { margin: 4px 0; font-size: 9.2pt; }
figure.diagram { margin: 16px auto; text-align: center; page-break-inside: avoid;
  background: #ffffff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 12px; }
figure.diagram svg { max-width: 100%; height: auto; }
@page landscapefig {
  size: A4 landscape; margin: 12mm 14mm 14mm 14mm;
  @bottom-center { content: "Cayenne — Spice.ai's CDC acceleration engine"; font-size: 8pt; color: #94a3b8; }
  @bottom-right  { content: counter(page) " / " counter(pages); font-size: 8pt; color: #94a3b8; }
}
.landscape-fig { page: landscapefig; page-break-before: always; page-break-after: always;
  text-align: center; }
.landscape-fig svg { max-width: 100%; height: auto; }
.landscape-fig figcaption { font-size: 9pt; color: #64748b; margin-top: 10px; }
/* any referenced figure (committed .svg via <img src=...>) scales to its column */
img { max-width: 100%; height: auto; }
hr { border: none; border-top: 1px solid #e2e8f0; margin: 18px 0; }
ul, ol { margin: 7px 0; padding-left: 22px; }
li { margin: 3px 0; }
"""

# Title / cover block prepended
COVER = """
<div class="cover">
  <div class="cover-kicker">SPICE.AI · crates/cayenne</div>
  <div class="cover-title">Cayenne</div>
  <div class="cover-sub">Spice.ai's acceleration engine for high-rate CDC — a lakehouse table format built on Vortex</div>
  <div class="cover-desc">A breadth-first technical walkthrough — from the three-tier overview down to
  the locks, the fused deletion index, and the CDC write pipeline — with comparisons to
  Apache Iceberg, Delta Lake, and Apache Hudi.</div>
</div>
"""
COVER_CSS = """
.cover { page-break-after: always; padding-top: 70mm; }
.cover-kicker { font-size: 10pt; letter-spacing: 3px; color: #6366f1; font-weight: 600; }
.cover-title { font-size: 58pt; font-weight: 800; color: #312e81; margin: 6px 0 0; letter-spacing: -1px; }
.cover-sub { font-size: 16pt; color: #4338ca; margin-top: 8px; font-weight: 500; }
.cover-desc { font-size: 10.5pt; color: #475569; margin-top: 22px; max-width: 130mm; line-height: 1.6; }
.cover-title, .cover-sub, .cover-kicker { line-height: 1.1; }
"""

html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>{CSS}{COVER_CSS}</style></head>
<body>{COVER}{body}</body></html>"""

with open(HTML_OUT, "w", encoding="utf-8") as f:
    f.write(html)
print(f"Wrote {HTML_OUT} ({len(html)} bytes)", flush=True)

from weasyprint import HTML
HTML(string=html, base_url=".").write_pdf(PDF_OUT)
print(f"Wrote {PDF_OUT} ({os.path.getsize(PDF_OUT)//1024} KB)", flush=True)
