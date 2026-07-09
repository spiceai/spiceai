# "Life of a change" landscape waterfall — 6 generations, in-memory/warm/cold bracket.
import html
DARK="#1e293b"; BORDER="#312e81"; ACCENT="#6366f1"; SUB="#475569"; SEQ="#6366f1"; FAINT="#94a3b8"
def esc(s): return html.escape(s, quote=True)

def box(x,y,w,h,title,sub,seq):
    cx=x+w/2
    t=f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" fill="#ffffff" stroke="{BORDER}" stroke-width="2"/>'
    t+=f'<text x="{cx}" y="{y+21}" text-anchor="middle" font-size="15" font-weight="700" fill="{DARK}">{esc(title)}</text>'
    yy=y+38
    for line in sub:
        t+=f'<text x="{cx}" y="{yy}" text-anchor="middle" font-size="11.5" fill="{SUB}">{esc(line)}</text>'; yy+=15
    t+=f'<text x="{cx}" y="{y+h-7}" text-anchor="middle" font-size="11" font-style="italic" fill="{SEQ}">{esc(seq)}</text>'
    return t

def cyl(x,y,w,h,title,sub,seq):
    ry=15; cx=x+w/2
    t=(f'<path d="M{x},{y+ry} A{w/2},{ry} 0 0 1 {x+w},{y+ry} L{x+w},{y+h-ry} '
       f'A{w/2},{ry} 0 0 1 {x},{y+h-ry} Z" fill="#ffffff" stroke="{BORDER}" stroke-width="2"/>')
    t+=f'<ellipse cx="{cx}" cy="{y+ry}" rx="{w/2}" ry="{ry}" fill="#ffffff" stroke="{BORDER}" stroke-width="2"/>'
    t+=f'<text x="{cx}" y="{y+42}" text-anchor="middle" font-size="15" font-weight="700" fill="{DARK}">{esc(title)}</text>'
    yy=y+59
    for line in sub:
        t+=f'<text x="{cx}" y="{yy}" text-anchor="middle" font-size="11.5" fill="{SUB}">{esc(line)}</text>'; yy+=14
    t+=f'<text x="{cx}" y="{y+h-20}" text-anchor="middle" font-size="10.5" font-style="italic" fill="{SEQ}">{esc(seq)}</text>'
    return t

def arrow(x1,y1,x2,y2,label,lx,ly,dash=False,fs=10,wrap=30):
    d=' stroke-dasharray="6 5"' if dash else ''
    ym=(y1+y2)/2
    t=f'<path d="M{x1},{y1} C{x1},{ym} {x2},{ym} {x2},{y2}" fill="none" stroke="{DARK}" stroke-width="2" marker-end="url(#ah)"{d}/>'
    t+=label_only(lx,ly,_wrap(label,wrap),fs=fs,stroke=ACCENT)
    return t

def _wrap(label,wrap):
    words=label.split(); lines=[]; cur=""
    for wd in words:
        if len(cur)+len(wd)+1>wrap: lines.append(cur); cur=wd
        else: cur=(cur+" "+wd).strip()
    if cur: lines.append(cur)
    return lines

def label_only(lx,ly,lines,fs=10,stroke=DARK,dash=False):
    bw=max(len(l) for l in lines)*(fs*0.56)+12; bh=len(lines)*(fs+3)+6
    sd=' stroke-dasharray="4 3"' if dash else ''
    t=f'<rect x="{lx-bw/2}" y="{ly-bh/2}" width="{bw}" height="{bh}" rx="5" fill="#ffffff" stroke="{stroke}" stroke-width="1"{sd} opacity="0.97"/>'
    yy=ly-bh/2+fs+1
    for l in lines:
        t+=f'<text x="{lx}" y="{yy}" text-anchor="middle" font-size="{fs}" fill="#334155">{esc(l)}</text>'; yy+=fs+3
    return t

def bracket(y0,y1,label):
    x=118; tick=11
    t=f'<path d="M{x+tick},{y0} L{x},{y0} L{x},{y1} L{x+tick},{y1}" fill="none" stroke="{FAINT}" stroke-width="1.5"/>'
    ymid=(y0+y1)/2
    t+=f'<text x="46" y="{ymid}" text-anchor="middle" font-size="13" font-weight="700" fill="#64748b" transform="rotate(-90 46,{ymid})">{esc(label)}</text>'
    return t

W=1200;H=744
svg=[f'<svg viewBox="0 0 {W} {H}" width="{W}" height="{H}" xmlns="http://www.w3.org/2000/svg" font-family="Helvetica Neue, Helvetica, Arial, sans-serif">']
svg.append(f'<defs><marker id="ah" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L9,3 L0,6 Z" fill="{DARK}"/></marker>'
           f'<marker id="ahg" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto"><path d="M0,0 L9,3 L0,6 Z" fill="{FAINT}"/></marker></defs>')

DBx,DBy,DBw,DBh=958,26,205,110
n1=(788,152,205,70); n2=(618,262,205,70); n3=(430,374,205,80); n4=(258,494,205,70); n5=(150,604,205,82)

# left tier bracket
svg.append(bracket(152,332,"in-memory (RAM)"))
svg.append(bracket(374,564,"warm (local disk)"))
svg.append(bracket(604,686,"cold (object store)"))
svg.append(f'<text x="46" y="90" text-anchor="middle" font-size="12" fill="{FAINT}" transform="rotate(-90 46,90)">source</text>')

# DB (source) + transactions inflow (label above, arrow down into the top)
svg.append(cyl(DBx,DBy,DBw,DBh,"System of record",["(e.g. Postgres) —","the source database"],"seq 12,651+ (live)"))
cxDB=DBx+DBw/2
svg.append(f'<text x="{cxDB}" y="14" text-anchor="middle" font-size="11" fill="#334155">transactions / writes</text>')
svg.append(f'<path d="M{cxDB},18 L{cxDB},33" fill="none" stroke="{DARK}" stroke-width="2" marker-end="url(#ah)"/>')

# generation boxes
svg.append(box(*n1,"Changes stream",["ordered change events","from the CDC connector"],"seq 12,601–12,650"))
svg.append(box(*n2,"Level-0 / tier-0",["inline blobs / mem-tier (RAM)"],"seq 12,501–12,600"))
svg.append(box(*n3,"Published snapshots",["one current + protected tail","(all visible to scans)"],"seq 10,001–12,500"))
svg.append(box(*n4,"Compacted base",["merged target-sized files"],"seq 2,001–10,000"))
svg.append(box(*n5,"Cold object-store tier",["Z-order-clustered Vortex,","read-optimized (optional)"],"seq ≤ 2,000"))

# transition arrows (down-left)
svg.append(arrow(1000,DBy+DBh,975,152,"CDC connector emits ordered changes",1072,172,wrap=18))
svg.append(arrow(838,222,812,262,"apply loop: coalesce a burst; small burst → inline / mem-tier",690,245,wrap=30))
svg.append(arrow(665,332,640,374,"checkpoint / flush: tier → Vortex file, Stage B pointer flip (visible)",505,350,wrap=30))
svg.append(arrow(480,454,455,494,"maintenance compaction + seq-prefix bake (COW background)",355,472,wrap=34))
svg.append(arrow(306,564,282,604,"BackgroundColdTierPromoter: re-materialize + Z-order cluster, overwrite to object store",470,584,wrap=44))

# large-burst bypass (dashed): changes stream -> published (lands on the box's left edge), skipping tier-0
svg.append(f'<path d="M788,175 C450,200 360,340 430,410" fill="none" stroke="{DARK}" stroke-width="1.8" stroke-dasharray="6 5" marker-end="url(#ah)"/>')
svg.append(label_only(408,276,["large burst: Stage A + Stage B","publishes direct (bypasses tier-0)"],fs=10,dash=True))

# bottom time axis
svg.append(f'<line x1="400" y1="716" x2="1150" y2="716" stroke="{FAINT}" stroke-width="1.2" marker-end="url(#ahg)"/>')
svg.append(f'<text x="410" y="709" font-size="12" fill="{FAINT}">older</text>')
svg.append(f'<text x="1142" y="709" text-anchor="end" font-size="12" fill="{FAINT}">newer  ·  time / recency →</text>')

svg.append('</svg>')
SVG = "\n".join(svg)
with open("waterfall.svg", "w", encoding="utf-8") as f:
    f.write(SVG)
print(f"wrote waterfall.svg ({len(SVG)} bytes)")
# Optional landscape preview PDF — only if WeasyPrint's native deps are present.
# waterfall.svg is the committed artifact referenced by cayenne.md; the preview
# is a convenience and its absence never blocks regenerating the figure.
try:
    from weasyprint import HTML
    HTML(string=f"<html><head><style>@page{{size:A4 landscape; margin:8mm}}</style></head><body style='margin:0'>{SVG}</body></html>").write_pdf("waterfall_ls.pdf")
    print("wrote waterfall_ls.pdf preview")
except Exception as e:  # noqa: BLE001 — preview is best-effort
    print(f"skipped waterfall_ls.pdf preview ({e})")
