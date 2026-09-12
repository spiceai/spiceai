"""Generate original vector technical figures and high-resolution DOCX images."""
from pathlib import Path
from reportlab.pdfgen import canvas
from reportlab.lib.colors import HexColor, Color, white
import fitz
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
pdfmetrics.registerFont(TTFont("Inter", str(Path.home()/"Library/Fonts/Inter-Regular.ttf")))
pdfmetrics.registerFont(TTFont("Inter-Bold", str(Path.home()/"Library/Fonts/Inter-Bold.ttf")))
root=Path(__file__).parent/'figures'
root.mkdir(exist_ok=True)
W,H=740,399
BG='#F3F3F6'; BORDER='#D3D3D9'; INK='#2E2F36'; MUTED='#77778B'; ORANGE='#FF622A'

def box(c,x,y,w,h,title,sub='',hero=False):
    c.setFillColor(HexColor(ORANGE if hero else '#FFFFFF'))
    c.setStrokeColor(HexColor('#BC3201' if hero else BORDER));c.setLineWidth(1.5)
    c.roundRect(x,y,w,h,7.25,fill=1,stroke=1)
    c.setFillColor(white if hero else HexColor(INK));c.setFont('Inter-Bold',13)
    c.drawCentredString(x+w/2,y+h/2+(5 if sub else -4),title)
    if sub:
        c.setFont('Inter',10)
        c.setFillColor(Color(1,1,1,.86) if hero else HexColor(MUTED))
        c.drawCentredString(x+w/2,y+h/2-13,sub)

def path(c,points,label='',lx=None,ly=None):
    c.setStrokeColor(HexColor(MUTED));c.setLineWidth(1)
    p=c.beginPath();p.moveTo(*points[0])
    for point in points[1:]:p.lineTo(*point)
    c.drawPath(p)
    if label:
        c.setFillColor(HexColor(MUTED));c.setFont('Inter',10)
        c.drawCentredString(lx if lx is not None else sum(x for x,y in points)/len(points),ly if ly is not None else points[0][1]+8,label)

def draw(name,title,top,hero,bottom):
    file=root/(name+'.pdf'); c=canvas.Canvas(str(file),pagesize=(W,H))
    c.setFillColor(HexColor(BG));c.setStrokeColor(HexColor('#E3E3E8'));c.setLineWidth(.5)
    c.roundRect(.25,.25,W-.5,H-.5,5.75,fill=1,stroke=1)
    c.setFillColor(HexColor(INK));c.setFont('Inter-Bold',17);c.drawString(32,363,title)
    widths=(W-64-20*(len(top)-1))/len(top)
    top_centers=[]
    for i,(a,b) in enumerate(top):
        x=32+i*(widths+20);box(c,x,271,widths,60,a,b);top_centers.append(x+widths/2)
    box(c,230,153,280,66,*hero,hero=True)
    for x in top_centers:
        path(c,[(x,271),(x,244),(370,244),(370,221)])
    widths=(W-64-20*(len(bottom)-1))/len(bottom)
    for i,(a,b) in enumerate(bottom):
        x=32+i*(widths+20);box(c,x,35,widths,60,a,b)
        path(c,[(370,153),(370,124),(x+widths/2,124),(x+widths/2,97)])
    c.save()
    pdf=fitz.open(file);page=pdf[0]
    page.get_pixmap(matrix=fitz.Matrix(2.2,2.2),alpha=False).save(root/(name+'.png'))
    # SVG is vector-native and editable; PDF supplies the embedded raster render.
    (root/(name+'.svg')).write_text(page.get_svg_image(text_as_path=False))
    pdf.close()

figures=[
('runtime','An application-facing data runtime',[('Application','Business operations'),('SQL / Search / Models','Bounded interfaces')],('Spice runtime','Planning, execution, and serving'),[('Sources','Databases, files, documents'),('Accelerated state','Chosen storage and refresh'),('Model services','Local or remote inference')]),
('plan','Follow the shape of a query',[('Source scan','8 fixture orders')],('Paid-status filter','6 paid orders remain'),[('Grouped aggregate','2 tenant totals')]),
('lakehouse','Make data coverage explicit',[('Historical export','Published immutable version'),('Operational changes','Current source state')],('Coverage contract','Cutoff, identity, and reconciliation'),[('Historical query','Before the agreed boundary'),('Serving working set','At or after the boundary')]),
('freshness','The age of an application answer',[('Source commit','Business change becomes durable'),('Source publication','Export or change-log visibility')],('Spice visibility','Refresh or replication progress'),[('Result cache','Configured reuse interval'),('Application response','Coverage and freshness status')]),
('cayenne','Cayenne is more than its data files',[('Ingestion','Rows and keyed mutations'),('Query','A coherent visible table')],('Cayenne visibility','Schema, sequences, and snapshots'),[('Columnar data','Vortex-backed representation'),('Metadata','Table and recovery state'),('Maintenance','Compaction and reclamation')]),
('rag','Ground answers in authorized evidence',[('Authenticated question','Tenant and user scope'),('Deterministic facts','Authorized order lookup')],('Evidence assembly','Eligible passages with stable citations'),[('Generator','Bounded prompt and toolset'),('Validation','Claims, citations, and output')]),
('mcp','Keep tool directions and trust visible',[('External agent','Authenticated tool caller')],('Spice MCP boundary','Expose or route selected capabilities'),[('Data tools','Scoped SQL and retrieval'),('External tools','Separate identity and data flow')]),
('cluster','Separate coordination from execution',[('Application jobs','Identity, deadlines, and result ownership')],('Scheduler layer','Plans, stages, and job state'),[('Executors','Tasks, scans, and shuffle'),('Shared state','Supported durable backend'),('Data sources','Executor access and identity')]),
('testing','Use evidence suited to the claim',[('Business contract','Rows, keys, and meaning'),('Deployment contract','Identity, state, and lifecycle')],('Acceptance run','Same inputs and recorded environment'),[('SQL artifacts','Rows and query plans'),('AI artifacts','Candidates and citations'),('Operational artifacts','Metrics, logs, and recovery')]),
('northstar','Northstar: one contract across two products',[('Sales client','Paid totals by authorized tenant'),('Support client','Policies with source identity')],('Northstar service','Trusted scope and bounded operations'),[('SQL interface','Parameterized report queries'),('Search interface','Structured evidence retrieval')]),
]
figures.extend([
('enterprise-policy','Make the authorization decision explicit',[('Authenticated principal','User, tenant, and roles'),('Requested resource','Dataset, model, tool, endpoint')],('Policy and access plan','Allowed action, rows, and columns'),[('Data result','Authorized facts'),('AI operation','Approved evidence and tools')]),
('enterprise-operator','Reconcile an Enterprise application',[('Reviewed custom resource','SpicepodSet or SpicepodCluster'),('Installed API contract','Operator and CRD versions')],('Spice Kubernetes operator','Desired state and observed conditions'),[('Workloads','StatefulSets and pod identity'),('Connectivity','Services and network rules'),('State','Volumes, Secrets, certificates')]),
('enterprise-snapshot','Restore a known data version',[('Source state','Commit and replay boundary'),('Owned acceleration','Schema, data, partition identity')],('Published snapshot','Versioned artifact and integrity record'),[('Consumer bootstrap','Load a compatible generation'),('Reconciliation','Catch up and verify business facts')]),
])
for args in figures: draw(*args)
print('Created',len(figures),'original figures (PDF, SVG, PNG).')
