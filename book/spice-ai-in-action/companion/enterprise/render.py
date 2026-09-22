"""Render Kubernetes JSON manifests without shell interpolation or a cluster connection."""
import argparse,json,os
from pathlib import Path
root=Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('kind',choices=['spicepodset','spicepodcluster']);p.add_argument('--output',required=True);a=p.parse_args()
required={'REPLACE_ENTERPRISE_REPOSITORY':'BOOK_ENTERPRISE_REPOSITORY','REPLACE_TESTED_RUNTIME_TAG':'BOOK_ENTERPRISE_TAG'}
if a.kind=='spicepodcluster':required.update({'REPLACE_BUCKET':'BOOK_STATE_BUCKET','REPLACE_STORAGE_CLASS':'BOOK_STORAGE_CLASS'})
values={}
for placeholder,name in required.items():
 value=os.environ.get(name,'').strip()
 if not value:raise SystemExit('Set '+name+' to the reviewed deployment value.')
 values[placeholder]=value

def render(value):
 if isinstance(value,dict):return {k:render(v) for k,v in value.items()}
 if isinstance(value,list):return [render(v) for v in value]
 if isinstance(value,str):
  for old,new in values.items():value=value.replace(old,new)
 return value
result=render(json.loads((root/(a.kind+'.template.json')).read_text()))
out=Path(a.output);out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps(result,indent=2)+'\n');print(out)
