"""Offline outer-CRD validation. Requires PyYAML and jsonschema; no cluster access."""
import argparse,json
from pathlib import Path
import yaml,jsonschema
p=argparse.ArgumentParser();p.add_argument('--rendered-chart',required=True);p.add_argument('--output',default='manifest-validation.json');a=p.parse_args()
root=Path(__file__).resolve().parent
resources=[d for d in yaml.safe_load_all(Path(a.rendered_chart).read_text()) if d]
crds={d['spec']['names']['kind']:d for d in resources if d['kind']=='CustomResourceDefinition'}
results=[]
for path in sorted(root.glob('*.template.json')):
 manifest=json.loads(path.read_text());kind=manifest['kind'];version_name=manifest['apiVersion'].split('/')[1]
 version=next(v for v in crds[kind]['spec']['versions'] if v['name']==version_name)
 errors=list(jsonschema.Draft7Validator(version['schema']['openAPIV3Schema']).iter_errors(manifest))
 results.append({'file':path.name,'valid':not errors,'errors':[e.message for e in errors]})
record={'scope':'Offline outer-CRD structural validation; no runtime, admission, image, identity, or cluster execution. Nested Spicepod needs separate validation.','results':results}
Path(a.output).write_text(json.dumps(record,indent=2));print(json.dumps(record,indent=2))
raise SystemExit(0 if all(r['valid'] for r in results) else 1)
