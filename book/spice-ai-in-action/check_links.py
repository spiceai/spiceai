from pathlib import Path
import re,json,urllib.request,urllib.error,concurrent.futures
root=Path(__file__).resolve().parent
urls=sorted(set(re.findall(r'\]\((https://[^)\s]+)\)',(root/'manuscript.md').read_text())))
def check(url):
 try:
  req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0','Accept':'text/markdown, text/html;q=0.9'},method='HEAD')
  with urllib.request.urlopen(req,timeout=20) as r:return {'url':url,'status':r.status,'final_url':r.url}
 except urllib.error.HTTPError as e:return {'url':url,'status':e.code}
 except Exception as e:return {'url':url,'error':str(e)}
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:results=list(pool.map(check,urls))
(root/'evidence/link-check.json').write_text(json.dumps(results,indent=2))
print(json.dumps({'links':len(results),'non_200':[r for r in results if r.get('status')!=200]},indent=2))
