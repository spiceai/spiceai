#!/bin/bash

set -e

QUERY_NAME=$1
QUERY_FILE="${QUERY_NAME}.sql"
QUERY_SQL=$(cat "./crates/test-framework/src/queries/chbench/${QUERY_FILE}")

ANALYZE_JSON_PATH=$(mktemp)
ANALYZE_TEXT_PATH=$(mktemp)

curl -s -X POST http://localhost:8090/v1/sql \
  -H "Content-Type: text/plain" \
  --data "explain analyze ${QUERY_SQL}" > $ANALYZE_JSON_PATH

jq -r '.[] | .plan' $ANALYZE_JSON_PATH > $ANALYZE_TEXT_PATH

echo "===== total elapsed_compute by operator type (ms), sorted ====="
sed 's/µ/u/g' $ANALYZE_TEXT_PATH | awk '
function ms(v,u){return u=="ns"?v/1e6:(u=="us"?v/1e3:(u=="ms"?v:(u=="s"?v*1000:v)))}
{ op=$1; gsub(/[:,]/,"",op); if(op !~ /Exec$/) next; cnt[op]++;
  if (match($0,/elapsed_compute=[0-9.]+[a-z]+/)) {
    t=substr($0,RSTART+16,RLENGTH-16); u=t; gsub(/[0-9.]/,"",u); v=t; gsub(/[a-z]/,"",v);
    m=ms(v+0,u); sum[op]+=m; if(m>mx[op])mx[op]=m; }
}
END{ for(o in cnt) printf "%10.2f ms  %-30s  n=%-5d  max=%8.3f ms\n", sum[o], o, cnt[o], mx[o] }' | sort -rn
echo
echo "===== operator instance counts ====="
grep -oE '^[[:space:]]*[A-Za-z]+Exec' $ANALYZE_TEXT_PATH | tr -d ' ' | sort | uniq -c | sort -rn

sed 's/µ/u/g' $ANALYZE_TEXT_PATH | awk '
function ms(v,u){return u=="ns"?v/1e6:(u=="us"?v/1e3:(u=="ms"?v:(u=="s"?v*1000:v)))}
function getms(line,key,   t,u,v){ if(match(line,key"=[0-9.]+[a-z]+")){t=substr(line,RSTART+length(key)+1,RLENGTH-length(key)-1);u=t;gsub(/[0-9.]/,"",u);v=t;gsub(/[a-z]/,"",v);return ms(v+0,u)} return 0 }
function rows(line,   t,mult){ if(match(line,/output_rows=[0-9.]+ ?[KM]?/)){t=substr(line,RSTART+12,RLENGTH-12); mult=1; if(t~/K/)mult=1000; if(t~/M/)mult=1000000; gsub(/[^0-9.]/,"",t); return (t+0)*mult} return 0 }
/^[[:space:]]*DataSourceExec/{
  n++; r=rows($0); tr+=r;
  op+=getms($0,"time_elapsed_opening");
  sc+=getms($0,"time_elapsed_scanning_total");
  pr+=getms($0,"time_elapsed_processing");
  ud+=getms($0,"time_elapsed_scanning_until_data");
  s=getms($0,"time_elapsed_scanning_total"); if(s>mxsc){mxsc=s;mxr=r}
  o=getms($0,"time_elapsed_opening"); if(o>mxop)mxop=o;
}
END{
  printf "DataSourceExec instances=%d   total output_rows=%.0f\n\n", n, tr;
  printf "  time_elapsed_opening        sum=%8.1f ms   max=%7.2f ms   (footer open + layout)\n", op, mxop;
  printf "  time_elapsed_scanning_total sum=%8.1f ms   max=%7.2f ms   (rows=%.0f)  (decode+IO)\n", sc, mxsc, mxr;
  printf "  time_elapsed_processing     sum=%8.1f ms\n", pr;
  printf "  time_elapsed_scan_until_data sum=%7.1f ms   (first-batch latency, summed)\n", ud;
}'
echo
echo "===== top 12 individual DataSourceExec scanning_total times ====="
sed 's/µ/u/g' $ANALYZE_TEXT_PATH | awk '
function ms(v,u){return u=="ns"?v/1e6:(u=="us"?v/1e3:(u=="ms"?v:(u=="s"?v*1000:v)))}
function getms(line,key,   t,u,v){ if(match(line,key"=[0-9.]+[a-z]+")){t=substr(line,RSTART+length(key)+1,RLENGTH-length(key)-1);u=t;gsub(/[0-9.]/,"",u);v=t;gsub(/[a-z]/,"",v);return ms(v+0,u)} return 0 }
/^[[:space:]]*DataSourceExec/{ printf "%.2f ms scan  %.2f ms open  %s\n", getms($0,"time_elapsed_scanning_total"), getms($0,"time_elapsed_opening"), (match($0,/output_rows=[0-9.]+ ?[KM]?/)?substr($0,RSTART,RLENGTH):"") }' | sort -rn | head -12

rm $ANALYZE_TEXT_PATH $ANALYZE_JSON_PATH