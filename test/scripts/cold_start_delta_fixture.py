# Copyright 2024-2026 The Spice.ai OSS Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate the synthetic audit-events Delta table that the cold-start tests load.

Table features: liquid clustering (tenant_id, emitted_at, event), deletion vectors, row tracking and
v2 checkpoints. The data is deterministic for a given --rows/--commits. Layout: append commits, an
OPTIMIZE, more small append commits, then a DELETE of the oldest events, which writes deletion vectors.

Requires Java 17, `pyspark==3.5.5` and `delta-spark==3.3.2`.
"""

import argparse
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

p = argparse.ArgumentParser()
p.add_argument("--out", required=True)
p.add_argument("--rows", type=int, required=True)
p.add_argument("--commits", type=int, default=24)
p.add_argument("--post-optimize-commits", type=int, default=8)
p.add_argument("--delete-fraction", type=float, default=0.02)
args = p.parse_args()

builder = (
    SparkSession.builder.appName("cold-start-fixture")
    .master("local[8]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.databricks.delta.properties.defaults.checkpointInterval", "10")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
out = os.path.abspath(args.out)

spark.sql(f"""
CREATE TABLE delta.`{out}` (
  id STRING NOT NULL,
  tenant_id STRING NOT NULL,
  emitted_at TIMESTAMP NOT NULL,
  event STRING NOT NULL,
  actor_id STRING,
  actor_type STRING,
  actor_ip STRING,
  user_agent STRING,
  resource_type STRING,
  resource_id STRING,
  outcome STRING,
  request_id STRING,
  payload STRING
) USING DELTA
CLUSTER BY (tenant_id, emitted_at, event)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.checkpointPolicy' = 'v2',
  'delta.dataSkippingStatsColumns' = 'tenant_id,emitted_at,event',
  'delta.logRetentionDuration' = 'interval 7 days'
)
""")

EVENTS = [
    "user.login", "user.logout", "user.password_reset", "user.mfa_enroll", "policy.update",
    "policy.create", "policy.delete", "device.register", "device.quarantine", "file.download",
    "file.upload", "file.share", "mail.release", "mail.block", "mail.quarantine", "admin.role_grant",
    "admin.role_revoke", "api.key_create", "api.key_revoke", "report.export", "scan.start",
    "scan.complete", "alert.ack", "alert.escalate", "integration.sync", "billing.update",
    "session.expire", "user.invite", "user.disable", "group.update",
]
events_sql = "array(" + ",".join(f"'{e}'" for e in EVENTS) + ")"

# Base timestamp: rows span ~120 days; later commits carry later rows.
SELECT = f"""
SELECT
  concat_ws('-', substr(md5(cast(n AS STRING)), 1, 8), substr(md5(cast(n AS STRING)), 9, 4),
            substr(md5(cast(n AS STRING)), 13, 4), substr(md5(cast(n AS STRING)), 17, 4),
            substr(md5(cast(n AS STRING)), 21, 12)) AS id,
  concat('tenant-', lpad(cast(pmod(n * 7919, 997) AS STRING), 4, '0')) AS tenant_id,
  timestamp_seconds(1767225600 + cast(n * (10368000.0 / {args.rows}) AS BIGINT) + pmod(n * 31, 600)) AS emitted_at,
  element_at({events_sql}, cast(pmod(n * 13, {len(EVENTS)}) + 1 AS INT)) AS event,
  concat('usr_', substr(sha1(cast(pmod(n, 50000) AS STRING)), 1, 20)) AS actor_id,
  element_at(array('user', 'service', 'admin', 'system'), cast(pmod(n, 4) + 1 AS INT)) AS actor_type,
  concat_ws('.', cast(pmod(n, 223) + 1 AS STRING), cast(pmod(n * 7, 255) AS STRING),
            cast(pmod(n * 11, 255) AS STRING), cast(pmod(n * 17, 255) AS STRING)) AS actor_ip,
  element_at(array('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0',
                   'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 Safari/605.1',
                   'python-requests/2.32.3', 'Go-http-client/2.0', 'okhttp/4.12.0'),
             cast(pmod(n * 3, 5) + 1 AS INT)) AS user_agent,
  element_at(array('mailbox', 'policy', 'device', 'file', 'user', 'group', 'report'),
             cast(pmod(n * 5, 7) + 1 AS INT)) AS resource_type,
  concat('res_', substr(sha2(cast(pmod(n, 2000000) AS STRING), 256), 1, 24)) AS resource_id,
  element_at(array('success', 'success', 'success', 'failure', 'denied'), cast(pmod(n, 5) + 1 AS INT)) AS outcome,
  substr(sha2(cast(n AS STRING), 256), 1, 32) AS request_id,
  to_json(named_struct(
    'v', 3,
    'source', element_at(array('console', 'api', 'scheduler', 'sync'), cast(pmod(n, 4) + 1 AS INT)),
    'geo', named_struct('country', element_at(array('US', 'GB', 'DE', 'IN', 'BR', 'AU'), cast(pmod(n, 6) + 1 AS INT)),
                        'asn', pmod(n * 101, 65000)),
    'changes', array(named_struct('field', 'state', 'old', substr(md5(cast(n * 3 AS STRING)), 1, 12),
                                  'new', substr(md5(cast(n * 5 AS STRING)), 1, 12)),
                     named_struct('field', 'owner', 'old', substr(sha1(cast(n * 7 AS STRING)), 1, 16),
                                  'new', substr(sha1(cast(n * 11 AS STRING)), 1, 16))),
    'trace', md5(cast(pmod(n, 200000) AS STRING)),
    'tags', array('audit', element_at(array('soc2', 'iso27001', 'hipaa', 'gdpr'), cast(pmod(n, 4) + 1 AS INT)))
  )) AS payload
FROM (SELECT id AS n FROM range({{start}}, {{end}}, 1, {{parts}}))
"""

total_commits = args.commits + args.post_optimize_commits
per_commit = args.rows // total_commits


def append(i: int, parts: int) -> None:
    start = i * per_commit
    end = args.rows if i == total_commits - 1 else (i + 1) * per_commit
    df = spark.sql(SELECT.format(start=start, end=end, parts=parts))
    df.write.format("delta").mode("append").save(out)


for i in range(args.commits):
    append(i, 4)
spark.sql(f"OPTIMIZE delta.`{out}`")
for i in range(args.commits, total_commits):
    append(i, 2)

cutoff_seconds = 1767225600 + int(10368000 * args.delete_fraction)
spark.sql(f"DELETE FROM delta.`{out}` WHERE emitted_at < timestamp_seconds({cutoff_seconds})")

detail = spark.sql(f"DESCRIBE DETAIL delta.`{out}`").collect()[0].asDict()
count = spark.read.format("delta").load(out).count()
props = detail.get("properties")
print(f"rows_visible={count}")
print(f"num_files={detail['numFiles']} size_bytes={detail['sizeInBytes']}")
print(f"clustering={detail.get('clusteringColumns')} min_reader={detail['minReaderVersion']} "
      f"min_writer={detail['minWriterVersion']} features={detail.get('tableFeatures')}")
print(f"properties={props}")
spark.stop()
