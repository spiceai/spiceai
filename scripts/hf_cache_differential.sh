#!/bin/bash
# Differential check of Spice's Hugging Face cache against the official `hf` CLI, the oracle.
#
# For each case, at the commit `main` names now:
#   1. Spice (the `huggingface-hub` crate's `fetch` example) downloads the files into cache A,
#      and `hf download` downloads them into cache B.
#   2. Every file must have the same bytes in both caches and match the Hub's checksum: the
#      SHA-256 of an LFS or Xet file, the git object id of any other. Spice's snapshot entry
#      must be a symlink into its blobs, as `huggingface_hub` makes them.
#   3. Each tool must read the other's cache with HF_HUB_OFFLINE=1.
#
# Usage: scripts/hf_cache_differential.sh [work dir]
# Needs network access to huggingface.co and the `hf` CLI on PATH
# (`uv tool install huggingface_hub` installs it).
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${1:-$ROOT/target/hf_cache_differential}"
HF="$(command -v hf)" || { echo "the hf CLI is not on PATH" >&2; exit 2; }
# The path cargo reports for the example, so a CARGO_TARGET_DIR is honored. The reader takes
# every message before it picks one: cargo fails its build when a write finds the pipe closed.
FETCH=$(cargo build --quiet --manifest-path "$ROOT/Cargo.toml" -p huggingface-hub --example fetch \
  --message-format=json-render-diagnostics \
  | python3 -c 'import json, sys; messages = [json.loads(line) for line in sys.stdin]; print(next(m["executable"] for m in messages if m.get("executable") and m["target"]["name"] == "fetch"))') \
  || exit 2
rm -rf "$WORK"; mkdir -p "$WORK"
CASES=(
  "model HuggingFaceTB/SmolLM2-135M-Instruct config.json tokenizer.json tokenizer_config.json generation_config.json model.safetensors"
  "model sentence-transformers/all-MiniLM-L6-v2 config.json 1_Pooling/config.json sentence_bert_config.json tokenizer.json model.safetensors"
  "model minishlab/potion-base-2M config.json tokenizer.json model.safetensors"
  "model bartowski/SmolLM2-135M-Instruct-GGUF SmolLM2-135M-Instruct-Q2_K.gguf"
  "model intfloat/e5-small-v2 config.json tokenizer.json model.safetensors"
  "model google-bert/bert-base-uncased config.json tokenizer.json model.safetensors"
  "model hf-internal-testing/tiny-random-bert-sharded-safetensors config.json model.safetensors.index.json model-00001-of-00005.safetensors model-00002-of-00005.safetensors model-00003-of-00005.safetensors model-00004-of-00005.safetensors model-00005-of-00005.safetensors"
  "model hf-internal-testing/tiny-random-bert-sharded model.safetensors.index.json model-00001-of-00005.safetensors model-00002-of-00005.safetensors model-00003-of-00005.safetensors model-00004-of-00005.safetensors model-00005-of-00005.safetensors"
  "dataset stanfordnlp/imdb plain_text/test-00000-of-00001.parquet"
  "space gradio/hello_world app.py README.md"
)
failures=0
for case in "${CASES[@]}"; do
  set -- $case
  kind="$1"; repo="$2"; shift 2; files=("$@")
  slug="${repo//\//__}"
  plural="${kind}s"
  commit=$(curl -s "https://huggingface.co/api/$plural/$repo/revision/main" | python3 -c "import json,sys; print(json.load(sys.stdin)['sha'])")
  A="$WORK/$slug/spice"; B="$WORK/$slug/hf"
  hf_type=()
  [ "$kind" != model ] && hf_type=(--repo-type "$kind")
  HF_HOME="$A" "$FETCH" "$kind" "$repo" "$commit" "${files[@]}" >"$WORK/$slug.spice.out" 2>"$WORK/$slug.spice.err"; rc_a=$?
  HF_HOME="$B" HF_HUB_DISABLE_TELEMETRY=1 "$HF" download "$repo" "${files[@]}" --revision "$commit" ${hf_type[@]+"${hf_type[@]}"} >"$WORK/$slug.hf.out" 2>"$WORK/$slug.hf.err"; rc_b=$?
  # Cross-reads, offline: hf reads Spice's cache, Spice reads hf's.
  HF_HOME="$A" HF_HUB_OFFLINE=1 "$HF" download "$repo" "${files[@]}" --revision "$commit" ${hf_type[@]+"${hf_type[@]}"} >"$WORK/$slug.hf-reads-spice.out" 2>&1; rc_c=$?
  HF_HOME="$B" HF_HUB_OFFLINE=1 "$FETCH" "$kind" "$repo" "$commit" "${files[@]}" >"$WORK/$slug.spice-reads-hf.out" 2>&1; rc_d=$?
  verdict=$(python3 - "$A/hub" "$B/hub" "$plural--${repo//\//--}" "$commit" "$repo" "$plural" "${files[@]}" <<'PY'
import hashlib, json, os, sys, urllib.parse, urllib.request
a, b, folder, commit, repo, plural, *files = sys.argv[1:]
tree = {}
url = f"https://huggingface.co/api/{plural}/{repo}/paths-info/{commit}"
data = "&".join(["expand=false"] + [f"paths={urllib.parse.quote(f)}" for f in files]).encode()
for entry in json.load(urllib.request.urlopen(urllib.request.Request(url, data=data))):
    tree[entry["path"]] = entry
problems = []
for f in files:
    pa = os.path.join(a, folder, "snapshots", commit, f)
    pb = os.path.join(b, folder, "snapshots", commit, f)
    try:
        da, db = open(pa, "rb").read(), open(pb, "rb").read()
    except OSError as e:
        problems.append(f"{f}: {e}")
        continue
    if da != db:
        problems.append(f"{f}: bytes differ")
    entry = tree.get(f, {})
    if entry.get("lfs"):
        if hashlib.sha256(da).hexdigest() != entry["lfs"]["oid"]:
            problems.append(f"{f}: SHA-256 differs from the Hub's")
    else:
        git = hashlib.sha1(b"blob %d\0" % len(da) + da).hexdigest()
        if git != entry.get("oid"):
            problems.append(f"{f}: git oid differs from the Hub's")
    if not os.path.islink(pa):
        problems.append(f"{f}: Spice's snapshot entry is not a symlink")
print("OK" if not problems else "; ".join(problems))
PY
)
  [ "$verdict" != OK ] || [ $rc_a -ne 0 ] || [ $rc_b -ne 0 ] || [ $rc_c -ne 0 ] || [ $rc_d -ne 0 ] && failures=$((failures+1))
  printf '%-58s commit=%s files=%d spice=%d hf=%d hf-reads-spice=%d spice-reads-hf=%d -> %s\n' \
    "$kind $repo" "${commit:0:7}" "${#files[@]}" $rc_a $rc_b $rc_c $rc_d "$verdict"
done
echo "failures=$failures"
[ "$failures" -eq 0 ]
