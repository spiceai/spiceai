#!/bin/sh
# Renders the chart once per directory in cases/ and compares the result with
# that case's expected output.
#
# A case directory holds:
#   values.yaml     the values to render with
#   args            optional: extra `helm template` arguments, one line
#                   (for example `--show-only templates/podmonitor.yaml`)
#   expected.yaml   the rendered output, or
#   expected-error  text the failed render's error output must contain
#
# Usage: deploy/chart/tests/run.sh [case…]; HELM names the helm binary.
# UPDATE=1 rewrites each expected.yaml from the current render.
set -u
HELM=${HELM:-helm}
dir=$(cd "$(dirname "$0")" && pwd)
chart=$(dirname "$dir")
if [ "$#" -eq 0 ]; then
  set -- $(ls "$dir/cases")
fi
# Helm 4 ends each rendered document with the empty lines its template ends
# with, and Helm 3 trims them. Drop the empty lines just before a `---`
# separator, so both versions render the same text; every line inside a
# document is still compared.
drop_blank_before_separator() {
  awk '$0 == "" { blank++; next }
    { if ($0 != "---") for (; blank > 0; blank--) print ""; blank = 0; print }'
}
failed=0
diff_file=$(mktemp)
trap 'rm -f "$diff_file"' EXIT
for name in "$@"; do
  case_dir="$dir/cases/$name"
  args=""
  [ -f "$case_dir/args" ] && args=$(cat "$case_dir/args")
  # shellcheck disable=SC2086
  out=$($HELM template spiceai "$chart" --namespace spice -f "$case_dir/values.yaml" $args 2>&1)
  status=$?
  # The chart's version is in every label; leave it out of the comparison.
  out=$(printf '%s\n' "$out" | sed -e 's/^\( *helm.sh\/chart:\) .*/\1 VERSION/' \
    -e 's/^\( *app.kubernetes.io\/version:\) .*/\1 VERSION/' | drop_blank_before_separator)
  if [ -f "$case_dir/expected-error" ]; then
    if [ "$status" -eq 0 ]; then
      echo "FAIL $name: the render succeeded, but it must fail with: $(cat "$case_dir/expected-error")"
      failed=1
    elif ! printf '%s\n' "$out" | grep -qF -- "$(cat "$case_dir/expected-error")"; then
      echo "FAIL $name: the render failed with:"
      printf '%s\n' "$out"
      echo "but must fail with: $(cat "$case_dir/expected-error")"
      failed=1
    else
      echo "ok   $name (refused)"
    fi
    continue
  fi
  if [ "$status" -ne 0 ]; then
    echo "FAIL $name: the render failed:"
    printf '%s\n' "$out"
    failed=1
    continue
  fi
  if [ "${UPDATE:-}" = 1 ]; then
    printf '%s\n' "$out" > "$case_dir/expected.yaml"
    echo "updated $name"
  elif printf '%s\n' "$out" | diff -u "$case_dir/expected.yaml" - > "$diff_file"; then
    echo "ok   $name"
  else
    echo "FAIL $name: the render differs from expected.yaml:"
    cat "$diff_file"
    failed=1
  fi
done
exit "$failed"
