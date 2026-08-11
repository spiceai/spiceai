def contains_token($container):
  (($container.command // []) + ($container.args // [])) as $command
  | ([$command[]? | select(. == "--token" or startswith("--token="))] | length > 0)
    or ([
      $command[]?
      | select((. == "--token" or startswith("--token=")) | not)
      | select(test("(^|[[:space:];|&])--token($|[=[:space:];|&])"))
    ] | length > 0);

(
  [
    .spec.template.spec.containers[]?
    | select(contains_token(.))
  ]
  | length == 0
) and (
  [
    .spec.template.spec.containers[]?
    | .env[]?
    | select(.valueFrom.secretKeyRef.name? == $secret)
  ]
  | length == 0
)
