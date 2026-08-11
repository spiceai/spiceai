def shell_command($command):
  (($command[0] // "") | test("(^|/)(sh|bash|dash|ash|zsh)$"));

def contains_token($container):
  (($container.command // []) + ($container.args // [])) as $command
  | ([$command[]? | select(. == "--token" or startswith("--token="))] | length > 0)
    or (
      shell_command($command)
      and ([
        $command[]?
        | select(test("(^|[[:space:];|&])--token($|[=[:space:];|&])"))
      ] | length > 0)
    );

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
