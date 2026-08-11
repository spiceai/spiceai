def token_matches($command):
  [
    range(0; $command | length) as $index
    | if $command[$index] == "--token" then
        {index: $index, width: 2, ref: ($command[$index + 1] // "")}
      elif ($command[$index] | startswith("--token=")) then
        {index: $index, width: 1, ref: ($command[$index] | ltrimstr("--token="))}
      else
        empty
      end
  ];

def shell_command($command):
  (($command[0] // "") | test("(^|/)(sh|bash|dash|ash|zsh)$"));

def embedded_token_syntax($command):
  if shell_command($command) then
    [
      $command[]
      | select(test("(^|[[:space:];|&])--token($|[=[:space:];|&])"))
    ]
  else
    []
  end;

(. // {}) as $values
| ($values.command // []) as $command
| ($values.cloudConnect.bootstrapSecretName // null) as $remembered_secret
| token_matches($command) as $matches
| embedded_token_syntax($command) as $token_syntax
| if $remembered_secret != null
    and (($remembered_secret | type) != "string" or ($remembered_secret | length) == 0) then
    error("remembered bootstrap Secret name is invalid")
  elif ($matches | length) > 1 then
    error("installed command contains more than one --token argument")
  elif ($token_syntax | length) > 0 then
    error("installed command contains unsupported embedded or shell-form --token syntax")
  elif ($matches | length) == 0 then
    {
      values: {
        command: $command,
        additionalEnv: ($values.additionalEnv // []),
        cloudConnect: (($values.cloudConnect // {}) + {
          bootstrapSecretName: $remembered_secret
        })
      },
      bootstrapSecretName: $remembered_secret
    }
  else
    $matches[0] as $match
    | if ($match.ref | test("^\\$\\([A-Za-z_][A-Za-z0-9_]*\\)$") | not) then
        error("installed --token argument is not an environment expansion")
      else
        ($match.ref | capture("^\\$\\((?<name>[A-Za-z_][A-Za-z0-9_]*)\\)$").name) as $token_env
        | [($values.additionalEnv // [])[] | select(.name == $token_env)] as $token_envs
        | if ($token_envs | length) != 1 then
            error("installed token environment entry is missing or ambiguous")
          elif (($token_envs[0].valueFrom.secretKeyRef.name // "") | length) == 0 then
            error("installed token environment entry has no Secret name")
          else
            ($token_envs[0].valueFrom.secretKeyRef.name) as $installed_secret
            | if $remembered_secret != null and $remembered_secret != $installed_secret then
                error("remembered bootstrap Secret name does not match the installed token secretKeyRef")
              else
            {
              values: {
                command: [
                  range(0; $command | length) as $index
                  | select($index < $match.index or $index >= ($match.index + $match.width))
                  | $command[$index]
                ],
                additionalEnv: [
                  ($values.additionalEnv // [])[]
                  | select(.name != $token_env)
                ],
                cloudConnect: (($values.cloudConnect // {}) + {
                  bootstrapSecretName: $installed_secret
                })
              },
              bootstrapSecretName: $installed_secret
            }
              end
          end
      end
  end
