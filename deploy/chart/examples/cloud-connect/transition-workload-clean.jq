def contains_token($container):
  (($container.command // []) + ($container.args // [])) as $command
  | ([$command[]? | select(. == "--token" or startswith("--token="))] | length > 0)
    or ([
      $command[]?
      | select((. == "--token" or startswith("--token=")) | not)
      | select(test("(^|[[:space:];|&])--token($|[=[:space:];|&])"))
    ] | length > 0);

def referenced_secret_names($pod):
  [
    $pod.imagePullSecrets[]?.name?,
    (
      ($pod.containers[]?, $pod.initContainers[]?, $pod.ephemeralContainers[]?)
      | .env[]?.valueFrom.secretKeyRef.name?,
        .envFrom[]?.secretRef.name?
    ),
    (
      $pod.volumes[]?
      | .secret.secretName?,
        .projected.sources[]?.secret.name?,
        .azureFile.secretName?,
        .cephfs.secretRef.name?,
        .cinder.secretRef.name?,
        .flexVolume.secretRef.name?,
        .iscsi.secretRef.name?,
        .rbd.secretRef.name?,
        .scaleIO.secretRef.name?,
        .storageos.secretRef.name?
    )
  ];

(
  [
    .spec.template.spec.containers[]?
    | select(contains_token(.))
  ]
  | length == 0
) and (
  referenced_secret_names(.spec.template.spec)
  | index($secret) == null
)
