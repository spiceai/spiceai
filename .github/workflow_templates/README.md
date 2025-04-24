# GitHub Actions Workflow Templates for YAML Anchors

Unfortunately, GitHub Actions does not yet support YAML anchors: <https://github.com/actions/runner/issues/1182>.

It is supposed to be coming in 2025, but until that feature is available, we have to manually generate any workflow files that use YAML anchors.

Once it is available, we can move these files to the `.github/workflows` directory and remove the tool-generated notice.

## Tool generated notice

Add the following notice to the top of any tool-generated workflow file. Replace the `yq` command with the command that generates the file.

```yaml
##################################################################################################################
#                                                                                                                #
#  WARNING: TOOL-GENERATED FILE - DO NOT EDIT MANUALLY!                                                          #
#                                                                                                                #
#  Command: yq 'explode(.)' .github/workflow_templates/spiced_docker.yml > .github/workflows/spiced_docker.yml   #
#                                                                                                                #
#  Keep this notice at the top of the file.                                                                      #
#                                                                                                                #
##################################################################################################################
```

## `spiced_docker.yml`

```bash
yq 'explode(.)' .github/workflow_templates/spiced_docker.yml > .github/workflows/spiced_docker.yml
```

## `spiced_docker_nightly.yml`

```bash
yq 'explode(.)' .github/workflow_templates/spiced_docker_nightly.yml > .github/workflows/spiced_docker_nightly.yml
```
