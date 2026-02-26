# Session Context

## User Prompts

### Prompt 1

in `.github/workflows/build_and_release.yml` we build certain spiced tags per matrix type. on manual dispatch, add the option to only run one of the allowed tags so that we don't build binaries when we don't need to

### Prompt 2

make a commit comand for me

### Prompt 3

[Request interrupted by user for tool use]

### Prompt 4

Instead, look at `tools/pr-builds`. Can we use this here, maybe as a flag?

### Prompt 5

what is default when no `--tag`? Just all available?

### Prompt 6

` tag: Option<String>,` is a bit messy. can we use a string enum?

