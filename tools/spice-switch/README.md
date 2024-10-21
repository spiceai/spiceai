# Spice Switch

A simple script to help switch spicepods easier. Displays a list of the available files ending in `spicepod.yaml` or `spicepod.yml`.

Selecting an option copies the selected file into `spicepod.yaml`. The original spicepod is left untouched.

Example terminal output:

```bash
┌─────────────────────────────────────────────────────────────────────────────────────────────────| Select Configuration File |─────────────────────────────────────────────────────────────────────────────────────────────────┐
│ a.spicepod.yml                                                                                                                                                                                                               ^│
│ c.spicepod.yml                                                                                                                                                                                                                │
│ b.spicepod.yml                                                                                                                                                                                                                │
```

## Installation

Requires `prompt-toolkit`. Install the script system wide (or venv wide) with `pip install .`. Once installed, can be ran with `spice-switch`.
