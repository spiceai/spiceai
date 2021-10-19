import os
import argparse

apiHost = "https://api.github.com"
token = os.getenv("GITHUB_TOKEN")

if token == None:
    print("ERROR: Specify GITHUB_TOKEN environment variable")
    exit(1)

parser = argparse.ArgumentParser()
parser.add_argument('action')
parser.add_argument('--owner')
parser.add_argument('--repo')
parser.add_argument('--tag')
parser.add_argument('--release-name')
parser.add_argument('--body')
parser.add_argument('artifacts', action="append")

args = parser.parse_args()

print(args)