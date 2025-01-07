#!/bin/bash

set -aue

if [ "$#" -ne 2 ]; then
  echo "Usage: ./setup-semi-structure-spicepod.bash <row size> <number of rows>"
  echo "Example: ./setup-semi-structure-spicepod.bash 1M 100000"
  exit 1
fi

# check if synth_collection_tmp exists
if [ -d "synth_collection_tmp" ]; then
	echo "synth_collection_tmp already exists. Aborting."
	echo "Please remove 'synth_collection_tmp' and try again."
	exit 1
fi

# case match for row size
case "$1" in
  1M)
    arraya_size=40000
    ;;
  10M)
    arraya_size=400000
    ;;
  100M)
    arraya_size=4000000
    ;;
  *)
    echo "Invalid row size. Aborting."
    exit 1
    ;;
esac

# multiple line string
synth_collection=$(cat <<-END
{
  "type": "array",
  "length": {
    "type": "number",
    "subtype": "u64",
    "range": {
      "low": 1,
      "high": 4,
      "step": 1
    }
  },
  "content": {
    "type": "object",
    "username": {
      "type": "one_of",
      "variants": [
        {
          "type": "same_as",
          "ref": "users.content.email"
        },
        {
          "type": "string",
          "faker": {
            "generator": "username"
          }
        }
      ]
    },
    "credit_card": {
      "type": "string",
      "faker": {
        "generator": "credit_card"
      }
    },
    "id": {
      "type": "number",
      "subtype": "u64",
      "id": {}
    },
    "num_logins": {
      "type": "number",
      "subtype": "u64",
      "range": {
        "low": 19,
        "high": 96,
        "step": 1
      }
    },
    "created_at": {
      "type": "date_time",
      "format": "%Y-%m-%dT%H:%M:%S%z",
      "begin": "2015-01-01T00:00:00+0000",
      "end": "2018-01-01T00:00:00+0000"
    },
    "email": {
      "type": "string",
      "faker": {
        "generator": "safe_email"
      }
    },
    "is_active": {
      "type": "bool",
      "frequency": 0.1
    },
    "last_login_at": {
      "type": "date_time",
      "format": "%Y-%m-%dT%H:%M:%S%z",
      "begin": "2018-01-02T00:00:00+0000",
      "end": "2020-01-01T00:00:00+0000"
    },
    "transactions": {
      "type": "array",
      "length": {
        "type": "number",
        "subtype": "u64",
        "constant": $arraya_size
      },
      "content": {
        "type": "object",
        "id": {
          "type": "number",
          "subtype": "u64",
          "id": {}
        },
        "credit_card": {
          "type": "string",
          "faker": {
            "generator": "credit_card"
          }
        }
      }
    },
    "info": {
      "type": "object",
      "random_a": {
        "type": "date_time",
        "format": "%Y-%m-%dT%H:%M:%S%z",
        "begin": "2015-01-01T00:00:00+0000",
        "end": "2018-01-01T00:00:00+0000"
      },
      "random_b": {
        "type": "date_time",
        "format": "%Y-%m-%dT%H:%M:%S%z",
        "begin": "2015-01-01T00:00:00+0000",
        "end": "2018-01-01T00:00:00+0000"
      },
      "random_c": {
        "type": "date_time",
        "format": "%Y-%m-%dT%H:%M:%S%z",
        "begin": "2015-01-01T00:00:00+0000",
        "end": "2018-01-01T00:00:00+0000"
      }
    }
  }
}
END
)

# check if synth command exists
if ! command -v synth &> /dev/null
then
    echo "synth command not found. Aborting."
    echo "Install it with:"
    echo "$ curl -sSL https://getsynth.com/install | sh"
    exit 1
fi

mkdir -p synth_collection_tmp
clean_up () {
  ARG=$?
  rm -rf synth_collection_tmp
  exit $ARG
}
trap clean_up EXIT

echo "$synth_collection" > synth_collection_tmp/users.json
synth generate synth_collection_tmp --collection users --size $2 --to json:synth_collection_tmp/users-generated.json


# test if spicepod.yaml exists
if [ -f "spicepod.yaml" ]; then
  echo "spicepod.yaml found. Aborting."
  exit 1
fi

echo "version: v1" >> spicepod.yaml
echo "kind: Spicepod" >> spicepod.yaml
echo "name: test" >> spicepod.yaml
echo "datasets:" >> spicepod.yaml
echo "  - from: duckdb:read_json('synth_collection_tmp/users-generated.json')" >> spicepod.yaml
echo "    name: structs" >> spicepod.yaml
echo "    acceleration:" >> spicepod.yaml
echo "      enabled: true" >> spicepod.yaml

spiced
