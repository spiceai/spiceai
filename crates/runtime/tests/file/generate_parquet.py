import pandas as pd


def main():
    rows = [
        {
            "integer": 1,
            "text": "a",
            "boolean": True,
            "float": 1.0,
            "timestamp": "2021-01-01T00:00:00",
            "date": "2021-01-01",
            "time": "00:00:00",
            "list": [1, 2, 3],
            "set": {1, 2, 3},
            "dict": {"a": 1, "b": 2},
        },
    ]

    dtypes = {
        "integer": "int64",
        "text": "string",
        "boolean": "bool",
        "float": "float64",
        "timestamp": "datetime64[ns]",
        "date": "datetime64[ns]",
        "time": "datetime64[ns]",
        "list": "object",
        "set": "object",
        "dict": "object",
    }

    df = pd.DataFrame(rows)
    for col, dtype in dtypes.items():
        if col == "time":
            df[col] = pd.to_datetime(df[col]).dt.time
        elif col == "date":
            df[col] = pd.to_datetime(df[col]).dt.date
        elif col == "timestamp":
            df[col] = pd.to_datetime(df[col])
        else:
            df[col] = df[col].astype(dtype)

    df.to_parquet("datatypes.parquet", index=False)


if __name__ == "__main__":
    main()
