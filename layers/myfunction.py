try:

    import json
    import sys
    import requests
    from deltalake import DeltaTable, write_deltalake
    import pandas as pd
    import polars as pl
    import pyarrow as pa
    print("All imports ok ...")
except Exception as e:
    print("Error Imports : {} ".format(e))


def lambda_handler(event, context):
    df = pd.DataFrame({"num": [5, 6], "animal": ["cat", "dog"]})
    delta_path = 's3://processed-deltalakes/'
    write_deltalake(delta_path, df)
    dt = DeltaTable(delta_path).to_pandas()
    print("Hello AWS!")
    print("event = {}".format(event))
    return {
        'statusCode': 200,
    }