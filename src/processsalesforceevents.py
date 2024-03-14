try:
    import json
    import sys
    import time
    from deltalake import DeltaTable, write_deltalake
    import pandas as pd
    import polars as pl
    import pyarrow as pa
    import inflect
    import boto3
    from datetime import datetime
    print("All imports ok ...")

except Exception as e:
    print("Error Imports : {} ".format(e))


def lambda_handler(event, context):
    # change s3 bucket path
    delta_path = 's3://bosa-shared-dev-data-warehouse-us-west-1/salesforce-events/'
    max_retries = 3
    retry_delay_seconds = 5
    # Specify your Athena table and partition column
    table_name = 'salesforce_employee'
    partition_column = 'entityname'
    db_name = 'salesforce_poc'
    #OutputLocation = ''
    new_partitions = ['Employee__c']

    for retry_count in range(max_retries):
        print("event = {}".format(event))
        result_dict = handle_json_event(event)
        #keys_to_track = this could be list of attributes where we want to track changes for. Don't use keys to remove.
        #keys_to_remove = ["version","id","detail-type","source","account","time","region","resources"]
        keys_to_select = ["entityName","changeType", "changedFields", "changeOrigin", "recordIds", "First_Name__c", "Last_Name__c", "CreatedDate", "Name", "Tenure__c", "schemaId"]
        try:
            result_dict = {key: result_dict[key] for key in result_dict if key in keys_to_select}
        except Exception as ex:
            print(f"Error: {ex}")
            print("no keys")

        result_dict = {key: value for key, value in result_dict.items() if value is not None}

        #Entity Name needs to be appended once only.
        if retry_count < 1:
            entityname = result_dict.get('entityName')
            delta_path = delta_path + entityname + "/" + partition_column + "=" + entityname + "/"
            print(f"Delta Path: {delta_path}")

        if result_dict.get('changeType') == 'CREATE':
            try:
                create_deltalake_events(result_dict, delta_path)
                #addtablepartionsifnotexist(table_name, db_name, partition_column, new_partitions)
                break
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(retry_delay_seconds)

        elif result_dict.get('changeType') == 'UPDATE':
            try:
                if any(x in result_dict.get('changedFields') for x in keys_to_select):
                    update_deltalake_events(result_dict, delta_path)
                    #addtablepartionsifnotexist(table_name, db_name, partition_column, new_partitions)
                    break
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(retry_delay_seconds)

        elif result_dict.get('changeType') == 'DELETE':
            try:
                delete_deltalake_events(result_dict, delta_path)
                #addtablepartionsifnotexist(table_name, db_name, partition_column, new_partitions)
                break
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(retry_delay_seconds)

        elif result_dict.get('changeType') == 'UNDELETE':
            try:
                Undelete_deltalake_events(result_dict, delta_path)
                #addtablepartionsifnotexist(table_name, db_name, partition_column, new_partitions)
                break
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(retry_delay_seconds)
    print("For loop ended.")
    addtablepartionsifnotexist(table_name, db_name, partition_column, new_partitions)


def check_query_status(query_execution_id):
    athena_client = boto3.client('athena')
    response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
    state = response['QueryExecution']['Status']['State']
    return state

def get_query_results(query_execution_id):
    athena_client = boto3.client('athena')
    response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    return response
def addtablepartionsifnotexist(table_name, db_name, partition_column, new_partitions):
    """

    @param table_name: Glue table Name
    @param db_name: DB Name
    @param partition_column: Existing Table Partition Column
    @param new_partitions: New Partitions that need to be added in the column
    @return:
    """

    athena_client = boto3.client('athena')
    # Check existing partitions
    print(f"Table Name: {table_name}")
    print(f"DB Name: {db_name}")
    print(f"Partition: {partition_column}")
    print(f"New Partition: {new_partitions}")

    response = athena_client.start_query_execution(
        QueryString=f"SHOW PARTITIONS {table_name}",
        QueryExecutionContext={'Database': db_name},
        #ResultConfiguration={'OutputLocation': OutputLocation}
    )

    # Get the query execution ID
    query_execution_id = response['QueryExecutionId']
    # Poll until the query completes
    while True:
        state = check_query_status(query_execution_id)
        print(f"query state: {state}")
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break  # Exit the loop if the query has completed

        time.sleep(5)  # Wait for 5 seconds before checking again

    # Wait for the query to complete and retrieve the results
    query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    print(f"Query Results: {query_results}")
    # Extract existing partition values from the results
    existing_partitions = [row['Data'][0]['VarCharValue'] for row in query_results['ResultSet']['Rows'][1:]]

    # Generate ALTER TABLE statements for missing partitions
    alter_table_statements = [
        f"ALTER TABLE {table_name} ADD PARTITION ({partition_column}='{value}');"
        for value in new_partitions if value not in existing_partitions
    ]

    # Execute ALTER TABLE statements
    for statement in alter_table_statements:
        athena_client.start_query_execution(
            QueryString=statement,
            QueryExecutionContext={'Database': db_name},
            #ResultConfiguration={'OutputLocation': OutputLocation}
        )
def handle_json_event(data, parent_key="", result_dict=None):
    """
    This fucntion performs some cleanup on the result_dict to remove empty values and handles nested objects.

    @param data: Event data recieved from salesforce.
    @param parent_key: This will handle nested attribute key in event's payload.
    @param result_dict: A dictionary with valid values (Empty values removed from the event dictionary).
    @return: result_dict.
    """
    if result_dict is None:
        result_dict = {}
    for key, value in data.items():
        new_key = key if not parent_key else f"{key}"
        if isinstance(value, dict):
            handle_json_event(value, parent_key=new_key, result_dict=result_dict)
        else:
            result_dict[new_key] = value if value != [] else None
    return result_dict

def create_deltalake_events(result_dict, delta_path):
    """
    @param result_dict: Dictionary with the attributes that need to be catered in delta lake
    @param delta_path: S3 path where delta lake parquet files are saved.
    """
    new_dict = convert_lists_to_strings(result_dict)
    new_dict = {key.lower(): value for key, value in new_dict.items()}
    new_dict["deleteflag"] = False
    new_dict["currentflag"] = True
    new_dict["start_date"] = datetime.now().strftime("%Y-%m-%d")
    new_dict["end_date"] = ""
    df = pl.from_dict(new_dict)
    print(df)
    df.write_delta(delta_path, mode="append")

def delete_deltalake_events(result_dict, delta_path):
    """
    @param result_dict: Dictionary with the attributes that need to be catered in delta lake
    @param delta_path: S3 path where delta lake parquet files are saved.
    """
    dt = DeltaTable(delta_path)
    # new_dict = columns_manipulation(result_dict)
    new_dict = {key.lower(): value for key, value in result_dict.items()}
    df = pl.from_dict(new_dict)
    source = schema_conversion(df)
    (
        dt.merge(source=source,
             predicate='s.recordids = t.recordids',
             source_alias='s',
             target_alias='t')
             .when_matched_update(updates={"deleteflag":"true"})
             .execute()
    )

def Undelete_deltalake_events(result_dict, delta_path):
    """
    @param result_dict: Dictionary with the attributes that need to be catered in delta lake
    @param delta_path: S3 path where delta lake parquet files are saved.
    """
    dt = DeltaTable(delta_path)
    # new_dict = columns_manipulation(result_dict)
    new_dict = {key.lower(): value for key, value in result_dict.items()}
    df = pl.from_dict(new_dict)
    source = schema_conversion(df)
    (
        dt.merge(
            source=source,
            predicate='s.recordids = t.recordids',
            source_alias='s',
            target_alias='t')
            .when_matched_update(updates={"deleteflag":"false"})
            .execute()
    )

def update_deltalake_events(result_dict, delta_path):
    """
    @param result_dict: Dictionary with the attributes that need to be catered in delta lake
    @param delta_path: S3 path where delta lake parquet files are saved.
    """
    new_dict = convert_lists_to_strings(result_dict)
    new_dict = {key.lower(): value for key, value in new_dict.items()}
    new_dict["deleteflag"] = False
    new_dict["currentflag"] = True
    new_dict["start_date"] = datetime.now().strftime("%Y-%m-%d")
    new_dict["end_date"] = ""
    df = pl.from_dict(new_dict)
    print(df)

    old_df = pl.read_delta(delta_path)
    new_df_to_insert = df.join(old_df, on="recordids", how='semi')
    new_df_to_insert = new_df_to_insert.with_columns(pl.lit('').alias("mergekey")).select(pl.all())

    df = df.with_columns(pl.col("recordids").alias("mergekey")).select(pl.all())
    staged_df = pl.concat([new_df_to_insert, df])

    dynamic_cols = list(new_dict.keys())

    my_delta_table = DeltaTable(delta_path)
    source_staged_df = staged_df.to_arrow()
    delta_schema = _convert_pa_schema_to_delta(source_staged_df.schema)
    source_staged_df = source_staged_df.cast(delta_schema)

    insert_updates = {col: f"staged_updates.{col}" for col in dynamic_cols}
    insert_updates["currentflag"] = "true"
    insert_updates["end_date"] = "null"

    (
        my_delta_table.merge(
            source=source_staged_df,
            predicate="my_target.recordids = staged_updates.mergekey",
            source_alias="staged_updates",
            target_alias="my_target",
        )
        .when_matched_update(predicate="my_target.currentflag = true",
                             updates={"currentflag": "false", "end_date": "staged_updates.start_date"})
        .when_not_matched_insert(updates=insert_updates)
        .execute()
    )

    print(my_delta_table.to_pandas())


def convert_lists_to_strings(data) -> dict:
    """
    Converts a list of values to a comma separated string.
    @param data: A dictionary with key value pairs.
    @return: A dictionary where list values are converted to a string with comma separated values
    """
    for key, value in data.items():
        if isinstance(value, dict):
            convert_lists_to_strings(value)
        elif isinstance(value, list):
            data[key] = ', '.join(map(str, value))
    return data

def schema_conversion(df):
    """
    @param df: Polars Dataframe that needs to be converted to Pyarrow dataframe.
    @return: A dataframe with delta lake supported schema.
    """
    source = df.to_arrow()
    delta_schema = _convert_pa_schema_to_delta(source.schema)
    return source.cast(delta_schema)


def _convert_pa_schema_to_delta(schema: pa.schema) -> pa.schema:
    """Converts a PyArrow schema to a schema compatible with Delta Lake."""
    # TODO: Add time zone support
    dtype_map = {
        pa.uint8(): pa.int8(),
        pa.uint16(): pa.int16(),
        pa.uint32(): pa.int32(),
        pa.uint64(): pa.int64(),
        pa.timestamp("ns"): pa.timestamp("us"),
        pa.timestamp("ms"): pa.timestamp("us"),
        pa.large_string(): pa.string(),
        pa.large_binary(): pa.binary(),
    }

    def dtype_to_delta_dtype(dtype: pa.DataType) -> pa.DataType:
        # Handle nested types
        if isinstance(dtype, pa.LargeListType):
            return list_to_delta_dtype(dtype)
        elif isinstance(dtype, pa.StructType):
            return struct_to_delta_dtype(dtype)

        try:
            return dtype_map[dtype]
        except KeyError:
            return dtype

    def list_to_delta_dtype(dtype: pa.LargeListType) -> pa.ListType:
        nested_dtype = dtype.value_type
        nested_dtype_cast = dtype_to_delta_dtype(nested_dtype)
        return pa.list_(nested_dtype_cast)

    def struct_to_delta_dtype(dtype: pa.StructType) -> pa.StructType:
        fields = [dtype.field(i) for i in range(dtype.num_fields)]
        fields_cast = [pa.field(f.name, dtype_to_delta_dtype(f.type)) for f in fields]
        return pa.struct(fields_cast)

    return pa.schema([pa.field(f.name, dtype_to_delta_dtype(f.type)) for f in schema])