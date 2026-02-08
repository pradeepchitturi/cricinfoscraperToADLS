import os
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- ADLS Configuration ---
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME", "adlschitturidemo")
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")
ADLS_CONTAINER_NAME = "cricinfo-mens-international"


def get_adls_client():
    """Authenticate and return ADLS Gen2 client"""
    if not ADLS_ACCOUNT_KEY:
        raise ValueError("ADLS_ACCOUNT_KEY environment variable not set")

    return DataLakeServiceClient(
        account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
        credential=ADLS_ACCOUNT_KEY
    )


def upload_to_adls(service_client, container_name, file_path, data: bytes):
    """Upload file to ADLS Gen2"""
    file_system_client = service_client.get_file_system_client(container_name)
    file_client = file_system_client.get_file_client(file_path)

    file_client.upload_data(data, overwrite=True)
    print(f"✅ Uploaded file to ADLS: {file_path}")


def save_dataframe_to_adls(df, partition_key_column,file_format,file_name):
    """Process table and upload partitioned data to ADLS."""

    adls_client = get_adls_client()
    if adls_client is None:
        return

    try:
        if df.empty:
            print(f"No data found in dataframe")
            return

        print(f"Found {len(df)} rows in {file_name} dataframe")

        # Ensure the partition key column exists
        if partition_key_column not in df.columns:
            print(f"Partition key column '{partition_key_column}' not found.")
            print(f"Available columns: {df.columns.tolist()}")
            return

        # 2. Partition data by key column
        unique_keys = df[partition_key_column].nunique()
        print(f"Partitioning by '{partition_key_column}' - {unique_keys} unique values")

        for key_value, group in df.groupby(partition_key_column):
            # Define ADLS directory path for partitioning
            # Format: table_name/partition_key=value/data.csv

            if file_format.lower() == "csv":
                data = group.to_csv(index=False).encode("utf-8")
                partition_path = f"cricket_commentary/{partition_key_column}={key_value}/{file_name}_data.csv"

            elif file_format.lower() == "json":
                data = group.to_json(orient="records", lines=True).encode("utf-8")
                partition_path = f"cricket_commentary/{partition_key_column}={key_value}/{file_name}_data.json"

            else:
                raise ValueError("Unsupported file format. Use csv or json")

            upload_to_adls(adls_client, ADLS_CONTAINER_NAME, partition_path, data)

            print(f"Successfully processed all partitions for {file_name}")

    except Exception as e:
        print(f"An error occurred during processing: {e}")
        import traceback
        traceback.print_exc()

