import csv
import os
import tempfile
import uuid
from datetime import datetime

from google.cloud import bigquery, storage


PROJECT_ID = os.getenv("GCP_PROJECT", "sp-uat-gdt-udp-datalake")
DATASET_ID = "nmp"


def gcs_to_bigquery(event, context=None):
    """
    CSV from GCS -> BigQuery with Action-aware processing.

    Business key: (Service, TxnNo)
    - Action = 'X'  -> DELETE matching block
    - Action in ('I','R') -> UPSERT block (latest per (Service,TxnNo) within the file)
    Adds 3 generated columns in destination:
      - batch_unique_id (STRING)
      - partition_yearmonth (STRING 'YYYYMM')
      - ingestion_ts (TIMESTAMP)
    """

    # --- HTTP or GCS event parsing ---
    if hasattr(event, "get_json"):  # HTTP
        body = event.get_json(silent=True) or {}
        bucket_name = body.get("bucket")
        file_name = body.get("name")
        if not bucket_name or not file_name:
            return ("Missing 'bucket' or 'name' in request body", 400)
    else:  # GCS event
        bucket_name = event.get("bucket")
        file_name = event.get("name")
        if not bucket_name or not file_name:
            raise ValueError(f"Invalid GCS event payload: {event}")

    print(f"Triggered for bucket={bucket_name}, file={file_name}")

    storage_client = storage.Client()
    bq = bigquery.Client()

    uri = f"gs://{bucket_name}/{file_name}"
    table_id = "npm_ingestion"
    dest_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    stg_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}_staging"

    # --- Read header to build STRING schema for staging/destination core cols ---
    tmp_path = os.path.join(tempfile.gettempdir(), os.path.basename(file_name))
    storage_client.bucket(bucket_name).blob(file_name).download_to_filename(tmp_path)
    with open(tmp_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        headers = [h.strip().strip('"') for h in next(reader)]

    # Required cols for key/action
    required = {"Service", "TxnNo", "Action"}
    missing_req = required - set(headers)
    if missing_req:
        msg = f"Missing required column(s) {missing_req} in file header."
        print("ERROR:", msg)
        if hasattr(event, "get_json"):
            return (msg, 400)
        return

    src_schema = [bigquery.SchemaField(h, "STRING") for h in headers]

    # --- Ensure destination table exists with extra 3 generated columns ---
    dest_schema = list(src_schema) + [
        bigquery.SchemaField("batch_unique_id", "STRING"),
        bigquery.SchemaField("partition_yearmonth", "STRING"),
        bigquery.SchemaField("ingestion_ts", "TIMESTAMP"),
    ]
    try:
        bq.get_table(dest_ref)
    except Exception:
        table = bigquery.Table(dest_ref, schema=dest_schema)
        # Optional partitioning:
        # table.time_partitioning = bigquery.TimePartitioning(
        #     type_=bigquery.TimePartitioningType.MONTH, field="ingestion_ts"
        # )
        bq.create_table(table)
        print(f"Created destination table {dest_ref}")

    # --- Schema guard (compare only core columns, ignore the 3 generated) ---
    dest_tbl = bq.get_table(dest_ref)
    dest_core = [f.name for f in dest_tbl.schema
                 if f.name not in ("batch_unique_id", "partition_yearmonth", "ingestion_ts")]
    if dest_core != headers:
        newcols = set(headers) - set(dest_core)
        missing = set(dest_core) - set(headers)
        msg = f"Schema mismatch. New={newcols} Missing={missing}. Skipping load."
        print("WARNING:", msg)
        if hasattr(event, "get_json"):
            return (msg, 200)
        return

    # --- Ensure staging table exists (CSV schema only) ---
    try:
        bq.get_table(stg_ref)
    except Exception:
        bq.create_table(bigquery.Table(stg_ref, schema=src_schema))
        print(f"Created staging table {stg_ref}")

    # --- Load CSV into staging (truncate each run) ---
    load_cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        field_delimiter=",",
        quote_character='"',
        allow_quoted_newlines=True,
        encoding="UTF-8",
    )
    bq.load_table_from_uri(uri, stg_ref, job_config=load_cfg).result()
    print(f"Staged {uri} into {stg_ref}")

    # --- Action processing ---
    batch_id = uuid.uuid4().hex
    yyyymm = datetime.utcnow().strftime("%Y%m")

    # 1) DELETE blocks requested by Action='X'
    delete_sql = f"""
    DELETE FROM `{dest_ref}`
    WHERE (Service, TxnNo) IN (
      SELECT DISTINCT Service, TxnNo
      FROM `{stg_ref}`
      WHERE UPPER(IFNULL(Action,'')) = 'X'
    )
    """
    bq.query(delete_sql).result()
    print("Applied deletes (Action='X').")

    # 2) UPSERT latest (Service,TxnNo) rows from I/R
    #    - Pick latest by TxnDt where parseable; otherwise ROW_NUMBER() still yields one row
    merge_sql = f"""
    MERGE `{dest_ref}` T
    USING (
      SELECT *
      FROM (
        SELECT
          S.*,
          ROW_NUMBER() OVER (
            PARTITION BY S.Service, S.TxnNo
            ORDER BY SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', S.TxnDt) DESC
          ) AS rn
        FROM `{stg_ref}` S
        WHERE UPPER(IFNULL(S.Action,'')) IN ('I','R')
      )
      WHERE rn = 1
    ) S
    ON T.Service = S.Service AND T.TxnNo = S.TxnNo
    WHEN MATCHED THEN
      UPDATE SET
        {", ".join([f"T.`{c}` = S.`{c}`" for c in headers])},
        T.batch_unique_id = "{batch_id}",
        T.partition_yearmonth = "{yyyymm}",
        T.ingestion_ts = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT ({", ".join([*(f"`{c}`" for c in headers), "`batch_unique_id`", "`partition_yearmonth`", "`ingestion_ts`"])})
      VALUES ({", ".join([*(f"S.`{c}`" for c in headers), f'"{batch_id}"', f'"{yyyymm}"', "CURRENT_TIMESTAMP()"])})
    """
    bq.query(merge_sql).result()
    print("Applied upserts (Action IN I/R).")

    msg = f"Processed {file_name} into {dest_ref}"
    print(msg)
    if hasattr(event, "get_json"):
        return (msg, 200)
