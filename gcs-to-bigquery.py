# main.py
import csv
import os
import tempfile
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery, storage

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
PROJECT_ID = os.getenv("GCP_PROJECT", "sp-uat-gdt-udp-datalake")

# Existing BigQuery tables (MUST already exist)
RAW_DATASET = "parcel_tracking_raw"
RAW_TABLE = "moss_ingestion"             # CSV columns as STRING + load_date STRING
STG_TABLE = "moss_ingestion_staging"     # CSV columns only, all STRING

CURATED_DATASET = "parcel_tracking_curated"
DEST_TABLE = "daily_charges_file"        # typed curated table (+ 3 generated cols)

# Format of TxnDt in file;
TXNDT_FORMAT = "%Y-%m-%d %H:%M:%S"

# ──────────────────────────────────────────────────────────────────────────────
# Columns typed in curated table (for cast mapping)
# ──────────────────────────────────────────────────────────────────────────────
NUMERIC_COLS = {
    "ReceiptAmt", "CollectedAmt", "RefundAmt", "Rate",
    "GrossAmt", "NetAmt", "GSTAmt", "ActualTotalWtkg",
    "RegisteredAmt", "KGRate", "MbagWtKg",
    "Mbag1stXkgRate", "MbagAddkgRate",
}
INTEGER_COLS = {
    "Qty", "ItemWtgm", "PerItemWtStepgm",
    "MbagQty", "Mbag1stXkg", "PPIVol", "EDSLineNo",
}
# Everything else from the CSV is STRING in curated.

# ──────────────────────────────────────────────────────────────────────────────
# Cloud Function entrypoint
# ──────────────────────────────────────────────────────────────────────────────
def gcs_to_bigquery(event, context=None):
    """
    Flow:
      GCS CSV -> STAGING (truncate & load) ->
      RAW (append with load_date=YYYYMMDD) ->
      CURATED (DELETE where Action='X', then UPSERT latest (Service,TxnNo) for Action IN ('I','R')).

    Business key: (Service, TxnNo)
    Generated columns in CURATED:
      - batch_unique_id (STRING)
      - partition_yearmonth (INTEGER YYYYMM)
      - ingestion_ts (TIMESTAMP)
    """

    # ── Parse trigger (GCS or HTTP) ───────────────────────────────────────────
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

    print(f"[Trigger] bucket={bucket_name}, file={file_name}")

    storage_client = storage.Client()
    bq = bigquery.Client()

    uri = f"gs://{bucket_name}/{file_name}"

    raw_ref = f"{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}"
    stg_ref = f"{PROJECT_ID}.{RAW_DATASET}.{STG_TABLE}"
    dest_ref = f"{PROJECT_ID}.{CURATED_DATASET}.{DEST_TABLE}"

    # Derived tags (UTC)
    now_utc = datetime.now(timezone.utc)
    load_date = now_utc.strftime("%Y%m%d")        # RAW tag as STRING
    part_yyyymm = int(now_utc.strftime("%Y%m"))   # INTEGER for curated.partition_yearmonth
    batch_id = uuid.uuid4().hex

    # ── Read CSV header to validate required columns ──────────────────────────
    tmp_path = os.path.join(tempfile.gettempdir(), os.path.basename(file_name))
    storage_client.bucket(bucket_name).blob(file_name).download_to_filename(tmp_path)

    with open(tmp_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        headers = [h.strip().strip('"') for h in next(reader)]

    required = {"Service", "TxnNo", "Action"}
    missing_req = required - set(headers)
    if missing_req:
        msg = f"Missing required column(s) {missing_req} in file header."
        print("ERROR:", msg)
        if hasattr(event, "get_json"):
            return (msg, 400)
        return

    # ── Verify tables exist & schema order guards (no creation here) ──────────
    try:
        stg_tbl = bq.get_table(stg_ref)
        raw_tbl = bq.get_table(raw_ref)
        dest_tbl = bq.get_table(dest_ref)
    except Exception as e:
        msg = f"Required table missing: {e}"
        print("ERROR:", msg)
        if hasattr(event, "get_json"):
            return (msg, 500)
        return

    stg_cols = [f.name for f in stg_tbl.schema]
    if stg_cols != headers:
        msg = f"Staging schema mismatch. Expected {stg_cols}, got file headers {headers}"
        print("ERROR:", msg)
        if hasattr(event, "get_json"):
            return (msg, 400)
        return

    raw_cols = [f.name for f in raw_tbl.schema]
    if not raw_cols or raw_cols[-1] != "load_date" or raw_cols[:-1] != headers:
        msg = "RAW schema mismatch. RAW must have all CSV columns (same order) plus trailing 'load_date'."
        print("ERROR:", msg)
        if hasattr(event, "get_json"):
            return (msg, 400)
        return

    dest_core = [f.name for f in dest_tbl.schema
                 if f.name not in ("batch_unique_id", "partition_yearmonth", "ingestion_ts")]
    if dest_core != headers:
        msg = "DEST schema mismatch. DEST must start with CSV columns (same order), then the 3 generated fields."
        print("ERROR:", msg)
        if hasattr(event, "get_json"):
            return (msg, 400)
        return

    # ── STAGING load (truncate & load CSV) ────────────────────────────────────
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
    print(f"[Stage] {uri} -> {stg_ref} (WRITE_TRUNCATE)")

    # ── Append into RAW with load_date ────────────────────────────────────────
    cols_csv = ", ".join(f"`{c}`" for c in headers)
    insert_raw_sql = f"""
    INSERT INTO `{raw_ref}` ({cols_csv}, `load_date`)
    SELECT {cols_csv}, '{load_date}'
    FROM `{stg_ref}`
    """
    bq.query(insert_raw_sql).result()
    print(f"[Raw Append] {stg_ref} -> {raw_ref} (load_date={load_date})")

    # ── CURATED: DELETE where Action='X' (use EXISTS) ─────────────────────────
    delete_sql = f"""
    DELETE FROM `{dest_ref}` AS T
    WHERE EXISTS (
      SELECT 1
      FROM `{raw_ref}` R
      WHERE R.load_date = '{load_date}'
        AND UPPER(IFNULL(R.Action,'')) = 'X'
        AND R.Service = T.Service
        AND R.TxnNo = T.TxnNo
    )
    """
    bq.query(delete_sql).result()
    print("[Action] DELETE applied for Action='X'.")

    # ── Build CASTed source for MERGE (Raw → Curated types) ───────────────────
    def cast_expr(col: str) -> str:
        if col in NUMERIC_COLS:
            return f"SAFE_CAST(NULLIF(TRIM(R.`{col}`), '') AS NUMERIC) AS `{col}`"
        if col in INTEGER_COLS:
            return f"SAFE_CAST(NULLIF(TRIM(R.`{col}`), '') AS INT64) AS `{col}`"
        # default STRING → leave as-is
        return f"R.`{col}` AS `{col}`"

    cast_select_list = ",\n          ".join(cast_expr(c) for c in headers)

    set_all = ", ".join([f"T.`{c}` = S.`{c}`" for c in headers])
    insert_cols = ", ".join(
        [*(f"`{c}`" for c in headers), "`batch_unique_id`", "`partition_yearmonth`", "`ingestion_ts`"]
    )
    insert_vals = ", ".join(
        [*(f"S.`{c}`" for c in headers), f"'{batch_id}'", f"{part_yyyymm}", "CURRENT_TIMESTAMP()"]
    )

    # ── CURATED: UPSERT latest per (Service,TxnNo) where Action IN ('I','R') ──
    merge_sql = f"""
    MERGE `{dest_ref}` T
    USING (
      SELECT *
      FROM (
        SELECT
          {cast_select_list},
          ROW_NUMBER() OVER (
            PARTITION BY R.Service, R.TxnNo
            ORDER BY SAFE.PARSE_TIMESTAMP('{TXNDT_FORMAT}', R.TxnDt) DESC
          ) AS rn
        FROM `{raw_ref}` R
        WHERE R.load_date = '{load_date}'
          AND UPPER(IFNULL(R.Action,'')) IN ('I','R')
      )
      WHERE rn = 1
    ) S
    ON T.Service = S.Service AND T.TxnNo = S.TxnNo
    WHEN MATCHED THEN UPDATE SET
      {set_all},
      T.batch_unique_id = '{batch_id}',
      T.partition_yearmonth = {part_yyyymm},
      T.ingestion_ts = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols})
      VALUES ({insert_vals})
    """
    bq.query(merge_sql).result()
    print("[Action] UPSERT applied for Action IN ('I','R').")

    msg = f"OK: {uri} -> RAW {raw_ref} (load_date={load_date}) -> MERGE into {dest_ref}"
    print(msg)
    if hasattr(event, "get_json"):
        return (msg, 200)
