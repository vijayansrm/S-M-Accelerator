"""
FastAPI application for uploading, merging, and validating CSV files.
Supports large file streaming and dynamic Pydantic validation.
"""
# pylint: disable=too-many-return-statements,too-many-locals,too-many-branches,too-many-statements,broad-exception-caught,unsubscriptable-object,unsupported-assignment-operation,line-too-long,trailing-whitespace
# Standard library imports
import asyncio
from datetime import date, datetime, timezone
import gc
import json
import logging
import os
from typing import Any, List, Optional, Union
import uuid

# Third-party library imports
from contextlib import asynccontextmanager
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd
from pydantic import BaseModel, Field, ConfigDict, ValidationError

# Local module imports
import config         # Application configuration settings
from mysqlConnecter import db_connector # For database enrichment

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Configure a separate logger for invalid records
os.makedirs(config.LOG_DIR, exist_ok=True)
invalid_logger = logging.getLogger("invalid_records")
invalid_logger.setLevel(logging.ERROR)
invalid_logger.propagate = False
invalid_handler = logging.FileHandler(config.INVALID_RECORDS_LOG_PATH, mode='a', encoding='utf-8')
invalid_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
invalid_logger.addHandler(invalid_handler)

async def cleanup_old_files():
    """Background task to delete files older than 48 hours."""
    while True:
        try:
            logger.info("Running scheduled cleanup of old files...")
            now = datetime.now(timezone.utc).timestamp()
            cutoff = now - (48 * 3600)  # 48 hours

            dirs_to_clean = [config.LOG_DIR, config.RESPONSE_DIR]
            for directory in dirs_to_clean:
                if not os.path.exists(directory):
                    continue
                for filename in os.listdir(directory):
                    filepath = os.path.join(directory, filename)
                    if os.path.isfile(filepath):
                        # Don't delete the main invalid records log
                        if filepath == config.INVALID_RECORDS_LOG_PATH:
                            continue
                        if os.path.getmtime(filepath) < cutoff:
                            os.remove(filepath)
                            logger.info("Cleaned up old file: %s", filepath)
        except Exception as e:
            logger.error("Error during file cleanup: %s", e)
        
        # Sleep for 1 hour
        await asyncio.sleep(3600)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the cleanup background task
    task = asyncio.create_task(cleanup_old_files())
    yield
    # Cancel the task on shutdown
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

# Initialize the FastAPI application
app = FastAPI(title=config.API_TITLE, lifespan=lifespan)

def _normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes all column names to lowercase and handles duplicates."""
    new_cols = []
    seen = {}
    for col in df.columns:
        base = str(col).strip().lower()
        if base in seen:
            seen[base] += 1
            new_cols.append(f"{base}_{seen[base]}")
        else:
            seen[base] = 0
            new_cols.append(base)
    df.columns = new_cols
    
    # Map alternative column names to standard schema
    rename_map = {
        'transaction_datetime': 'timestamp',
        'product_id': 'productid'
    }
    df = df.rename(columns=rename_map)
    return df

# Pydantic Schema for Claim Data
class ClaimSchema(BaseModel):
    transaction_id: str = Field(..., alias="transaction_id")
    timestamp: datetime = Field(..., alias="timestamp")
    patient_token: str = Field(..., alias="patient_token")
    prescriber_npi: Union[int, str] = Field(..., alias="prescriber_npi")
    pharmacy_id: str = Field(..., alias="pharmacy_id")
    ndc_code: str = Field(..., alias="ndc_code")
    payer_bin: Union[int, str] = Field(..., alias="payer_bin")
    response_status: str = Field(..., alias="response_status")
    zip_code: Optional[Union[int, str]] = Field(None, alias="zip_code")
    payer_id: Optional[Union[int, str]] = Field(None, alias="payer_id")
    productid: Optional[Union[str, int]] = Field(None, alias="productid")
    claim_status: Optional[str] = Field(None, alias="claim_status")
    reject_code: Optional[Union[int, str]] = Field(None, alias="reject_code")
    reject_message: Optional[str] = Field(None, alias="reject_message")

    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=True,
        extra='allow' # Allow extra columns after enrichment
    )

# Pydantic Schema for Leakage Activity Data
class LeakageActivitySchema(BaseModel):
    BrandName: str
    Pharmacy_ID: str
    Pharmacy_Rejections: int
    Total_Brand_Rejections: int
    Leakage_Percentage: float
    Prescriber_NPI: Optional[str] = None
    generic_name: Optional[str] = None
    manufacturer: Optional[str] = None
    productid: Optional[Union[str, int]] = None
    Processed_TimeStamp: datetime
    State: Optional[str] = None
    HCP_Name: Optional[str] = None
    zip_code: Optional[str] = None

    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=True
    )

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ALLOW_ORIGINS,
    allow_credentials=config.CORS_ALLOW_CREDENTIALS,
    allow_methods=config.CORS_ALLOW_METHODS,
    allow_headers=config.CORS_ALLOW_HEADERS,
)

def standardize_ndc(val: Any) -> Optional[str]:
    """Standardizes NDC codes into database NDC format (e.g., NDC00249)."""
    if pd.isna(val) or not isinstance(val, (str, int)):
        return None
    val_str = str(val).strip()
    if not val_str:
        return None
    
    # If it already starts with 'NDC', it's already in the DB format
    if val_str.upper().startswith('NDC'):
        return val_str.upper()
        
    # Handle hyphenated format (e.g., 00006-0249-31)
    if '-' in val_str:
        parts = val_str.split('-')
        # The middle part is typically the product code
        if len(parts) >= 2:
            try:
                prod_id = int(parts[1])
                return f"NDC{prod_id:05d}"
            except ValueError:
                pass
                
    # Handle 11-digit or 10-digit unhyphenated numeric format (e.g., 00006024931)
    clean_digits = ''.join(c for c in val_str if c.isdigit())
    if len(clean_digits) in (10, 11):
        try:
            if len(clean_digits) == 11:
                prod_id = int(clean_digits[5:9])
            else:
                prod_id = int(clean_digits[4:8])
            return f"NDC{prod_id:05d}"
        except ValueError:
            pass
            
    # Fallback to try and extract a valid product_id from simple integers
    try:
        prod_id = int(clean_digits)
        if config.NDC_PROD_ID_MIN <= prod_id <= config.NDC_PROD_ID_MAX:
            return f"NDC{prod_id:05d}"
    except ValueError:
        pass
        
    return val_str

async def _load_and_clean_file(file: UploadFile) -> pd.DataFrame:
    """Loads a CSV or Excel file and cleans its headers."""
    try:
        file.file.seek(0)
        filename = file.filename.lower()
        if filename.endswith('.csv'):
            try:
                df = await asyncio.to_thread(pd.read_csv, file.file, encoding='utf-8-sig')
            except (UnicodeDecodeError, Exception):
                logger.info("UTF-8 decoding failed for %s, falling back to latin-1.", file.filename)
                file.file.seek(0)
                df = await asyncio.to_thread(pd.read_csv, file.file, encoding='latin-1')
        elif filename.endswith(('.xlsx', '.xls')):
            df = await asyncio.to_thread(pd.read_excel, file.file)
        else:
            raise ValueError(f"Unsupported file type: {file.filename}")

        # Clean headers
        df.columns = [str(c).strip() if c else f"unknown_{i}" for i, c in enumerate(df.columns)]
        logger.info("Loaded %s with %d rows.", file.filename, len(df))
        return df
    except Exception as e:
        logger.error("Failed to parse file %s: %s", file.filename, e)
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file format: {file.filename}"
        ) from e

async def _merge_dataframes(dataframes: List[pd.DataFrame]) -> pd.DataFrame:
    """Merges a list of dataframes sequentially to optimize memory."""
    if len(dataframes) == 1:
        return dataframes[0]

    logger.info("Merging multiple dataframes...")
    final_df = dataframes[0]
    for i, next_df in enumerate(dataframes[1:], start=1):
        # Find common columns for an intelligent merge
        common_cols = list(set(final_df.columns).intersection(set(next_df.columns)))
        if common_cols:
            logger.info("Merging on common columns: %s", common_cols)
            final_df = await asyncio.to_thread(pd.merge, final_df, next_df, on=common_cols, how='outer')
        else:
            logger.info("No common columns found. Concatenating.")
            final_df = await asyncio.to_thread(
                pd.concat, [final_df, next_df], ignore_index=True, sort=False
            )
        dataframes[i] = None
        gc.collect()
    logger.info("Merge complete.")
    return final_df

def _clean_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """Consistently replaces NaN/Inf values with None for JSON compliance."""
    return df.astype(object).where(pd.notnull(df), None)

def _mask_pii(data: dict) -> dict:
    """Masks sensitive fields in a dictionary for logging."""
    masked = data.copy()
    if 'patient_token' in masked:
        token = str(masked['patient_token'])
        if len(token) > 4:
            masked['patient_token'] = f"{token[:4]}****"
        else:
            masked['patient_token'] = "****"
    return masked

def _validate_and_filter_data(df: pd.DataFrame) -> tuple[pd.DataFrame, List[dict]]:
    """
    Validates each row against ClaimSchema efficiently.
    Returns (valid_df, invalid_records_list).
    """
    valid_rows = []
    invalid_records = []
    validation_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info("Starting optimized row-level validation for %d records.", len(df))
    
    # Pre-convert to dicts for faster iteration than iterrows()
    records = df.to_dict(orient='records')
    
    for row_dict in records:
        try:
            # Rely on Pydantic's built-in type coercion
            validated_row = ClaimSchema(**row_dict).model_dump(mode='json', by_alias=True)
            valid_rows.append(validated_row)
        except (ValidationError, ValueError) as e:
            err_msg = str(e).replace('\n', ' ')
            
            # MASK PII before logging or saving to CSV
            masked_row = _mask_pii(row_dict)
            
            invalid_entry = {
                "Validation_Timestamp": validation_ts,
                "Error_Message": err_msg,
                "Row_Data": json.dumps(masked_row)
            }
            invalid_records.append(invalid_entry)
            # Use invalid_logger for observability
            invalid_logger.error("Validation failed: %s | Data: %s", err_msg, masked_row)
    
    logger.info("Validation complete: %d valid, %d invalid.", len(valid_rows), len(invalid_records))
    return pd.DataFrame(valid_rows), invalid_records

def _log_invalid_to_csv(invalid_records: List[dict], request_id: str) -> Optional[str]:
    """Saves invalid records to a request-specific CSV file."""
    if not invalid_records:
        return None
    
    filename = f"InvalidLog_{request_id}.csv"
    filepath = os.path.join(config.LOG_DIR, filename)
    
    os.makedirs(config.LOG_DIR, exist_ok=True)
    df_invalid = pd.DataFrame(invalid_records)
    df_invalid.to_csv(filepath, index=False)
    logger.info("Invalid records logged to: %s", filepath)
    return filepath

def _get_validated_records(df: pd.DataFrame):
    """Efficient generator for records from a DataFrame."""
    return df.to_dict(orient='records')

def _remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Removes duplicate Patient_Token records, keeping the latest by Timestamp."""
    # Ensure columns exist in normalized lowercase
    target_token_col = 'patient_token'
    target_time_col = 'timestamp'

    if target_token_col in df.columns and target_time_col in df.columns:
        logger.info("Deduplicating based on '%s' and latest '%s'.", target_token_col, target_time_col)
        initial_count = len(df)
        df = df.copy()

        # 1. Standardize Patient_Token
        df[target_token_col] = df[target_token_col].astype(str).str.strip()

        # 2. Parse Timestamps robustly
        df['_temp_ts'] = pd.to_datetime(df[target_time_col].astype(str).str.strip(), errors='coerce', utc=True)

        # 3. Sort and drop duplicates (keep newest)
        df = df.sort_values(by=[target_token_col, '_temp_ts'], ascending=[True, False], na_position='last')
        df = df.drop_duplicates(subset=[target_token_col], keep='first').drop(columns=['_temp_ts'])

        removed = initial_count - len(df)
        logger.info("Deduplication complete. Removed %d records.", removed)
        return df
    else:
        logger.error("Deduplication failed: Missing 'patient_token' or 'timestamp' columns.")
        raise HTTPException(status_code=400, detail="Deduplication failed: patient_token and timestamp columns are required.")

def _calculate_leakage(df: pd.DataFrame) -> List[dict]:
    """
    Calculates leakage metrics based on the plan.
    Formula: Leakage % = (Pharmacy Rejections for Brand / Total Rejections of Brand) * 100
    Expects normalized lowercase column names.
    """
    df = df.copy()
    
    # 1. Ensure mandatory columns exist for grouping
    if 'claim_status' not in df.columns or 'pharmacy_id' not in df.columns:
        logger.warning("Leakage calculation skipped: Required columns (pharmacy_id, claim_status) not found.")
        return []

    # Robust fallback: Ensure brandname is clean, and fill any null/NaN values with fallbacks.
    def clean_brandname(val):
        if pd.isna(val):
            return None
        val_str = str(val).strip()
        if val_str.lower() in ('none', 'nan', ''):
            return None
        return val_str

    if 'brandname' in df.columns:
        df['brandname'] = df['brandname'].apply(clean_brandname)
    else:
        df['brandname'] = None

    # Populate fallback values for missing/null brandnames
    if 'productid' in df.columns:
        fallback_pid = df['productid'].apply(lambda x: f"UnknownBrand_PID_{x}" if pd.notna(x) and str(x).strip().lower() not in ('none', 'nan', '') else None)
        df['brandname'] = df['brandname'].fillna(fallback_pid)

    if 'ndc_code' in df.columns:
        fallback_ndc = df['ndc_code'].apply(lambda x: f"UnknownBrand_NDC_{x}" if pd.notna(x) and str(x).strip().lower() not in ('none', 'nan', '') else None)
        df['brandname'] = df['brandname'].fillna(fallback_ndc)

    df['brandname'] = df['brandname'].fillna('UnknownBrand')

    # Filter for Rejections based on claim_status
    rejected_df = df[df['claim_status'].astype(str).str.upper() == 'REJECTED'].copy()
    
    if rejected_df.empty:
        logger.info("No rejected claims found for leakage calculation.")
        return []

    # 2. Calculate Total Rejections per Brand
    brand_totals = rejected_df.groupby('brandname', dropna=False).size().reset_index(name='total_brand_rejections')

    # 3. Calculate Aggregated Rejections
    # We include extra columns in the grouping if they exist to pass them along
    group_cols = ['brandname', 'pharmacy_id']
    for extra in ['zip_code', 'prescriber_npi', 'full name', 'state']:
        if extra in rejected_df.columns:
            group_cols.append(extra)
    
    # Use dropna=False to avoid losing records where some metadata is missing
    pharmacy_stats = rejected_df.groupby(group_cols, dropna=False).size().reset_index(name='pharmacy_rejections')

    # 4. Merge totals and calculate percentages
    merged_stats = pd.merge(pharmacy_stats, brand_totals, on='brandname', how='left')
    merged_stats['leakage_percentage'] = (
        (merged_stats['pharmacy_rejections'] / merged_stats['total_brand_rejections']) * 100
    ).round().astype(int)

    # Map other metadata columns if they exist
    for col in ['generic_name', 'manufacturer', 'productid']:
        if col in rejected_df.columns:
            mapping = rejected_df.drop_duplicates('brandname').set_index('brandname')[col]
            merged_stats[col] = merged_stats['brandname'].map(mapping)

    # Ensure ZIP_Code fallback
    if 'zip_code' in rejected_df.columns and 'zip_code' not in merged_stats.columns:
        zip_map = rejected_df.drop_duplicates('pharmacy_id').set_index('pharmacy_id')['zip_code']
        merged_stats['zip_code'] = merged_stats['pharmacy_id'].map(zip_map)

    # Rename to CamelCase as expected by LeakageActivitySchema and _insert_leakage_data
    rename_map = {
        'brandname': 'BrandName',
        'pharmacy_id': 'Pharmacy_ID',
        'pharmacy_rejections': 'Pharmacy_Rejections',
        'total_brand_rejections': 'Total_Brand_Rejections',
        'leakage_percentage': 'Leakage_Percentage',
        'prescriber_npi': 'Prescriber_NPI',
        'full name': 'HCP_Name',
        'state': 'State',
        'zip_code': 'zip_code'
    }
    merged_stats = merged_stats.rename(columns=rename_map)

    # Reorder columns to the specified layout (excluding Branch_Name)
    ordered_cols = [
        "BrandName",
        "Pharmacy_ID",
        "zip_code",
        "State",
        "Prescriber_NPI",
        "HCP_Name",
        "Pharmacy_Rejections",
        "Total_Brand_Rejections",
        "Leakage_Percentage",
        "productid",
        "generic_name",
        "manufacturer"
    ]
    
    # Ensure all of these exist in merged_stats (set to None if missing)
    for col in ordered_cols:
        if col not in merged_stats.columns:
            merged_stats[col] = None
            
    merged_stats = merged_stats[ordered_cols]

    # Clean NaN/Inf for JSON compliance
    merged_stats = _clean_for_json(merged_stats)

    records = merged_stats.to_dict(orient='records')
    processed_ts = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    for r in records:
        r['Processed_TimeStamp'] = processed_ts
    return records

async def _fetch_product_master(product_ids: list, ndc_codes: list) -> pd.DataFrame:
    """Fetches specific records from Product_Master table to optimize memory."""
    if not product_ids and not ndc_codes:
        return pd.DataFrame()

    conditions = []
    params = []
    
    # We use format strings to create the correct number of placeholders
    if product_ids:
        placeholders = ', '.join(['%s'] * len(product_ids))
        conditions.append(f"product_id IN ({placeholders})")
        params.extend(product_ids)
        
    if ndc_codes:
        placeholders = ', '.join(['%s'] * len(ndc_codes))
        conditions.append(f"ndc_code IN ({placeholders})")
        params.extend(ndc_codes)
        
    where_clause = " OR ".join(conditions)
    query = f"SELECT * FROM Product_Master WHERE {where_clause}"

    try:
        logger.info("Fetching Product_Master batch from database...")
        results = await asyncio.to_thread(db_connector.fetch_all, query, tuple(params))
        if not results:
            logger.warning("No matching Product_Master records found.")
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        df = _normalize_df_columns(df)
        
        rename_map = {
            'product_id': 'productid',
            'brand_name': 'brandname',
            'generic_name': 'generic_name',
            'manufacturer': 'manufacturer'
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        
        # branch_name is no longer required
        
        return _clean_for_json(df)
        
    except Exception as e:
        logger.error("Failed to fetch Product_Master from database: %s", e)
        return pd.DataFrame()

async def _fetch_hcp_master(npi_codes: list) -> pd.DataFrame:
    """Fetches specific records from hcp_master table to optimize memory."""
    if not npi_codes:
        return pd.DataFrame()

    placeholders = ', '.join(['%s'] * len(npi_codes))
    query = f"SELECT * FROM hcp_master WHERE prescriber_npi IN ({placeholders})"

    try:
        logger.info("Fetching hcp_master batch from database...")
        results = await asyncio.to_thread(db_connector.fetch_all, query, tuple(npi_codes))
        if not results:
            logger.warning("No matching hcp_master records found.")
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        df = _normalize_df_columns(df)

        # Map hcp_master specific columns to standard names
        hcp_rename = {
            'practice_state': 'state',
            'practice_zip': 'zip_code'
        }
        df = df.rename(columns={k: v for k, v in hcp_rename.items() if k in df.columns})

        if 'prescriber_npi' in df.columns:
            df['prescriber_npi'] = df['prescriber_npi'].astype(str).str.strip()
        
        return _clean_for_json(df)
        
    except Exception as e:
        logger.error("Failed to fetch hcp_master from database: %s", e)
        return pd.DataFrame()

async def _fetch_zip_geography(zip_codes: list) -> pd.DataFrame:
    """Fetches state from Zip_Geography_Master based on zip_codes."""
    if not zip_codes:
        return pd.DataFrame()

    # Clean and filter zip_codes to only integers
    int_zips = []
    for z in zip_codes:
        try:
            val = int(float(str(z).strip()))
            int_zips.append(val)
        except (ValueError, TypeError):
            continue

    if not int_zips:
        return pd.DataFrame()

    # De-duplicate to minimize params size
    int_zips = list(set(int_zips))
    placeholders = ', '.join(['%s'] * len(int_zips))
    query = f"SELECT zip_code, state FROM accelerator.Zip_Geography_Master WHERE zip_code IN ({placeholders})"
    
    try:
        logger.info("Fetching Zip_Geography_Master batch from database...")
        results = await asyncio.to_thread(db_connector.fetch_all, query, tuple(int_zips))
        if not results:
            logger.warning("No matching Zip_Geography_Master records found.")
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        df.columns = [str(c).strip().lower() for c in df.columns]
        return df
    except Exception as e:
        logger.error("Failed to fetch Zip_Geography_Master from database: %s", e)
        # Fallback to query without schema prefix
        try:
            query_fallback = f"SELECT zip_code, state FROM Zip_Geography_Master WHERE zip_code IN ({placeholders})"
            results = await asyncio.to_thread(db_connector.fetch_all, query_fallback, tuple(int_zips))
            if results:
                df = pd.DataFrame(results)
                df.columns = [str(c).strip().lower() for c in df.columns]
                return df
        except Exception as e2:
            logger.error("Fallback Zip_Geography_Master query also failed: %s", e2)
        return pd.DataFrame()

async def _insert_leakage_data(records: List[dict]) -> int:
    """Helper to insert multiple leakage records into MySQL efficiently."""
    if not records:
        return 0
    
    query = """
        INSERT INTO Leakage_Activity (
            BrandName, Pharmacy_ID, zip_code, State, Prescriber_NPI, 
            HCP_Name, Pharmacy_Rejections, Total_Brand_Rejections, 
            Leakage_Percentage, productid, generic_name, manufacturer, 
            Processed_TimeStamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    data = []
    for r in records:
        # Robustly handle productid (must be int and not null in DB)
        raw_pid = r.get('productid')
        try:
            pid = int(raw_pid) if raw_pid is not None else 0
        except (ValueError, TypeError):
            pid = 0
            
        # Ensure Processed_TimeStamp is a string or datetime MySQL understands
        ts = r.get('Processed_TimeStamp')
        if isinstance(ts, datetime):
            ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(ts, str):
            # Try to normalize string format if it has T or Z
            ts_str = ts.replace('T', ' ').replace('Z', '').split('.')[0]
        else:
            ts_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        data.append((
            str(r.get('BrandName', 'Unknown')),
            str(r.get('Pharmacy_ID', 'Unknown')),
            str(r.get('zip_code')) if r.get('zip_code') else None,
            str(r.get('State')) if r.get('State') else None,
            str(r.get('Prescriber_NPI')) if r.get('Prescriber_NPI') else None,
            str(r.get('HCP_Name')) if r.get('HCP_Name') else None,
            int(r.get('Pharmacy_Rejections', 0)),
            int(r.get('Total_Brand_Rejections', 0)),
            float(r.get('Leakage_Percentage', 0.0)),
            pid,
            r.get('generic_name'),
            r.get('manufacturer'),
            ts_str
        ))
    
    return await asyncio.to_thread(db_connector.insert_many, query, data)

@app.post("/insert-leakage-activity")
async def insert_leakage_activity(records: List[LeakageActivitySchema]):
    """
    Receives JSON leakage records and inserts them into the MySQL table.
    """
    if not records:
        raise HTTPException(status_code=400, detail="No records provided.")
    
    # Convert Pydantic models to dicts
    records_dict = [r.model_dump() for r in records]
    
    row_count = await _insert_leakage_data(records_dict)
    
    if row_count == -1:
        raise HTTPException(status_code=500, detail="Failed to insert records into database.")
    
    logger.info("Successfully inserted %d records into Leakage_Activity.", row_count)
    return {"message": f"Successfully inserted {row_count} records.", "count": row_count}

@app.post("/process-and-merge-csv")
async def handle_csv_merge(files: List[UploadFile] = File(...)):
    """
    Receives one or more CSV/Excel files, merges them, validates rows,
    removes duplicates, enriches with SQL data, and calculates leakage.
    """
    if not files:
        logger.warning("Request received with no files.")
        raise HTTPException(status_code=400, detail="No files uploaded.")

    # Generate a unique request ID to prevent file collisions
    request_id = str(uuid.uuid4())
    logger.info("[%s] Processing %d uploaded files.", request_id, len(files))
    
    file_names_str = ", ".join(f.filename for f in files)
    dataframes = []
    final_df = None
    leakage_results = None

    try:
        for file in files:
            fname = file.filename.lower()
            if not (fname.endswith('.csv') or fname.endswith(('.xlsx', '.xls'))):
                logger.warning("[%s] Skipping unsupported file: %s", request_id, file.filename)
                continue
            df = await _load_and_clean_file(file)
            dataframes.append(_normalize_df_columns(df))

        if not dataframes:
            raise HTTPException(status_code=400, detail="No valid CSV or Excel files processed.")

        # 1. Merge all uploaded files
        final_df = await _merge_dataframes(dataframes)
        dataframes = [] # Clear list to free memory
        gc.collect()

        # Clean NaN/Inf for validation
        final_df = _clean_for_json(final_df)
        total_record_count = len(final_df)

        # 2. Row-level Validation
        final_df, invalid_records = _validate_and_filter_data(final_df)
        invalid_count = len(invalid_records)
        _log_invalid_to_csv(invalid_records, request_id)

        if final_df.empty:
            logger.warning("[%s] No valid records found after validation.", request_id)
            return {
                "FileDetails": {
                    "FileName": file_names_str,
                    "TotalRecordCount": total_record_count,
                    "SuccessCount": 0,
                    "InvalidRecordCount": invalid_count,
                    "DuplicateRecordCount": 0
                },
                "LeakageDetails": []
            }

        # 3. Deduplication
        before_dedup = len(final_df)
        final_df = _remove_duplicates(final_df)
        final_df = _clean_for_json(final_df)
        after_dedup = len(final_df)
        duplicate_count = before_dedup - after_dedup
        success_count = after_dedup

        # 4. Enrich with SQL Data (Targeted Query)
        unique_pids = final_df['productid'].dropna().unique().tolist() if 'productid' in final_df.columns else []
        
        # We need the standardized NDC for querying and merging
        if 'ndc_code' in final_df.columns:
            final_df['_temp_ndc'] = final_df['ndc_code'].apply(standardize_ndc)
            unique_ndcs = final_df['_temp_ndc'].dropna().unique().tolist()
        else:
            unique_ndcs = []
            
        unique_npis = final_df['prescriber_npi'].astype(str).str.strip().dropna().unique().tolist() if 'prescriber_npi' in final_df.columns else []

        product_master_df = await _fetch_product_master(unique_pids, unique_ndcs)
        hcp_master_df = await _fetch_hcp_master(unique_npis)
        
        if not product_master_df.empty:
            logger.info("[%s] Enriching with Product_Master SQL data.", request_id)
            pm_copy = product_master_df.copy()
            if 'ndc_code' in pm_copy.columns:
                pm_copy['_temp_ndc'] = pm_copy['ndc_code'].apply(standardize_ndc)
            else:
                pm_copy['_temp_ndc'] = None

            # Prepare columns to merge from Product_Master.
            # Match on _temp_ndc first (as requested by user matching NDC_Code).
            rename_db_cols = {
                'brandname': 'db_brandname',
                'generic_name': 'db_generic_name',
                'manufacturer': 'db_manufacturer',
                'productid': 'db_productid'
            }
            rename_db_cols = {k: v for k, v in rename_db_cols.items() if k in pm_copy.columns}
            pm_copy_ndc = pm_copy.rename(columns=rename_db_cols)
            
            # Select relevant columns for NDC merge
            ndc_merge_cols = ['_temp_ndc'] + list(rename_db_cols.values())
            pm_copy_ndc = pm_copy_ndc[ndc_merge_cols].dropna(subset=['_temp_ndc']).drop_duplicates('_temp_ndc')

            # Merge on _temp_ndc
            final_df = pd.merge(final_df, pm_copy_ndc, on='_temp_ndc', how='left')
            final_df = _clean_for_json(final_df)

            # Resolve the merged columns from the NDC-based merge
            target_cols = ['brandname', 'generic_name', 'manufacturer', 'productid']
            for col in target_cols:
                db_col = f"db_{col}"
                if db_col in final_df.columns:
                    if col not in final_df.columns:
                        final_df[col] = final_df[db_col]
                    else:
                        # Overwrite col with db_col if db_col is not null
                        final_df[col] = final_df[db_col].combine_first(final_df[col])
                    # Drop the db_col
                    final_df = final_df.drop(columns=[db_col])

            # In case some records didn't match via NDC but could match via productid,
            # we perform a fallback merge on productid:
            if 'productid' in final_df.columns and 'productid' in pm_copy.columns:
                # Rename all columns except productid to avoid KeyError on productid
                rename_pid_cols = {k: v for k, v in rename_db_cols.items() if k != 'productid'}
                pm_copy_pid = pm_copy.rename(columns=rename_pid_cols)
                pid_merge_cols = ['productid'] + list(rename_pid_cols.values())
                pm_copy_pid = pm_copy_pid[pid_merge_cols].dropna(subset=['productid']).drop_duplicates('productid')

                # Left merge on productid
                final_df = pd.merge(final_df, pm_copy_pid, on='productid', how='left')
                final_df = _clean_for_json(final_df)

                for col in target_cols:
                    if col == 'productid':
                        continue
                    db_col = f"db_{col}"
                    if db_col in final_df.columns:
                        if col not in final_df.columns:
                            final_df[col] = final_df[db_col]
                        else:
                            # Fill missing values only (productid is fallback here)
                            final_df[col] = final_df[col].combine_first(final_df[db_col])
                        final_df = final_df.drop(columns=[db_col])
                
        # Clean up temporary ndc column
        if '_temp_ndc' in final_df.columns:
            final_df = final_df.drop(columns=['_temp_ndc'])
        
        # HCP Enrichment
        if not hcp_master_df.empty and 'prescriber_npi' in final_df.columns:
            logger.info("[%s] Enriching with HCP_Master SQL data (prescriber_npi).", request_id)
            final_df['prescriber_npi'] = final_df['prescriber_npi'].astype(str).str.strip()
            final_df = pd.merge(final_df, hcp_master_df, on='prescriber_npi', how='left')
            final_df = _clean_for_json(final_df)
            
            # Consolidate duplicate columns after merge
            for col in ['zip_code', 'state']:
                col_x, col_y = f"{col}_x", f"{col}_y"
                if col_x in final_df.columns and col_y in final_df.columns:
                    final_df[col] = final_df[col_x].fillna(final_df[col_y])
                    final_df = final_df.drop(columns=[col_x, col_y])
                elif col_y in final_df.columns and col not in final_df.columns:
                    final_df = final_df.rename(columns={col_y: col})
            
            if 'first_name' in final_df.columns and 'last_name' in final_df.columns:
                logger.info("[%s] Creating 'full name' from HCP data.", request_id)
                final_df['full name'] = (
                    final_df['first_name'].fillna('').astype(str) + ' ' + 
                    final_df['last_name'].fillna('').astype(str)
                ).str.strip()
                final_df.loc[final_df['full name'] == '', 'full name'] = None

        # Process HCP names from CSV if available (takes priority)
        csv_has_names = 'hcp_first_name' in final_df.columns and 'hcp_last_name' in final_df.columns
        if csv_has_names:
            logger.info("[%s] Using HCP_First_Name and HCP_Last_Name from CSV file to generate HCP_Name.", request_id)
            csv_full_name = (
                final_df['hcp_first_name'].fillna('').astype(str) + ' ' + 
                final_df['hcp_last_name'].fillna('').astype(str)
            ).str.strip()
            
            if 'full name' in final_df.columns:
                # Prioritize CSV name, fallback to database name
                final_df['full name'] = csv_full_name.where(csv_full_name != '', final_df['full name'])
            else:
                final_df['full name'] = csv_full_name
                
            final_df.loc[final_df['full name'] == '', 'full name'] = None

        # Ensure HCP_Name is populated in final_df for the response/records
        if 'full name' in final_df.columns:
            final_df['HCP_Name'] = final_df['full name']

        # Zip Geography Enrichment
        if 'zip_code' in final_df.columns:
            # Standardize ZIP code to integer for matching
            def to_int_zip(z):
                try:
                    return int(float(str(z).strip()))
                except (ValueError, TypeError):
                    return None

            final_df['_temp_int_zip'] = final_df['zip_code'].apply(to_int_zip)
            unique_zips = final_df['_temp_int_zip'].dropna().unique().tolist()
            
            if unique_zips:
                logger.info("[%s] Enriching with Zip_Geography_Master data.", request_id)
                zip_geo_df = await _fetch_zip_geography(unique_zips)
                
                if not zip_geo_df.empty:
                    zip_geo_df = zip_geo_df.rename(columns={'zip_code': '_temp_int_zip'})
                    # Left merge on _temp_int_zip to retain all records
                    final_df = pd.merge(final_df, zip_geo_df, on='_temp_int_zip', how='left', suffixes=('', '_geo'))
                    
                    if 'state_geo' in final_df.columns:
                        if 'state' in final_df.columns:
                            # Prioritize geo-matched state, fallback to existing state, fallback to 'Unknown'
                            final_df['state'] = final_df['state_geo'].fillna(final_df['state']).fillna('Unknown')
                            final_df = final_df.drop(columns=['state_geo'])
                        else:
                            final_df['state'] = final_df['state_geo'].fillna('Unknown')
                            final_df = final_df.drop(columns=['state_geo'])
                    else:
                        if 'state' not in final_df.columns:
                            final_df['state'] = 'Unknown'
                        else:
                            final_df['state'] = final_df['state'].fillna('Unknown')
                else:
                    if 'state' not in final_df.columns:
                        final_df['state'] = 'Unknown'
                    else:
                        final_df['state'] = final_df['state'].fillna('Unknown')
            else:
                if 'state' not in final_df.columns:
                    final_df['state'] = 'Unknown'
                else:
                    final_df['state'] = final_df['state'].fillna('Unknown')
            
            # Clean up temporary integer zip column
            if '_temp_int_zip' in final_df.columns:
                final_df = final_df.drop(columns=['_temp_int_zip'])
        else:
            if 'state' not in final_df.columns:
                final_df['state'] = 'Unknown'
            else:
                final_df['state'] = final_df['state'].fillna('Unknown')
        
        # 5. Leakage Calculation
        leakage_results = _calculate_leakage(final_df)
        
        response_data = {
            "FileDetails": {
                "FileName": file_names_str,
                "TotalRecordCount": total_record_count,
                "SuccessCount": success_count,
                "InvalidRecordCount": invalid_count,
                "DuplicateRecordCount": duplicate_count
            },
            "LeakageDetails": leakage_results or []
        }

        if leakage_results:
            logger.info("[%s] Leakage calculation complete. Generated %d records.", request_id, len(leakage_results))
            
            # AUTOMATIC INSERTION INTO MYSQL
            inserted_count = await _insert_leakage_data(leakage_results)
            if inserted_count == -1:
                logger.error("[%s] Failed to insert leakage records into MySQL.", request_id)
            else:
                logger.info("[%s] Inserted %d leakage records into MySQL.", request_id, inserted_count)

            # Save unique report
            try:
                report_filename = f"leakage_report_{request_id}.json"
                leakage_file = os.path.join(config.RESPONSE_DIR, report_filename)
                os.makedirs(config.RESPONSE_DIR, exist_ok=True)
                with open(leakage_file, "w", encoding="utf-8") as f:
                    json.dump(response_data, f, indent=4)
            except OSError as e:
                logger.warning("[%s] Could not write leakage report to file: %s", request_id, e)
            
            return response_data

        # Final cleaning for JSON serialization
        final_df = _clean_for_json(final_df)
        
        row_count = len(final_df)
        logger.info("[%s] Returning records (%d rows).", request_id, row_count)

        if row_count > config.ROW_COUNT_THRESHOLD:
            logger.info("[%s] Using StreamingResponse.", request_id)
            
            file_details_dict = {
                "FileName": file_names_str,
                "TotalRecordCount": total_record_count,
                "SuccessCount": success_count,
                "InvalidRecordCount": invalid_count,
                "DuplicateRecordCount": duplicate_count
            }

            # Create a local reference for the generator
            # itertuples() is significantly faster than iterrows() for streaming
            def data_generator(df_stream, file_details):
                yield '{"FileDetails": ' + json.dumps(file_details) + ', "LeakageDetails": ['
                first = True
                for row in df_stream.itertuples(index=False):
                    if not first: yield ","
                    yield json.dumps(row._asdict())
                    first = False
                yield ']}'
            
            # Keep df alive for streaming, then clean up in background if needed
            resp = StreamingResponse(data_generator(final_df, file_details_dict), media_type="application/json")
            final_df = None # Prevent finally block from deleting it prematurely
            return resp

        # Fallback for smaller files
        raw_records = _get_validated_records(final_df)
        response_data["LeakageDetails"] = raw_records
        try:
            resp_filename = f"response_{request_id}.json"
            resp_path = os.path.join(config.RESPONSE_DIR, resp_filename)
            os.makedirs(config.RESPONSE_DIR, exist_ok=True)
            with open(resp_path, "w", encoding="utf-8") as f:
                json.dump(response_data, f, indent=4)
            logger.info("[%s] Results saved to %s", request_id, resp_path)
        except OSError as e:
            logger.warning("[%s] Could not write response to file: %s", request_id, e)

        return response_data

    except HTTPException as he:
        logger.error("[%s] HTTPException: %s", request_id, he.detail)
        raise he
    except Exception as e:
        logger.exception("[%s] Unexpected error during processing.", request_id)
        raise HTTPException(status_code=500, detail="Internal server error occurred.") from e
    finally:
        # AGGRESSIVE MEMORY CLEANUP
        logger.info("[%s] Performing memory cleanup.", request_id)
        del dataframes
        if final_df is not None:
            del final_df
        if leakage_results is not None:
            del leakage_results
        gc.collect() # Force garbage collection

async def process_csv_bytes(file_bytes: bytes, request_id: str = None, filename: str = None) -> dict:
    """Core logic extracted for the automated scheduler to use."""
    import io
    if not request_id:
        request_id = str(uuid.uuid4())
    logger.info("[%s] Automated processing of S3 CSV bytes started.", request_id)
    
    csv_text = file_bytes.decode('utf-8-sig')
    df = await asyncio.to_thread(pd.read_csv, io.StringIO(csv_text))
    df = _normalize_df_columns(df)
    total_record_count = len(df)
    
    # 2. Row-level Validation
    final_df, invalid_records = _validate_and_filter_data(df)
    invalid_count = len(invalid_records)
    _log_invalid_to_csv(invalid_records, request_id)
    
    if final_df.empty:
        logger.warning("[%s] No valid records found after validation.", request_id)
        return {
            "FileDetails": {
                "FileName": filename or "S3_File.csv",
                "TotalRecordCount": total_record_count,
                "SuccessCount": 0,
                "InvalidRecordCount": invalid_count,
                "DuplicateRecordCount": 0
            },
            "LeakageDetails": []
        }

    # 3. Deduplication
    before_dedup = len(final_df)
    final_df = _remove_duplicates(final_df)
    final_df = _clean_for_json(final_df)
    after_dedup = len(final_df)
    duplicate_count = before_dedup - after_dedup
    success_count = after_dedup
    
    # 4. Enrich with SQL Data (Targeted Query)
    unique_pids = final_df['productid'].dropna().unique().tolist() if 'productid' in final_df.columns else []
    
    if 'ndc_code' in final_df.columns:
        final_df['_temp_ndc'] = final_df['ndc_code'].apply(standardize_ndc)
        unique_ndcs = final_df['_temp_ndc'].dropna().unique().tolist()
    else:
        unique_ndcs = []
        
    unique_npis = final_df['prescriber_npi'].astype(str).str.strip().dropna().unique().tolist() if 'prescriber_npi' in final_df.columns else []

    product_master_df = await _fetch_product_master(unique_pids, unique_ndcs)
    hcp_master_df = await _fetch_hcp_master(unique_npis)
    
    if not product_master_df.empty:
        logger.info("[%s] Enriching with Product_Master SQL data.", request_id)
        pm_copy = product_master_df.copy()
        if 'ndc_code' in pm_copy.columns:
            pm_copy['_temp_ndc'] = pm_copy['ndc_code'].apply(standardize_ndc)
        else:
            pm_copy['_temp_ndc'] = None

        rename_db_cols = {
            'brandname': 'db_brandname',
            'generic_name': 'db_generic_name',
            'manufacturer': 'db_manufacturer',
            'productid': 'db_productid'
        }
        rename_db_cols = {k: v for k, v in rename_db_cols.items() if k in pm_copy.columns}
        pm_copy_ndc = pm_copy.rename(columns=rename_db_cols)
        
        ndc_merge_cols = ['_temp_ndc'] + list(rename_db_cols.values())
        pm_copy_ndc = pm_copy_ndc[ndc_merge_cols].dropna(subset=['_temp_ndc']).drop_duplicates('_temp_ndc')

        final_df = pd.merge(final_df, pm_copy_ndc, on='_temp_ndc', how='left')
        final_df = _clean_for_json(final_df)

        target_cols = ['brandname', 'generic_name', 'manufacturer', 'productid']
        for col in target_cols:
            db_col = f"db_{col}"
            if db_col in final_df.columns:
                if col not in final_df.columns:
                    final_df[col] = final_df[db_col]
                else:
                    final_df[col] = final_df[db_col].combine_first(final_df[col])
                final_df = final_df.drop(columns=[db_col])

        if 'productid' in final_df.columns and 'productid' in pm_copy.columns:
            rename_pid_cols = {k: v for k, v in rename_db_cols.items() if k != 'productid'}
            pm_copy_pid = pm_copy.rename(columns=rename_pid_cols)
            pid_merge_cols = ['productid'] + list(rename_pid_cols.values())
            pm_copy_pid = pm_copy_pid[pid_merge_cols].dropna(subset=['productid']).drop_duplicates('productid')

            final_df = pd.merge(final_df, pm_copy_pid, on='productid', how='left')
            final_df = _clean_for_json(final_df)

            for col in target_cols:
                if col == 'productid':
                    continue
                db_col = f"db_{col}"
                if db_col in final_df.columns:
                    if col not in final_df.columns:
                        final_df[col] = final_df[db_col]
                    else:
                        final_df[col] = final_df[col].combine_first(final_df[db_col])
                    final_df = final_df.drop(columns=[db_col])
            
    if '_temp_ndc' in final_df.columns:
        final_df = final_df.drop(columns=['_temp_ndc'])
    
    # HCP Enrichment
    if not hcp_master_df.empty and 'prescriber_npi' in final_df.columns:
        logger.info("[%s] Enriching with HCP_Master SQL data (prescriber_npi).", request_id)
        final_df['prescriber_npi'] = final_df['prescriber_npi'].astype(str).str.strip()
        final_df = pd.merge(final_df, hcp_master_df, on='prescriber_npi', how='left')
        final_df = _clean_for_json(final_df)
        
        for col in ['zip_code', 'state']:
            col_x, col_y = f"{col}_x", f"{col}_y"
            if col_x in final_df.columns and col_y in final_df.columns:
                final_df[col] = final_df[col_x].fillna(final_df[col_y])
                final_df = final_df.drop(columns=[col_x, col_y])
            elif col_y in final_df.columns and col not in final_df.columns:
                final_df = final_df.rename(columns={col_y: col})
        
        if 'first_name' in final_df.columns and 'last_name' in final_df.columns:
            final_df['full name'] = (
                final_df['first_name'].fillna('').astype(str) + ' ' + 
                final_df['last_name'].fillna('').astype(str)
            ).str.strip()
            final_df.loc[final_df['full name'] == '', 'full name'] = None

    if 'full name' in final_df.columns:
        final_df['HCP_Name'] = final_df['full name']

    # Zip Geography Enrichment
    if 'zip_code' in final_df.columns:
        def to_int_zip(z):
            try:
                return int(float(str(z).strip()))
            except (ValueError, TypeError):
                return None

        final_df['_temp_int_zip'] = final_df['zip_code'].apply(to_int_zip)
        unique_zips = final_df['_temp_int_zip'].dropna().unique().tolist()
        
        if unique_zips:
            zip_geo_df = await _fetch_zip_geography(unique_zips)
            if not zip_geo_df.empty:
                zip_geo_df = zip_geo_df.rename(columns={'zip_code': '_temp_int_zip'})
                final_df = pd.merge(final_df, zip_geo_df, on='_temp_int_zip', how='left', suffixes=('', '_geo'))
                
                if 'state_geo' in final_df.columns:
                    if 'state' in final_df.columns:
                        final_df['state'] = final_df['state_geo'].fillna(final_df['state']).fillna('Unknown')
                        final_df = final_df.drop(columns=['state_geo'])
                    else:
                        final_df['state'] = final_df['state_geo'].fillna('Unknown')
                        final_df = final_df.drop(columns=['state_geo'])
                else:
                    if 'state' not in final_df.columns:
                        final_df['state'] = 'Unknown'
                    else:
                        final_df['state'] = final_df['state'].fillna('Unknown')
            else:
                if 'state' not in final_df.columns:
                    final_df['state'] = 'Unknown'
        if '_temp_int_zip' in final_df.columns:
            final_df = final_df.drop(columns=['_temp_int_zip'])

    # 5. Leakage Calculation
    leakage_results = _calculate_leakage(final_df)
    if leakage_results:
        # AUTOMATIC INSERTION INTO MYSQL
        inserted_count = await _insert_leakage_data(leakage_results)
        logger.info("[%s] Inserted %d leakage records into MySQL.", request_id, inserted_count)
        
    return {
        "FileDetails": {
            "FileName": filename or "S3_File.csv",
            "TotalRecordCount": total_record_count,
            "SuccessCount": success_count,
            "InvalidRecordCount": invalid_count,
            "DuplicateRecordCount": duplicate_count
        },
        "LeakageDetails": leakage_results or []
    }

@app.get("/auto-process-status")
async def get_auto_process_status():
    """
    Triggers AWS S3 file checking/processing and returns the current status.
    """
    from scheduler import check_s3_sync
    status_dict = await check_s3_sync()
    return status_dict

class LeakageUpdatePayload(BaseModel):
    newValue: int
    updatedBy: Optional[str] = "System"

@app.post("/leakage-percent")
def update_leakage_percent(payload: LeakageUpdatePayload):
    """
    Updates the active leakage threshold and logs it in the history table.
    """
    try:
        # Get current active value (if table is empty, default to 15)
        current_query = "SELECT new_value FROM leakage_history ORDER BY id DESC LIMIT 1"
        current_result = db_connector.fetch_all(current_query)
        prev_val = current_result[0]["new_value"] if current_result else getattr(config, 'LEAKAGE_THRESHOLD', 15)
        
        # Insert new entry
        insert_query = """
            INSERT INTO leakage_history (previous_value, new_value, updated_by)
            VALUES (%s, %s, %s)
        """
        res = db_connector.execute_query(insert_query, (prev_val, payload.newValue, payload.updatedBy))
        if res == -1:
            raise HTTPException(status_code=500, detail="Failed to insert leakage history into database.")
        
        return {
            "message": "Leakage percentage updated successfully",
            "previousValue": prev_val,
            "newValue": payload.newValue
        }
    except Exception as e:
        logger.error(f"Error updating leakage percent: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/leakage-activity")
def get_leakage_activity(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """
    Retrieves records from Leakage_Activity database table, optionally filtered by date.
    """
    try:
        query = "SELECT * FROM Leakage_Activity"
        params = []
        conditions = []
        
        # If dates are supplied, we can filter by Processed_TimeStamp
        if start_date:
            conditions.append("Processed_TimeStamp >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("Processed_TimeStamp <= %s")
            params.append(end_date)
            
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        query += " ORDER BY Processed_TimeStamp DESC"
        
        results = db_connector.fetch_all(query, tuple(params) if params else None)
        if results is None:
            return []
            
        # Clean results for JSON compliance (handling decimal/datetime conversion)
        cleaned_results = []
        for row in results:
            dt = row.get("Processed_TimeStamp")
            if isinstance(dt, datetime):
                dt_str = dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                dt_str = str(dt) if dt else None
                
            cleaned_results.append({
                "id": row.get("id"),
                "brandName": row.get("BrandName"),
                "pharmacyId": row.get("Pharmacy_ID"),
                "zipCode": row.get("zip_code"),
                "prescriberNpi": row.get("Prescriber_NPI"),
                "hcpName": row.get("HCP_Name"),
                "state": row.get("State"),
                "pharmacyRejections": row.get("Pharmacy_Rejections"),
                "totalBrandRejections": row.get("Total_Brand_Rejections"),
                "leakagePercentage": float(row.get("Leakage_Percentage")) if row.get("Leakage_Percentage") is not None else 0.0,
                "genericName": row.get("generic_name"),
                "manufacturer": row.get("manufacturer"),
                "productId": row.get("productid"),
                "date": dt_str
            })
        return cleaned_results
    except Exception as e:
        logger.error(f"Error fetching leakage activity: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

@app.get("/leakage-percent/history")
def get_leakage_history():
    """
    Retrieves the history of leakage percentage modifications.
    """
    try:
        query = "SELECT previous_value, new_value, updated_by, updated_at FROM leakage_history ORDER BY id DESC"
        result = db_connector.fetch_all(query)
        formatted_results = []
        if result:
            for row in result:
                dt = row["updated_at"]
                if isinstance(dt, datetime):
                    dt_str = dt.strftime('%d/%m/%Y %I:%M %p')
                else:
                    dt_str = str(dt)
                
                formatted_results.append({
                    "prevValue": row["previous_value"],
                    "newValue": row["new_value"],
                    "updatedBy": row["updated_by"],
                    "lastModified": dt_str
                })
        return formatted_results
    except Exception as e:
        logger.error(f"Error fetching leakage history: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

@app.get("/config")
def get_app_config():
    """
    Returns app configuration parameters (e.g. leakage threshold) to the frontend.
    """
    try:
        query = "SELECT new_value FROM leakage_history ORDER BY id DESC LIMIT 1"
        result = db_connector.fetch_all(query)
        if result:
            return {"LEAKAGE_THRESHOLD": result[0]["new_value"]}
    except Exception as e:
        logger.error(f"Error fetching current leakage threshold from DB: {e}")
    
    return {"LEAKAGE_THRESHOLD": getattr(config, 'LEAKAGE_THRESHOLD', 15)}

@app.get("/hcp-stats")
def get_hcp_stats():
    """
    Retrieves aggregated active HCP engagement statistics from the database,
    including the Fatigue Index calculation.
    """
    try:
        # Query unique HCP count
        hcp_query = "SELECT COUNT(DISTINCT HCP_ID) as hcp_count FROM hcp_engagement_logs"
        hcp_result = db_connector.fetch_all(hcp_query)
        hcp_count = hcp_result[0]["hcp_count"] if hcp_result else 0
        
        # Query total engagement sums
        sums_query = """
            SELECT 
                SUM(Email_Sent) as sent, 
                SUM(Email_Opened) as opened, 
                SUM(Email_Clicked) as clicked, 
                SUM(Webinar_Minutes) as minutes 
            FROM hcp_engagement_logs
        """
        sums_result = db_connector.fetch_all(sums_query)
        
        sent = 0
        opened = 0
        clicked = 0
        minutes = 0
        if sums_result and sums_result[0]:
            r = sums_result[0]
            sent = int(r["sent"]) if r["sent"] is not None else 0
            opened = int(r["opened"]) if r["opened"] is not None else 0
            clicked = int(r["clicked"]) if r["clicked"] is not None else 0
            minutes = int(r["minutes"]) if r["minutes"] is not None else 0

        # Calculate Average Fatigue Index
        # Formula from plan.txt:
        # 1. Unclicked Touch Score (40%): (MIN(Unclicked_Touches_7D, 5) / 5) * 40
        # 2. Open Rate Score (25%): (1 - (Email_Open_Count / Email_Sent_Count)) * 25
        # 3. Click Rate Score (25%): (1 - (Email_Click_Count / Email_Sent_Count)) * 25
        # 4. Recency Score (10%): <7d:0, 7-14d:5, >14d:10
        
        fatigue_query = """
            SELECT AVG(fatigue_score) as avg_fatigue
            FROM (
                SELECT 
                    HCP_ID,
                    (
                        LEAST(SUM(CASE WHEN Email_Sent > 0 AND Email_Clicked = 0 AND STR_TO_DATE(Engagement_Date, '%c/%e/%Y') >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) THEN 1 ELSE 0 END), 5) / 5 * 40
                        +
                        (1 - (SUM(Email_Opened) / NULLIF(SUM(Email_Sent), 0))) * 25
                        +
                        (1 - (SUM(Email_Clicked) / NULLIF(SUM(Email_Sent), 0))) * 25
                        +
                        CASE 
                            WHEN DATEDIFF(CURDATE(), MAX(STR_TO_DATE(Engagement_Date, '%c/%e/%Y'))) > 14 THEN 10
                            WHEN DATEDIFF(CURDATE(), MAX(STR_TO_DATE(Engagement_Date, '%c/%e/%Y'))) >= 7 THEN 5
                            ELSE 0
                        END
                    ) as fatigue_score
                FROM hcp_engagement_logs
                GROUP BY HCP_ID
            ) as t
        """
        fatigue_result = db_connector.fetch_all(fatigue_query)
        avg_fatigue = float(fatigue_result[0]["avg_fatigue"]) if fatigue_result and fatigue_result[0]["avg_fatigue"] is not None else 0.0

        # Query channel breakdown
        channel_query = "SELECT Channel, COUNT(*) as cnt FROM hcp_engagement_logs GROUP BY Channel"
        channel_result = db_connector.fetch_all(channel_query)
        channels = {}
        if channel_result:
            for row in channel_result:
                channels[row["Channel"]] = int(row["cnt"])

        return {
            "hcp_count": hcp_count,
            "total_sent": sent,
            "total_opened": opened,
            "total_clicked": clicked,
            "webinar_minutes": minutes,
            "channels": channels,
            "avg_fatigue": round(avg_fatigue, 2)
        }
    except Exception as e:
        logger.error(f"Error fetching HCP stats: {e}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")


@app.get("/leakage-fatigue-table")
def get_leakage_fatigue_table():
    """
    Returns leakage activity rows joined with per-HCP fatigue scores.
    Reuses the same fatigue calculation logic from `/hcp-stats` but aggregated per HCP.
    """
    try:
        query = """
            SELECT
                la.zip_code AS zip_code,
                la.State AS State,
                la.BrandName AS BrandName,
                la.Leakage_Percentage AS Leakage_Percentage,
                la.HCP_Name AS HCP_Name,
                per.Fatigue_Score AS Fatigue_Score
            FROM Leakage_Activity la
            LEFT JOIN (
                SELECT HCP_ID,
                    (
                        LEAST(SUM(CASE WHEN Email_Sent > 0 AND Email_Clicked = 0 AND STR_TO_DATE(Engagement_Date, '%c/%e/%Y') >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) THEN 1 ELSE 0 END), 5) / 5 * 40
                        +
                        (1 - (SUM(Email_Opened) / NULLIF(SUM(Email_Sent), 0))) * 25
                        +
                        (1 - (SUM(Email_Clicked) / NULLIF(SUM(Email_Sent), 0))) * 25
                        +
                        CASE
                            WHEN DATEDIFF(CURDATE(), MAX(STR_TO_DATE(Engagement_Date, '%c/%e/%Y'))) > 14 THEN 10
                            WHEN DATEDIFF(CURDATE(), MAX(STR_TO_DATE(Engagement_Date, '%c/%e/%Y'))) >= 7 THEN 5
                            ELSE 0
                        END
                    ) AS Fatigue_Score
                FROM hcp_engagement_logs
                GROUP BY HCP_ID
            ) per ON la.Prescriber_NPI = per.HCP_ID
        """

        results = db_connector.fetch_all(query)
        # Return an empty list rather than None for consistency
        return results or []
    except Exception as e:
        logger.exception("Error fetching leakage-fatigue-table")
        raise HTTPException(status_code=500, detail="Database query failed.") from e

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.HOST, port=config.PORT)
