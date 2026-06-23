"""
FastAPI application for receiving and storing reverse claim records.
Enforces strict JSON schema validation and writes directly to the database.
"""
import logging
import os
import uvicorn
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ConfigDict
from dotenv import load_dotenv

# Local module imports
import config
from mysqlConnecter import db_connector

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Reverse Claim API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic Schema for strict validation
class ReverseClaimRequest(BaseModel):
    Transaction_ID: str = Field(..., alias="Transaction_ID")
    Timestamp: str = Field(..., alias="Timestamp")
    Patient_Token: str = Field(..., alias="Patient_Token")
    Prescriber_NPI: str = Field(..., alias="Prescriber_NPI")
    Pharmacy_ID: str = Field(..., alias="Pharmacy_ID")
    NDC_Code: str = Field(..., alias="NDC_Code")
    Payer_BIN: str = Field(..., alias="Payer_BIN")
    Response_Status: str = Field(..., alias="Response_Status")
    Reject_Code: str = Field(..., alias="Reject_Code")
    Reject_Message: str = Field(..., alias="Reject_Message")

    model_config = ConfigDict(
        populate_by_name=True,
        extra='forbid',  # Reject any request with extra fields
        str_strip_whitespace=True
    )

@app.on_event("startup")
def startup_event():
    logger.info("Initializing database table for Reverse Claims...")
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Reverse_Claims (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Transaction_ID VARCHAR(255) NOT NULL,
        Timestamp VARCHAR(255) NOT NULL,
        Patient_Token VARCHAR(255) NOT NULL,
        Prescriber_NPI VARCHAR(255) NOT NULL,
        Pharmacy_ID VARCHAR(255) NOT NULL,
        NDC_Code VARCHAR(255) NOT NULL,
        Payer_BIN VARCHAR(255) NOT NULL,
        Response_Status VARCHAR(50) NOT NULL,
        Reject_Code VARCHAR(255) NOT NULL,
        Reject_Message VARCHAR(255) NOT NULL,
        Created_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    result = db_connector.execute_query(create_table_query)
    if result == -1:
        logger.error("Failed to initialize Reverse_Claims table in database.")
    else:
        logger.info("Reverse_Claims table initialized successfully.")

@app.get("/copay-count")
def get_copay_count():
    """
    Returns the total count of records in the Reverse_Claims table.
    """
    try:
        query = "SELECT COUNT(*) as count FROM Reverse_Claims"
        results = db_connector.fetch_all(query)
        if not results:
            return {"count": 0}
        return {"count": results[0]["count"]}
    except Exception as e:
        logger.exception("Database error while fetching copay count.")
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

@app.post("/reverse-claim", status_code=status.HTTP_201_CREATED)
def handle_reverse_claim(payload: ReverseClaimRequest):
    """
    Accepts JSON payload, strictly validates it against schema,
    and inserts it into the database.
    """
    logger.info("Received reverse claim request for Transaction_ID: %s", payload.Transaction_ID)
    
    insert_query = """
    INSERT INTO Reverse_Claims (
        Transaction_ID, Timestamp, Patient_Token, Prescriber_NPI,
        Pharmacy_ID, NDC_Code, Payer_BIN, Response_Status,
        Reject_Code, Reject_Message
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        payload.Transaction_ID,
        payload.Timestamp,
        payload.Patient_Token,
        payload.Prescriber_NPI,
        payload.Pharmacy_ID,
        payload.NDC_Code,
        payload.Payer_BIN,
        payload.Response_Status,
        payload.Reject_Code,
        payload.Reject_Message
    )
    
    try:
        # Check if transaction already exists
        check_query = "SELECT COUNT(*) as count FROM Reverse_Claims WHERE Transaction_ID = %s"
        exists_result = db_connector.fetch_all(check_query, (payload.Transaction_ID,))
        transaction_exists = exists_result and exists_result[0]['count'] > 0

        if transaction_exists:
            logger.info("Transaction_ID %s already exists. Skipping insertion.", payload.Transaction_ID)
            row_count = 1
        else:
            row_count = db_connector.execute_query(insert_query, params)
            if row_count <= 0:
                logger.error("Failed to insert reverse claim into database.")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to save reverse claim record to database."
                )
            logger.info("Successfully saved reverse claim record. Rows affected: %d", row_count)
        
        # Extract the date portion from the Timestamp field
        try:
            date_part = payload.Timestamp.split('T')[0]
            
            # Lookup product details from Product_Master using NDC_Code
            prod_query = """
            SELECT brand_name, generic_name, manufacturer, product_id 
            FROM Product_Master 
            WHERE ndc_code = %s 
            LIMIT 1
            """
            prod_results = db_connector.fetch_all(prod_query, (payload.NDC_Code,))
            if not prod_results:
                logger.warning(
                    "Product details not found in Product_Master for NDC_Code: %s. Skipping Leakage_Activity update.",
                    payload.NDC_Code
                )
            else:
                brand_name = prod_results[0]['brand_name']
                generic_name = prod_results[0]['generic_name']
                manufacturer = prod_results[0]['manufacturer']
                product_id = prod_results[0]['product_id']
                
                logger.info(
                    "Product lookup success: brand_name=%s, product_id=%s. Calculating rejections...",
                    brand_name, product_id
                )
                
                # Calculate counts from both copay_Claims_Transactions and Reverse_Claims
                # 1. Total claims count from copay_Claims_Transactions
                q_claims_total = """
                SELECT COUNT(*) as count 
                FROM copay_Claims_Transactions 
                WHERE BrandName = %s 
                  AND LEFT(transaction_datetime, 10) = %s 
                  AND claim_status = 'REJECTED'
                """
                claims_total_res = db_connector.fetch_all(q_claims_total, (brand_name, date_part))
                claims_total = claims_total_res[0]['count'] if claims_total_res else 0
                
                # 2. Total claims count from Reverse_Claims
                q_reverse_total = """
                SELECT COUNT(*) as count 
                FROM Reverse_Claims rc 
                JOIN Product_Master pm ON rc.NDC_Code = pm.ndc_code 
                WHERE pm.brand_name = %s 
                  AND LEFT(rc.Timestamp, 10) = %s
                """
                reverse_total_res = db_connector.fetch_all(q_reverse_total, (brand_name, date_part))
                reverse_total = reverse_total_res[0]['count'] if reverse_total_res else 0
                
                total_count = claims_total + reverse_total
                
                logger.info(
                    "Total Brand Rejections calculated: %d (Claims: %d, Reverse: %d) for brand: %s, date: %s",
                    total_count, claims_total, reverse_total, brand_name, date_part
                )
                
                # Find all matching records in Leakage_Activity
                select_query = """
                SELECT id, Pharmacy_ID 
                FROM Leakage_Activity 
                WHERE DATE(Processed_TimeStamp) = %s 
                  AND BrandName = %s
                """
                records = db_connector.fetch_all(select_query, (date_part, brand_name))
                logger.info("Found %d matching records in Leakage_Activity to recalculate and update.", len(records) if records else 0)
                
                if records:
                    for r in records:
                        rec_id = r['id']
                        pharm_id = r['Pharmacy_ID']
                        
                        # Calculate Pharmacy_Rejections from copay_Claims_Transactions
                        q_claims_pharm = """
                        SELECT COUNT(*) as count 
                        FROM copay_Claims_Transactions 
                        WHERE BrandName = %s 
                          AND LEFT(transaction_datetime, 10) = %s 
                          AND Pharmacy_ID = %s 
                          AND claim_status = 'REJECTED'
                        """
                        claims_pharm_res = db_connector.fetch_all(q_claims_pharm, (brand_name, date_part, pharm_id))
                        claims_pharm = claims_pharm_res[0]['count'] if claims_pharm_res else 0
                        
                        # Calculate Pharmacy_Rejections from Reverse_Claims
                        q_reverse_pharm = """
                        SELECT COUNT(*) as count 
                        FROM Reverse_Claims rc 
                        JOIN Product_Master pm ON rc.NDC_Code = pm.ndc_code 
                        WHERE pm.brand_name = %s 
                          AND LEFT(rc.Timestamp, 10) = %s 
                          AND rc.Pharmacy_ID = %s
                        """
                        reverse_pharm_res = db_connector.fetch_all(q_reverse_pharm, (brand_name, date_part, pharm_id))
                        reverse_pharm = reverse_pharm_res[0]['count'] if reverse_pharm_res else 0
                        
                        pharm_count = claims_pharm + reverse_pharm
                        
                        # Recalculate Leakage_Percentage
                        leakage_pct = int(round((pharm_count / total_count) * 100)) if total_count > 0 else 0
                        
                        # Update record by ID
                        update_query = """
                        UPDATE Leakage_Activity 
                        SET BrandName = %s,
                            Pharmacy_ID = %s,
                            generic_name = %s,
                            manufacturer = %s,
                            productid = %s,
                            Pharmacy_Rejections = %s,
                            Total_Brand_Rejections = %s,
                            Leakage_Percentage = %s
                        WHERE id = %s
                        """
                        update_params = (
                            brand_name,
                            pharm_id,
                            generic_name,
                            manufacturer,
                            str(product_id),
                            pharm_count,
                            total_count,
                            leakage_pct,
                            rec_id
                        )
                        rows_updated = db_connector.execute_query(update_query, update_params)
                        logger.info(
                            "Leakage_Activity record ID %d (Pharmacy_ID=%s) updated. Rows affected: %d, Pharmacy_Rejections: %d, Total_Brand_Rejections: %d, Leakage_Percentage: %d%%",
                            rec_id, pharm_id, rows_updated, pharm_count, total_count, leakage_pct
                        )
        except Exception as update_err:
            logger.error("Error occurred while updating Leakage_Activity: %s", str(update_err))

        return {
            "status": "success",
            "message": "Reverse claim record saved successfully.",
            "Transaction_ID": payload.Transaction_ID
        }
    except Exception as e:
        logger.exception("Database error while inserting reverse claim record.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database insertion failed: {str(e)}"
        )

if __name__ == "__main__":
    port = int(os.environ.get("REVERSE_CLAIM_PORT", 8001))
    uvicorn.run("reverse_claim_api:app", host="0.0.0.0", port=port, reload=True)
