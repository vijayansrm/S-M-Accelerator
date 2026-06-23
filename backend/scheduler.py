import logging
import asyncio
from datetime import datetime
from storage import S3Client

async def check_s3_sync():
    """
    Asynchronously checks S3 for the file, processes it if found, and returns the status directly.
    """
    # logging.info("Actively checking AWS S3 for today's file...")
    client = S3Client()
    
    today_str = datetime.today().strftime('%Y%m%d')
    expected_prefix = f"Leakage_{today_str}_"
    
    status_response = {
        "last_run": today_str,
        "status": "idle",
        "result": None,
        "error": None
    }
    
    try:
        folder_path = "Leakage"
        
        files = await asyncio.to_thread(client.list_files, folder_path) # Returns full S3 keys
        
        # Check if any file matches the prefix and isn't archived
        target_prefix = f"{folder_path}/{expected_prefix}"
        matched_key = next((key for key in files if key.startswith(target_prefix) and key.endswith(".csv") and "/Archive/" not in key), None)
        
        if matched_key:
            logging.info(f"Found {matched_key}! Processing asynchronously...")
            status_response["status"] = "processing"
            
            # Download file using the exact matched key
            file_bytes = await asyncio.to_thread(client.download_file, matched_key)
            
            actual_filename = matched_key.split("/")[-1]
            
            # Trigger the main processing logic
            from main import process_csv_bytes
            result_data = await process_csv_bytes(file_bytes, expected_prefix.strip('_'), filename=actual_filename)
            
            status_response["status"] = "success"
            status_response["result"] = result_data
            status_response["filename"] = actual_filename
            
            # Archive the file
            full_archive_path = f"{folder_path}/Archive/{actual_filename}"
            await asyncio.to_thread(client.archive_file, matched_key, full_archive_path)
            logging.info("Automated processing complete. File moved to Archive in S3.")
            
        else:
            # Check for invalid/mismatched unarchived files
            unarchived_files = [k for k in files if k.startswith(f"{folder_path}/") and "/Archive/" not in k and k != f"{folder_path}/"]
            if unarchived_files:
                bad_file_key = unarchived_files[0]
                bad_filename = bad_file_key.split("/")[-1]
                logging.warning(f"Ignored mismatched file: {bad_filename}")
                
                # Move to failed archive so we don't infinitely error on it
                await asyncio.to_thread(client.archive_file, bad_file_key, f"{folder_path}/Archive/Failed_{bad_filename}")
                
                status_response["status"] = "error"
                status_response["error"] = f"Invalid S3 File Ignored: '{bad_filename}' does not match expected format 'Leakage_YYYYMMDD_*.csv'."
                status_response["filename"] = bad_filename
            
    except Exception as e:
        logging.error(f"Error during S3 sync: {e}")
        status_response["status"] = "error"
        status_response["error"] = str(e)
        
    return status_response
