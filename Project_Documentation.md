# Data Leakage Analysis Platform - Project Documentation

## 1. Project Overview
The Data Leakage Analysis Platform is a robust, full-stack application designed to ingest, validate, and analyze regulatory compliance data. The platform features a dynamic React-based frontend dashboard and a FastAPI-powered Python backend. 

The application supports two distinct operational modes:
1. **Manual Ingestion:** Users can manually configure leakage thresholds and upload CSV files via an intuitive drag-and-drop web interface.
2. **Automated S3 Integration:** The backend seamlessly monitors an AWS S3 bucket for dynamically timestamped files (`Leakage_YYYYMMDD_...`). Upon detection, the file is instantly processed, validated, and safely moved to an archive directory, while the frontend dashboard automatically detects the event and visually tracks the progress.

---

## 2. Project Structure
The project is divided into two primary directories:
```text
D:\S&M Accelerators\
├── backend/                  # Python FastAPI application
│   ├── .env                  # Environment variables (AWS secrets)
│   ├── main.py               # API Endpoints and core processing logic
│   ├── scheduler.py          # Automated S3 file polling and sync logic
│   ├── storage.py            # AWS S3 connection and file management
│   └── requirements.txt      # Python dependencies
└── frontend/                 # React & Vite frontend application
    ├── src/
    │   ├── LeakageDashboard.jsx # Main Dashboard UI Component
    │   ├── main.jsx          # React Entry Point
    │   └── index.css         # TailwindCSS styles
    ├── tailwind.config.js    # Tailwind configuration
    ├── package.json          # Node dependencies
    └── vite.config.js        # Vite bundler configuration
```

---

## 3. Detailed File Breakdown & Purpose

### Backend Components

* **`backend/main.py`**
  * **Purpose:** The core FastAPI server entry point.
  * **Functionality:** 
    * Hosts the `/process-and-merge-csv` endpoint for manual frontend uploads.
    * Uses dynamic `Pydantic` models and `pandas` to read CSV bytes, scrub `NaN` values, validate rows, and convert them into clean JSON payloads.
    * Hosts the `/auto-process-status` endpoint, which triggers the synchronous S3 monitoring logic.

* **`backend/scheduler.py`**
  * **Purpose:** Handles the automated business logic for discovering and processing S3 files.
  * **Functionality:** 
    * Implements `check_s3_sync()`. It generates a target prefix (e.g., `Leakage_20260610_`) and queries S3.
    * If a matching unarchived file is found, it downloads the bytes, routes them through `main.py`'s processing engine, and leverages `storage.py` to move the file into the `Leakage/Archive/` directory.

* **`backend/storage.py`**
  * **Purpose:** The dedicated AWS adapter layer.
  * **Functionality:** Implements the `S3Client` wrapper utilizing `boto3`. Exposes isolated, reusable methods for listing files (`list_files`), downloading streams (`download_file`), and safely archiving objects (`archive_file`).

* **`backend/.env`**
  * **Purpose:** Secrets management.
  * **Functionality:** Stores `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, and `S3_BUCKET_NAME`. Must never be committed to source control.

### Frontend Components

* **`frontend/src/LeakageDashboard.jsx`**
  * **Purpose:** The entire user interface and state management.
  * **Functionality:** 
    * Implements a secure polling mechanism (every 3 seconds) to the backend's `/auto-process-status` endpoint.
    * **Automated S3 UI:** Features a top-pinned, permanently visible "S3 Import Status" widget. It idles on "Monitoring AWS bucket...", but launches an automated progress bar animation and locks the rest of the UI when an S3 import is detected.
    * **Manual Upload UI:** Provides a strictly isolated drag-and-drop zone.
    * **Configuration UI:** Includes a "Leakage Percentage" input field locked via regex to exactly two numeric digits, backed by browser `localStorage` to ensure persistence across sessions.

---

## 4. System Interactions (How it Works)

1. **The Polling Loop:** The React frontend initiates a background `fetch` interval every 3 seconds to `http://localhost:8000/auto-process-status`.
2. **S3 Handshake:** The FastAPI backend receives the ping and asks `scheduler.py` to scan the S3 bucket using `storage.py`.
3. **Data Processing:** 
   * If an expected file is found, it is downloaded in memory, cleaned, and parsed into valid JSON.
   * The file is moved to `Archive/`.
   * The backend responds to the HTTP request with `{"status": "success", "filename": "...", "result": [...]}`.
4. **UI Response:** 
   * The React dashboard receives the payload.
   * It initiates a 3-second simulated progress bar in the top widget.
   * It simultaneously greys out the manual configuration section to prevent data collision.
   * Upon completion, the result payload is rendered elegantly on the screen.

---

## 5. Configurations & Dependencies

**Backend Environment Variables (`.env`)**
```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_region
S3_BUCKET_NAME=your_bucket_name
```

**Key Dependencies**
* **Backend:** FastAPI, Uvicorn, Pandas (Data processing), Boto3 (AWS SDK), Pydantic (Data validation).
* **Frontend:** React, Vite, TailwindCSS.

---

## 6. Setup and Deployment Instructions

### Running the Backend
1. Open a terminal and navigate to `D:\S&M Accelerators\backend`.
2. Activate your virtual environment: `.\venv\Scripts\activate`
3. Install dependencies (if needed): `pip install -r requirements.txt`
4. Ensure your `.env` file is populated with valid AWS credentials.
5. Start the server: `python main.py`

### Running the Frontend
1. Open a new terminal and navigate to `D:\S&M Accelerators\frontend`.
2. Install dependencies (if first time): `npm install`
3. Start the Vite development server: `npm run dev`
4. Open the provided `localhost` link in your browser.

---

## 7. Notes and Future Improvements

* **Synchronous S3 Polling:** Currently, the S3 polling is executed synchronously within the HTTP request from the frontend. This ensures instant updates but slightly delays the HTTP response depending on AWS latency. If file sizes grow significantly, this logic should be decoupled into a true asynchronous Celery/APScheduler background worker.
* **File Validation:** The frontend strictly limits Leakage Percentage inputs to a maximum of two digits (e.g., `99%`). Ensure this aligns with business logic requirements.
* **Storage Archiving:** Files are automatically archived to `Leakage/Archive/` after automated S3 syncing. Ensure S3 bucket lifecycle policies are configured so the archive folder doesn't grow indefinitely over years.
