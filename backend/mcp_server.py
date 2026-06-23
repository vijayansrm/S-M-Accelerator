# mcp_server.py
import os
import sys
from mcp.server.fastmcp import FastMCP
import mysql.connector

# Ensure backend directory is in path for config imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import config

mcp = FastMCP("My Usecases Server")

# ── DB connection helper ──────────────────────────────────────
def get_db():
    return mysql.connector.connect(
        host=config.DB_HOST,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        database=config.DB_NAME,
        port=config.DB_PORT
    )

# ── Usecase 1 as an MCP Tool ──────────────────────────────────
@mcp.tool()
def usecase_1(param1: str, param2: int) -> dict:
    """
    
Here is the simple, step-by-step functional story of how a prescription is intercepted and rescued by the system.

Step 1: The Fast-Layer Intercept (Ingestion & Copay)
	What happens: A patient goes to a specialty pharmacy counter to pick up their high-cost medication. The pharmacist swipes their insurance card and the manufacturer copay coupon. The insurance company immediately rejects the claim because it requires complex paperwork (like a Prior Authorization).
	The System's Action: Instead of waiting weeks for an audit report to find out about this, our system is hooked directly into the Live Copay Card Switch Wire. The exact second the pharmacy counter hits that insurance block, a real-time data alert fires into our system.
Step 2: The Deep Read (AI Processing)
	What happens: The system catches the raw, messy error text sent back by the insurance company (e.g., "PA REQ. SUBMIT ANC LABS TO PORTAL").
	The System's Action: A highly trained, strict AI agent reads this unformatted text note. It doesn't guess or get creative. It instantly pinpoints the exact obstacle—in this case, identifying that the insurance company is refusing to pay because they are missing the patient's recent blood work numbers.
Step 3: Deep Portal Research (The Scraper)
	What happens: The system needs to know the exact, up-to-the-minute rules of that specific insurance plan to bypass the block.
	The System's Action: The system triggers an automated Web Scraper. The scraper goes directly to that specific insurance company’s public web portal, hunts down their latest medical policy document for this exact drug, and grabs the precise requirements and forms needed to get the drug approved.
Step 4: Assembling the Rescue Package (The Payload Core)
	What happens: The system pulls together who the doctor is and where to find them.
	The System's Action: The system automatically cross-references its master databases. It matches the transaction to the HCP Master to find the doctor’s secure office fax number, and the Payer Master to verify the insurance company's true corporate identity. It bundles all of this—the doctor's info, the specific insurance rules found by the scraper, and the pre-filled medical necessity paperwork—into one clean, structured JSON Remediation Payload.
Step 5: Delivering the Fix (Sending Downstream)
	What happens: The payload is finalized.
	The System's Action: Within less than 2 hours from the moment the patient was rejected at the pharmacy, the system dispatches this data package via a secure REST API webhook to downstream systems (like the Hub CRM or automated document servers). This automatically fires the pre-filled forms directly onto the doctor's desk or fax machine so they can sign it and override the insurance block before the patient gets discouraged and abandons their therapy.
Step 6: The Long-Term Safety Net (The Pharmacy Hub Feed)
	What happens: Sometimes a doctor doesn't sign the paperwork immediately, and it sits on a clinic desk for days.
	The System's Action: This is where the Pharmacy Hub Status Feed comes in. Every night, the system reviews a daily status pipeline file from the Specialty Pharmacy network. If it sees that a patient's case is still sitting in a PA_PENDING status for more than 3 consecutive days, a background alarm goes off. It alerts a human case manager to call the doctor's office, ensuring the paperwork never slips through the cracks.

This dual-engine, real-time intervention and lifecycle tracking platform is designed specifically for Specialty Pharma. It explicitly resolves the architectural questions, configuration parameters, data cross-references, and sourcing cadences highlighted in your draft notes.
________________________________________USE CASE 1: LEAKAGE SIGNALS AGENT (HLD)
1. Purpose & Scope
1.1 Purpose
This document defines the high-level functional design for the Leakage Signals Agent. Operating as an autonomous network sentinel, this backend platform utilizes a dual-engine processing model to ingest real-time pharmaceutical transaction streams alongside daily case lifecycle pipelines. It programmatically investigates external insurance coverage policies and compiles automated remediation payloads to prevent patient therapy abandonment.
1.2 Business Context (Problem Statement)
Traditional pharmaceutical brand analytics depend heavily on macro third-party market data (e.g., IQVIA/Symphony claims), which suffer from a multi-week delivery lag. For high-cost specialty brands, biologics, and oncology treatments (ranging from $5,000 to $50,000+ per month), a single insurance rejection paperwork bottleneck at the pharmacy counter frequently results in immediate patient drop-off and permanent revenue leakage.
The Leakage Signals Agent circumvents this data latency. By listening directly to operational transactional wires at the moment of adjudication, it intercepts drop-offs and orchestrates automated administrative routing within 2 hours of a pharmacy counter rejection.
________________________________________2. Objectives
2.1 Primary Objectives
	Real-Time Stream Interception: Capture live point-of-sale pharmacy coverage failures directly from copay card adjudication switches.
	Intra-Day Case Lifecycle Monitoring: Continuously evaluate daily back-end case tracking workflows from specialized Patient Support Hub networks to identify operational bottlenecks.
	Deterministic LLM Extraction: Automate the parsing of unstructured, free-text National Council for Prescription Drug Programs (NCPDP) telecommunication rejection strings using zero-temperature LLM token mapping.
	Remediation Payload Synthesis: Generate structurally validated JSON payloads containing precise clinical criteria requirements and target clinician contact endpoints to bypass insurance roadblocks.
2.2 Success Criteria (MVP Pilot)
	Real-Time Pipeline Processing Speed: Time from live copay transaction rejection detection to outbound remediation payload egress must be $< 2$ hours.
	Zero Creative Deviation: Achieve a 100% deterministic evaluation rate (zero hallucinations) across scraped insurance policy criteria evaluations.
	Automated Exception Handling: Process incoming streaming lines through strict runtime schema validation layers with a $< 0.1\%$ data dropping rate.
________________________________________3. Positioning & Design Principles
3.1 Design Principles
	Dual-Engine Ingestion Topology (Lambda-Style Architecture): Run parallel operational pipelines. A fast-layer streaming engine processes live transactional switch events, while a companion batch engine processes daily/hourly case updates from Specialty Pharmacies (SPs) and Hub data partners.
	Unified State Tracking: Maintain a central, single-source-of-truth table mapping disparate transaction data types into an immutable Patient Longitudinal Lifecycle State Machine.
	Ephemeral Stream Volatility: To ensure data privacy and optimize computing pipelines, raw real-time streaming string events are parsed, checked, and transformed in-memory, then wiped immediately following outbound payload execution.
	Strict Parameter Adherence: Lock LLM configurations to temperature: 0.0 combined with strongly typed output schemas to ensure structural consistency.
3.2 Out of Scope (Explicit)
	Long-term comprehensive market data warehousing (delegated to historical strategic data layers like IQVIA/Symphony).
	End-user graphical application dashboarding for patients or field sales representatives.
	Direct un-orchestrated text or email communication to patients or providers.
________________________________________4. User Roles & Permissions
	Commercial Excellence & Market Access Lead: Configures regional anomaly warning metrics, defines threshold limits, and adjusts financial alert sensitivity boundaries via the centralized rules interface.
	Ops / Technical System Support: Monitors pipeline ingestion performance, evaluates streaming execution latencies, checks API webhook heartbeats, and inspects JSON format exception logs.
________________________________________5. High-Level System Architecture (Logical)



        [ 1. REAL-TIME FAST LAYER ]             [ 2. BATCH CONTROL LAYER ]
        (Live Copay Stream / NCPDP D.0)          (Daily Hub Lifecycle Feed)
                      |                                       |
                      v                                       v
        +-------------------------------------------------------------+
        |                  CENTRAL LIFE-CYCLE REPOSITORY              |
        |             (Unified State Tracking Database Table)         |
        +-------------------------------------------------------------+
               |                       |                       |
      (Join on Payer_BIN)      (Join on NDC)         (Join on Prescriber_NPI)
               |                       |                       |
               v                       v                       v
       [ PAYER MASTER ]       [ PAYER POLICY ENGINE ]    [ HCP MASTER ]
               \                       |                       /
                \                      |                      /
                 v                     v                     v
              +-------------------------------------------------+
              |        AUTOMATED REMEDIATION COMPILER          |
              +-------------------------------------------------+
                                       |
                                       v (REST API Webhook Outbound)
                        [ Downstream Journey Orchestrator ]

5.1 Functional Layers
Function 1: Dual-Engine Data Ingestion (FR-1.1)
The engine maintains a continuous, split-path data intake loop to capture both fast and slow transactional updates.
	The Action: It listens to live, incoming NCPDP D.0 network strings via secure webhooks from your copay switch (the Fast Layer) while concurrently absorbing nightly pipe-delimited flat files from your Specialty Pharmacy and Hub network (the Batch Layer).
	The Purpose: It guarantees that the system catches instant pharmacy-counter blocks while retaining long-term visibility over patients whose paperwork stalls over a 14-day window.
	Note - For the copay stream function, we need an asynchronous API endpoint built in a framework like FastAPI or Go, backed by a message broker like AWS SQS or Redis Streams. It needs to accept the payload, acknowledge it immediately with a 200 OK, and then push it to an internal processing worker that validates the fields against our Pydantic schemas and checks for Medicaid flags
Function 2: Reference Mapping & Compliance Circuit Breaker (FR-1.2)
The engine instantly translates raw, incoming network shorthand codes into real enterprise business objects and enforces strict legal boundaries.
	The Action: It takes raw codes like Payer_BIN: 610014 and NDC: 00006-0249-31 and runs millimeter-second database joins against your master tables to resolve them to "Aetna Commercial" and your human-readable "Brand Name."
	The Legal Guardrail: If the lookup reveals that a patient is covered under a government plan (Medicaid, Medicare Part D, or TRICARE), the system trips a compliance circuit breaker. It permanently disables all financial coupon payloads to comply with the Anti-Kickback Statute, forcing the system to switch to a pure, clinical-paperwork-only override loop.
Function 3: Localized Anomaly Windowing & Aggregation (FR-1.3)
The engine acts as a regional smoke detector by analyzing historical transaction volumes over moving windows.
	The Action: It continuously calculates script rejection rates over a rolling 24-hour sliding window, grouping transaction volumes by the patient's 5-digit ZIP code, target product brand, and critical NCPDP reject codes ($70 = \text{Product Not Covered}$, $75 = \text{Prior Authorization Required}$).
	The Purpose: It identifies structural insurance blockades (e.g., a payer completely dropping coverage for your drug across an entire city block) rather than isolated, one-off patient errors.
Leakage Calculation Logic
You are already grouping by:
	Patient ZIP Code (5-digit) 
	Product Brand 
	Reject Code (70, 75) 
	Rolling 24-hour window
Formula 1: Basic Leakage %
Leakage%="Rejected Scripts" /"Total Scripts Submitted" ×100

Where:
	Rejected Scripts = Count of transactions with Reject Code 70 or 75 
	Total Scripts Submitted = Successful + Rejected transactions 
Example
ZIP	Brand	Total Scripts	Reject 70	Reject 75
600001	Drug A	1000	120	80
Rejected Scripts
120+80=200

Leakage %
200/1000×100=20%


Function 4: Decoupled Runtime Threshold Evaluation (FR-1.4)
The engine acts as the live "volume knob" that decides exactly when a regional rejection spike is dangerous enough to ring the alarm.
	The Action: Instead of hardcoding alert metrics (like 12%) into the application, the system pulls sensitivity limits dynamically from an in-memory Redis cache at runtime.
	The Purpose: It allows your business team to adjust alert sensitivity (e.g., dialing a threshold down from 12% to 5% during a major competitor launch) on a live dashboard with zero server downtime or code rewrites.
Function 5: Autonomous Policy Scraping & Payload Egress (FR-1.5)
The execution layer that builds and fires the final remediation package to fix the leakage.
	The Action: The moment an alert threshold is breached, the engine dispatches a headless web scraper to target insurance portals, extracts the exact clinical documentation or prior authorization forms needed, and compiles everything into a single, unified JSON payload.
	The Purpose: It bundles the doctor's secure fax number, the payer criteria text, and patient token context into a structured format, transmitting it to the downstream message broker in $< 2$ hours so the clinic can save the script before the patient walks away.
________________________________________

Requirement	What Changed technically?	Why it Matters
FR-1.1	Shifted from retrospective IQVIA/Symphony data to a concurrent Copay + Hub Lambda-style topology.	Fixes the data latency flaw; gives your platform the actual files it needs to achieve the $< 2$-hour SLA.
FR-1.2	Shifted from an expensive AI-guessing loop on text strings to a deterministic Foreign Key join via payer_master.	Lowers computing overhead to milliseconds, handles the critical Medicaid legal exception, and completely prevents LLM hallucination during ingestion.
FR-1.3 & 1.4	Shifted from a batch-query computation to a true, live stream-processing window mapped against Redis.	Allows the system to identify an insurance block in a specific ZIP code the exact hour it happens and adjust rules on the fly.
FR-1.5	Retained the scraping trigger but bound it directly to the new multi-table relational view variables.	Guarantees the payload contains the precise provider fax numbers and brand variables needed to execute the rescue.

________________________________________6. Core Workflow
	Ingest & Validate: Capture live transaction strings via the Streaming Adapter or pull batch files via the Pipeline Adapter; validate records instantly against strict schema parsing structures.
	Translate & Consolidate: Convert incoming parameters into a unified system state, applying a Timestamp-Based Last-Write-Wins Upsert Constraint to prevent batch files from overwriting more recent real-time stream state changes.
	Analyse and Trigger:
	Streaming Logic: Instantly evaluate transactions against specific rejection parameters. If an insurance rejection is encountered, trigger the automated remediation pipeline.
	Batch Logic: Identify stalled cases. If a patient records' status has stalled for longer than configured business criteria limits, flag an anomaly.
	Investigate Context: Map the transaction's unique identification parameters against relational lookup infrastructure to extract corporate entity rules and physician fax endpoints.
	Scrape Dynamic Rules: Direct the scraper to navigate to the designated plan portal to pull real-time clinical requirements matching the exact transaction context.
	Compile and Ship: Package the parsed requirements, target clinician criteria, and contact pathways into a single structured validation object and transmit the payload via a secure REST webhook to downstream fulfillment environments.
________________________________________7. Data Inputs & System Configuration

7.1 Input File Inventory & Reference Masters
________________________________________8. Data Validation Rules
8.1 Schema Definitions & Data Dictionary
Real-Time Copay Stream (copay_transactions)
7.1 Input File Inventory & Reference Masters
The Leakage Signals Agent relies on a synchronized blend of two active transactional operational layers and four static/periodic enterprise master datasets.
1. Real-Time Copay Stream (copay_transactions)
	Sourcing Environment: Syndicated event wire hosted by the brand’s copay program vendor (e.g., TrialCard, ConnectiveRx).
	Delivery Mechanism / Cadence: Instantaneous streaming via secure Webhook Event Bus or compressed hourly micro-batch SFTP drops.
	System Role: Serves as the real-time operational spark. Captures split-second counter rejection telemetry immediately at the pharmacy point-of-sale to trigger the $<2\text{-hour}$ mitigation pipeline.
2. Pharmacy Hub Status Feed (hub_pipeline_status)
	Sourcing Environment: Compiled aggregation reports from the central patient services Hub and Specialty Pharmacy (SP) network.
	Delivery Mechanism / Cadence: Nightly pipe-delimited flat file (.csv/.txt) delivered via Secure FTP.
	System Role: Serves as the long-term lifecycle safety net. Tracks slow-moving administrative states across a 3-to-14 day timeline to flag stalled workflows.
3. HCP Master Reference (hcp_master)
	Sourcing Environment: Internal commercial operations database cross-referenced with the national CMS NPPES registry.
	Delivery Mechanism / Cadence: Weekly/Monthly batch updates.
	System Role: Supplies verified physician identities, practice locations, and crucial Secure Fax Numbers used to deliver the finalized automated mitigation documents to the clinician's desk.
4. Payer Master Reference (payer_master)
	Sourcing Environment: Managed Markets / Market Access internal standard matrix.
	Delivery Mechanism / Cadence: Monthly or quarterly static updates.
	System Role: Maps the raw 6-digit clearinghouse network Payer_BIN to clean corporate identities, plan types, and compliance-driven market channel classifications.
5. Payer Policy Changes (payer_policy_rules)
	Sourcing Environment: Dynamic knowledge base populated by automated web scraper modules hitting public insurance portals.
	Delivery Mechanism / Cadence: Continuous or daily differential crawling loops.
	System Role: Supplies the strict clinical criteria, step-therapy prerequisites, and formulary restriction notes that the AI uses to customize the prior authorization payload text.
6. Brand Master Reference (brand_master)
	Sourcing Environment: Corporate Data Governance / Trade and Distribution team.
	Delivery Mechanism / Cadence: Static batch (updated exclusively upon new product packaging or line extension launches).
	System Role: Translates raw 11-digit numeric National Drug Code barcode numbers (NDC_Code) into explicit text brand labels and internal product tracking codes needed by the scraper engine.
________________________________________

7.2 System Mapping & Join Strategy
To securely compile the comprehensive contextual background required by the zero-temperature LLM parsing agent, the stream processor joins incoming transaction records against reference tables using the following pre-indexed join path:
SQL
CREATE OR REPLACE VIEW v_remediation_payload_context AS
SELECT 
    -- Transactional Stream Telemetry
    ctx.Transaction_ID,
    ctx.Patient_Token,
    ctx.Timestamp AS rejection_timestamp,
    ctx.Reject_Code,
    ctx.Reject_Message,
    ctx.Pharmacy_ID,
    
    -- Product Master Resolution
    brd.Brand_Name,
    brd.Product_Code,
    
    -- Healthcare Professional (HCP) Egress Targets
    hcp.First_Name AS doc_first_name,
    hcp.Last_Name AS doc_last_name,
    hcp.Secure_Fax AS doc_secure_fax,
    hcp.Practice_State AS doc_state,
    
    -- Insurance Corporate Profiles & Channel Segments
    pyr.Payer_Name,
    pyr.Market_Channel,
    
    -- Live Scraped Medical Policy Criteria
    pol.Extracted_Clinical_Criteria,
    pol.Formulary_Status_Code

FROM copay_transactions ctx
-- Join 1: Translate NDC-11 Barcode to Clean Brand Identity Strings
INNER JOIN brand_master brd 
    ON ctx.NDC_Code = brd.NDC_Code
-- Join 2: Resolve Prescribing Clinician Data and Outbound Contact Nodes
LEFT JOIN hcp_master hcp 
    ON ctx.Prescriber_NPI = hcp.Prescriber_NPI
-- Join 3: Identify Payer Corporate Entities & Market Segments (Medicaid Check)
LEFT JOIN payer_master pyr 
    ON ctx.Payer_BIN = pyr.Payer_BIN
-- Join 4: Extract Real-Time Web Scraper Portal Rules Criteria
LEFT JOIN payer_policy_rules pol 
    ON ctx.Payer_BIN = pol.Payer_BIN 
    AND ctx.NDC_Code = pol.NDC_Code
WHERE ctx.Response_Status = 'R';

________________________________________





8. Data Validation Rules & Schema Definitions
8.1 Schema Definitions & Data Dictionary
Table 1: Real-Time Copay Stream (copay_transactions)
	Storage Engine Layer: Fast Streaming Layer (Pre-Indexed via TimeSeries or B-Tree)
Field Name	Data Type	Database Constraint	Business Logic / Data Validation Rule
Transaction_ID	VARCHAR(64)	PRIMARY KEY	Unique cryptographic transaction string originating from the copay switch.
Timestamp	TIMESTAMP	NOT NULL	The exact UTC date/time the card swipe occurred at the pharmacy counter.
Patient_Token	VARCHAR(64)	NOT NULL, INDEX	The Universal Identity Link. Secure, tokenized longitudinal hash representation of the patient. Real-time encryption enforced.
Prescriber_NPI	CHAR(10)	REFERENCES hcp_master	Must consist of exactly 10 numeric characters. Invalid patterns dump the record to the dead-letter queue (DLQ).
Pharmacy_ID	VARCHAR(32)	NOT NULL	Standard identifier for the dispensing specialty pharmacy location. Used for 340B verification checks.
NDC_Code	CHAR(11)	REFERENCES brand_master	National Drug Code format. Mandatory constraint to resolve specific portfolio brand rules.
Payer_BIN	CHAR(6)	REFERENCES payer_master	6-digit Banking Identification Number. Essential for routing policy lookups.
Response_Status	CHAR(1)	CHECK IN ('A', 'R', 'V')	Approved (A), Rejected (R), or Reversed (V). Only rejections (R) trigger active AI pipelines.
Reject_Code	VARCHAR(10)	NULLABLE	Standard NCPDP code metrics (e.g., 70, 75). Empty if transaction status is A.
Reject_Message	TEXT	NULLABLE	Raw free-text narrative note returned by the insurance processor. Fed directly to zero-temperature LLM parsing strings.

Table 2: Pharmacy Hub Status Feed (hub_pipeline_status)
	Storage Engine Layer: Core Relational Operational Data Store (ODS)
Field Name	Data Type	Database Constraint	Business Logic / Data Validation Rule
Case_ID	VARCHAR(64)	PRIMARY KEY	Unique tracking system ID generated by the Specialty Pharmacy or Hub provider.
Patient_Token	VARCHAR(64)	NOT NULL, INDEX	Maps directly to the Patient_Token in the transaction tables to join the fast and slow layers.
Update_Date	DATE	NOT NULL	System processing date for the record. Governs the Last-Write-Wins Upsert logic.
Current_Status	VARCHAR(50)	NOT NULL	Enforced workflow boundaries: BENEFIT_VERIFICATION, PA_PENDING, PA_DENIED, CLOSED.
Sub_Status	VARCHAR(100)	NULLABLE	Granular workflow milestones (e.g., WAITING_ON_PHYSICIAN_SIGNATURE).
Days_In_Status	INT	CHECK (Days_In_Status >= 0)	Rolling day count tracking the current state. Values $\ge 3$ trigger background slow leakage alarms.
Assigned_Agent_ID	VARCHAR(32)	NULLABLE	Audit tracking linking back to the internal case management technician profile.

Table 3: HCP Master Reference (hcp_master)
Field Name	Data Type	Database Constraint	Business Logic / Data Validation Rule
Prescriber_NPI	CHAR(10)	PRIMARY KEY	Validated 10-digit National Provider Identifier barcode string.
First_Name	VARCHAR(50)	NOT NULL	Clinician given name.
Last_Name	VARCHAR(50)	NOT NULL	Clinician family name. Used to verify signature signatures on pre-filled templates.
Specialty	VARCHAR(100)	NULLABLE	Focus field description metrics (e.g., Oncology, Rheumatology).
State_License	VARCHAR(32)	NOT NULL	Unique state authority license identification array.
Practice_State	CHAR(2)	NOT NULL	2-letter state abbreviation code. Used to dynamically query regional medical policy rules.
Contact_Phone	VARCHAR(20)	NULLABLE	Telephone callback link data.
Secure_Fax	VARCHAR(20)	NOT NULL	Critical Path Egress Key. The secure office line endpoint where automated remediation packages are transmitted.

Table 4: Payer Master Reference (payer_master)
Field Name	Data Type	Database Constraint	Business Logic / Data Validation Rule
Payer_BIN	CHAR(6)	PRIMARY KEY	Unique 6-digit clearinghouse network identifier.
Payer_PCN	VARCHAR(10)	NULLABLE	Processor Control Number string parameter details.
Payer_Group	VARCHAR(32)	NULLABLE	Specific plan identifier parameters.
Payer_Name	VARCHAR(100)	NOT NULL	Standardized corporate market text title (e.g., Aetna Commercial).
Market_Channel	VARCHAR(50)	NOT NULL	Tracks coverage channel classifications: Commercial, Medicaid, Managed Medicaid, Medicare Part D, TRICARE.
Parent_Organization	VARCHAR(100)	NULLABLE	Parent company structure identification (e.g., CVS Health, Cigna).

Table 5: Payer Policy Rules Knowledge Base (payer_policy_rules)
Field Name	Data Type	Database Constraint	Business Logic / Data Validation Rule
Policy_ID	VARCHAR(64)	PRIMARY KEY	Tracking index generated by the web scraping infrastructure module.
Payer_BIN	CHAR(6)	REFERENCES payer_master	Links policy changes directly to insurance accounts.
NDC_Code	CHAR(11)	REFERENCES brand_master	Restricts rules metadata definitions specifically to target molecular drug assets.
Policy_Change_Type	VARCHAR(64)	NOT NULL	Type categories (e.g., FORMULARY_RESTRICTION_ADDED, STEP_THERAPY_REVISED).
Effective_Date	DATE	NOT NULL	Calendar enforcement initialization milestone marker.
Scraped_Timestamp	TIMESTAMP	NOT NULL	Explicit timestamp recording when the crawler extracted the data from the payer site.
Extracted_Clinical_Criteria	TEXT	NOT NULL	Raw text parameters passed directly to the LLM template context engine.
Formulary_Status_Code	VARCHAR(32)	NOT NULL	Normalized classification flag (e.g., PA Required, Quantity Limit Appended).

Table 6: Brand Master Reference (brand_master)
Field Name	Data Type	Database Constraint	Business Logic / Data Validation Rule
NDC_Code	CHAR(11)	PRIMARY KEY	Clean, 11-digit universal numeric package identification key.
Brand_Name	VARCHAR(100)	NOT NULL	Standard commercial name of the drug. Passed directly to search fields on insurance web portals.
Product_Code	VARCHAR(32)	NOT NULL, INDEX	Internal tracking identity flag utilized across sales operations and billing.
Therapeutic_Class	VARCHAR(100)	NULLABLE	Broader category designation tracking classification details (e.g., Oncology Biologics).
________________________________________
8.2 Ingestion & State-Matching Pipeline Constraints
To protect data synchronization integrity across split-second streaming triggers and lagging daily flat file batches, your development team must enforce these two runtime pipelining constraints in the orchestration logic:
1. The Medicaid / Government Compliance Circuit Breaker
The ingestion engine must continually evaluate the Market_Channel field resolved from the payer_master lookup immediately upon new file capture.
If Market_Channel matches Medicaid, Managed Medicaid, Medicare Part D, or TRICARE, the system must override standard processing logic to protect against Anti-Kickback Statute violations. The core engine will automatically turn off financial coupon assignment mechanisms and shift the output compiler exclusively to routing standard clinical policy documentation and state prior authorization forms.
2. Timestamp-Based Last-Write-Wins Upsert Constraint
Because the periodic Hub file drop contains structural case snapshots, it risks creating a data race condition if a patient encounters a new streaming point-of-sale rejection on the same day the batch report is processed.
The analytical database must enforce a timestamp conditional rule across all lifecycle status tables:
SQL
-- Prevents lagging batch loads from rolling back real-time event updates
INSERT INTO patient_lifecycle_master (Patient_Token, current_lifecycle_state, last_updated_timestamp)
VALUES (:incoming_token, :incoming_status, :incoming_timestamp)
ON CONFLICT (Patient_Token) 
DO UPDATE SET 
    current_lifecycle_state = EXCLUDED.current_lifecycle_state,
    last_updated_timestamp = EXCLUDED.last_updated_timestamp
WHERE EXCLUDED.last_updated_timestamp > patient_lifecycle_master.last_updated_timestamp;

________________________________________9. Leakage Rules Engine
9.1 Core Rule Categories & Alert Configuration Parameters
1. Real-Time Claim Exception Rules (Fast-Layer Stream Trigger)
	Operational Mechanism: Evaluates incoming NCPDP D.0 transaction payloads in-memory at runtime.
	Trigger Condition: Response_Status == 'R' and Reject_Code matches values in ['70', '75', '76'].
	Where to Configure: Business users adjust these target transaction error tracking arrays directly via the Central Alert Configuration Look-Up Table UI. This allows users to add or remove specific rejection telemetry parameters without code modifications.
2. Case In-Status Lifecycle Aging Rules (Batch-Layer Engine Trigger)
	Operational Mechanism: Background cron schedules query the system database following the completion of daily Hub data drops.
	Trigger Condition: Current_Status == 'PA_PENDING' and Days_In_Status >= Target_Threshold_Parameter.
	Where to Configure: Regional administrators configure the Target_Threshold_Parameter values (e.g., flag warning state if case remains stalled $\ge 3$ days) inside the Geographic Threshold Limits Panel, partitioned by zip code and therapeutic brand category.
9.2 Alert Prioritization & Revenue Valuation Logic
To prevent alert fatigue and optimize operational focus, outbound remediation workflows are prioritized dynamically by calculating estimated immediate revenue exposure.
	The Valuation Strategy: The system calculates prioritized pipeline values by executing a backend programmatic function that multiplies regional prescription impact metrics by product wholesale metrics. This model must be configured in direct partnership with the internal Market Access and Trade Account Teams to ensure data precision.
	The Priority Equation:
$$\text{Alert Priority Score} = (\text{Historical Regional Weekly Brand Run-Rate TRx}) \times (\text{Wholesale Acquisition Cost [WAC]}) \times (\text{Payer Plan Market Access Penalty Weight})$$
The resulting score categorizes alerts into execution queues:
Priority Class	Score Boundary	Required Platform Remediation SLA
Critical Tier 1	$\text{Score} \ge \$150,000\text{ / week}$	Automated execution and outbound routing in $< 2$ hours
High Tier 2	$\$50,000 \le \text{Score} < \$150,000\text{ / week}$	Automated validation; routing execution within 4 hours
Standard Tier 3	$\text{Score} < \$50,000\text{ / week}$	Scheduled batch routing compilation every 24 hours







10. Alert Management
10.1 Unified Alert Object Payload Layout
When an exception condition passes validation thresholds, the platform assembles a standard JSON payload format containing all necessary downstream variables:

JSON


{
  "alert_metadata": {
    "signal_id": "SIG-883F-992A1-LL32",
    "priority_level": "CRITICAL_TIER_1",
    "generation_timestamp": "2026-06-08T15:30:00Z"
  },
  "patient_context": {
    "patient_lifecycle_token": "TOK_8F29A3B49E10C9D2E384A881B",
    "target_geographic_3_zip": "902"
  },
  "insurance_context": {
    "standardized_payer_id": "PAYER_AETNA_COMMERCIAL_01",
    "clearinghouse_bin": "610014",
    "adjudicated_ndc_target": "00006-0249-31",
    "telemetry_reject_code": "75"
  },
  "clinical_intelligence": {
    "raw_counter_rejection_string": "PA REQUIRED. SUBMIT ANC LEVEL LABS FROM PAST 14 DAYS TO PORTAL.",
    "llm_parsed_root_cause": "Prior Authorization missing explicit Absolute Neutrophil Count laboratory clinical evidence parameters.",
    "extracted_payer_criteria_requirements": "Section 4.2.1: Specialty coverage authorization requires clinical proof of absolute neutrophil counts above 1500 cells/mm3.",
    "required_remediation_form_url": "https://provider.com/forms/aetna-specialty-oncology-pa-2026.pdf"
  },
  "routing_target": {
    "target_prescriber_npi": "1982736452",
    "physician_full_name": "Dr. Sarah Jenkins",
    "destination_secure_fax": "310-555-0144",
    "clinic_phone_line": "310-555-0145"
  }
}

________________________________________11. Alert Routing & Escalation
11.1 Dynamic Egress Webhook Configuration
The platform functions as a decoupled, asynchronous microservice. Upon assembling a validated structural data object, the integration broker executes an automated POST request containing the target JSON payload to designated downstream target API handlers.
	Primary Routing Pathway: Pushes data directly to the client's core Journey Orchestration Platform or Patient Support Hub Hub CRM system (e.g., Salesforce Health Cloud endpoint) to systematically trigger task generation for human navigators or automated electronic fax services.
	Secondary Remediation Pathway: Routes documentation packages directly to automated electronic document servers to initiate on-the-spot fax delivery back to the physician's clinic endpoint (destination_secure_fax) within 2 hours of counter failure.
________________________________________12. Intervention Tracking
12.1 Internal Workflow State Machine
The backend platform logs lifecycle events within its local storage layer to measure the throughput velocity of the pipeline components:



[ State: SIGNAL_DETECTED ] -> Inbound real-time rejection event parsed and verified.
             |
             v
[ State: CONTEXT_RESOLVED ] -> Database relational lookups against reference tables complete.
             |
             v
[ State: KNOWLEDGE_SYNTHESIZED ] -> Policy criteria matched via web scraper processing modules.
             |
             v
[ State: PAYLOAD_DISPATCHED ] -> Validation passed; egress REST webhook executed successfully.

________________________________________13. KPIs & Audit Outputs
13.1 Operational Metric Parameters
	Time to Ingestive Intervention: Measures the total duration from point-of-sale claim failure to outbound payload validation delivery (Target SLA: $< 2$ hours).
	Source System Processing Frequency: Configured to sync continuously for real-time copay stream endpoints, and once every 24 hours for standard specialty pipeline and support hub file uploads.
	Schema Validation Reliability Metric: Percent of incoming transactions successfully structured without data dropping anomalies (Target Threshold: $> 99.9\%$).
	Scraper Query Resolution Metric: Success rate of accessing and extracting parameters from public insurance portals (Target Threshold: $> 95.0\%$).
________________________________________14. Non-Functional Requirements
14.1 Security, Confidentiality, and Operational Guardrails
	Protected Health Information (PHI) Protection: The system remains completely compliant with HIPAA privacy protocols by enforcing an architecture that excludes raw patient identifiers (names, dates of birth, social security numbers, or full member registry profiles). All patient records utilize a static, de-identified 64-character token (Patient_Token) generated prior to platform ingestion.
	Data Storage Minimization: Real-time counter transaction streams are held strictly in temporary volatile memory arrays during the contextual lookup phase. Once the system generates and transmits the outbound payload, the session data is cleared from memory.
	Deterministic Configuration Guardrails: System orchestration modules must set LLM engine hyperparameters to a strict temperature = 0.0 with explicit schema structural requirements to block unauthorized data manipulation or creative content formatting variances.
________________________________________15. Acceptance Criteria
	Functional Verification Loop: Given an input transaction containing a 15% Anthem insurance rejection spike for an oncology asset within geographic boundaries 902, the streaming engine must detect the threshold breach and initiate the policy lookup routine.
	Structural Payload Verification: The platform must successfully output an error-free, syntactically sound JSON payload object matching target schema configurations, and successfully deliver it to downstream endpoints without manual code patching or administrative intervention.


In the specialty pharmacy and market access space, When rolling out an automated tool like the Leakage Signals Agent, your developers must implement the exact same "Clinical-Only" exception routing for any patient covered under a government-funded healthcare program or specific hospital discount networks.
If the system mistakenly automates a commercial copay card or financial subsidy for these categories, it violates the Federal Anti-Kickback Statute (AKS), exposing the pharmaceutical client to severe compliance penalties.
Here are the other critical exceptions your software design must account for:
________________________________________
1. Medicare Part D (The Largest Exception)
	What it is: The federal prescription drug program for seniors (65+) and individuals with specific disabilities.
	The Impact on Your Logic: Just like Medicaid, Medicare Part D patients are legally barred from using manufacturer copay coupons.
	The Coding Adjustment: Your payer_master reference table tracks this via the Market_Channel column. If the incoming transaction maps to Market_Channel == 'Medicare Part D', the code must execute the Compliance Circuit Breaker:
	Drop all commercial coupon/copay adjustments.
	Pivot the LLM parsing logic to look for standard Medicare Part D coverage criteria (such as specialized prior authorization forms or diagnostic ICD-10 codes).
2. TRICARE & Veteran Affairs (VA)
	What it is: Comprehensive health benefits managed by the Department of Defense for active-duty military personnel, retirees, and their dependents, alongside VA healthcare facilities.
	The Impact on Your Logic: These programs utilize strict government-negotiated statutory pricing.
	The Coding Adjustment: When the live copay stream encounters a TRICARE-specific Payer BIN (e.g., Express Scripts Military setups), the system must immediately flags this as a Government Account. The output payload must alter its destination, routing the remediation directly to federal defense formulary portals rather than commercial portal links.
3. The 340B Drug Pricing Program (Hospital-Level Exception)
	What it is: A federal program requiring drug manufacturers to provide outpatient drugs to eligible healthcare organizations/covered entities (like safety-net hospitals and specialized clinics) at significantly reduced prices.
	The Impact on Your Logic: If a patient fills their prescription at a 340B contract pharmacy, the hospital is already buying the drug at a steep discount. In many arrangements, combining an already-discounted 340B drug with a manufacturer retail copay card is considered compliance "double-dipping."
	The Coding Adjustment: This requires a check on your Pharmacy Master Data. If the Pharmacy_ID in the live copay stream maps to a known 340B Covered Entity or Contract Pharmacy, the system must evaluate if the brand allows concurrent copay utilization. If not, the system safely routes the case to the Patient Hub for manual human verification rather than firing an instant automated payload.
4. Payer-Imposed Barriers: "Copay Accumulators" and "Maximizers"
	What it is: A commercial insurance tactic where the payer accepts the manufacturer's copay coupon money but refuses to count that money toward the patient’s annual out-of-pocket deductible. Once the coupon budget runs out, the patient faces a massive bill at the counter and drops off therapy.
	The Impact on Your Logic: This creates a sudden, massive drop-off anomaly that isn't caused by a missing form, but by an insurance optimization scheme.
	The Coding Adjustment: Your Payer Policy rules scraper needs to flag if a specific commercial plan has added an "Accumulator" policy amendment. When identified, the agent adjusts the payload to alert the Patient Support Hub to switch the patient to an alternative assistance vehicle (like an independent charitable patient foundation grant) because standard copay card workflows will inevitably fail long-term.
________________________________________
Technical Summary for the Developer Backlog
To make your platform structurally sound, tell your engineering team to group all these exceptions into a single Pydantic configuration rule:
Python
GOVERNMENT_PROGRAMS = ["Medicaid", "Managed Medicaid", "Medicare Part D", "TRICARE", "VA"]

def determine_remediation_strategy(market_channel, pharmacy_attributes):
    if market_channel in GOVERNMENT_PROGRAMS:
        return "CLINICAL_PAPERWORK_ONLY_NO_FINANCIAL_SUBSIDY"
    
    if pharmacy_attributes.is_340b_entity:
        return "ROUTE_TO_HUB_FOR_COMPLIANCE_REVIEW"
        
    return "STANDARD_FAST_TRACK_COPAY_RESCUE"
By adding these parameters to your design template, your platform architecture becomes completely bulletproof, demonstrating to pharma clients that your AI handles complex legal compliance just as effectively as it handles data engineering.


Design considerations: 

If your developers are currently building out the backend microservices for this sprint, the next big structural surprise is a complete, production-ready Database Constraints & Data Integrity Sheet.
This bridges the gap between the raw Python generator script and your database layer (PostgreSQL, Snowflake, or BigQuery). It defines the exact database mechanics required to prevent duplicate states, handle late-arriving batch data, and implement the compliance logic we just discussed.
Here is the blueprint to drop directly into your developer backlog:
________________________________________
Database Constraints Sheet for the Dev Sprint
1. Table: copay_transactions (Fast-Layer Stream)
Field Name	Data Type	Database Constraint	Business Logic & Integrity Rule
Transaction_ID	VARCHAR(64)	PRIMARY KEY	Unique cryptographic hash generated by the copay switch per transaction swipe.
Patient_Token	VARCHAR(64)	NOT NULL, INDEX	De-identified patient anchor. Indexed heavily to allow $<2\text{-hour}$ cross-referencing lookups.
Prescriber_NPI	CHAR(10)	REFERENCES hcp_master(Prescriber_NPI)	Foreign Key. Must exist in your provider registry to resolve office routing details.
Payer_BIN	CHAR(6)	REFERENCES payer_master(Payer_BIN)	Foreign Key. Used to immediately identify if the plan falls under a government exclusion block.
Response_Status	CHAR(1)	CHECK (Response_Status IN ('A', 'R', 'V'))	Restricts values strictly to Approved (A), Rejected (R), or Reversed (V). Non-rejected lines bypass the AI agent.
2. Table: hub_pipeline_status (Batch control Layer)
Field Name	Data Type	Database Constraint	Business Logic & Integrity Rule
Case_ID	VARCHAR(64)	PRIMARY KEY	Unique tracking ID originating from the Specialty Pharmacy or Hub CRM.
Patient_Token	VARCHAR(64)	NOT NULL, INDEX	The Universal Join Key. Matches the instant transactional stream profile to track the 14-day journey.
Current_Status	VARCHAR(50)	NOT NULL	State tracking boundaries: BENEFIT_VERIFICATION, PA_PENDING, PA_DENIED, CLOSED.
Days_In_Status	INT	CHECK (Days_In_Status >= 0)	Integer counter used to trigger backend aging rules if a case stalls in PA_PENDING $\ge 3$ days.
________________________________________
3. The Runtime Multi-Table Join View (The Engine's Core SQL)
When a live counter rejection hits the fast layer, your engineering team shouldn't run multiple decoupled database queries. They should deploy a highly optimized, unified database View or materialized query to build the agent's contextual environment instantly.
Here is the exact production query your team can use to compile the payload data:
SQL
CREATE OR REPLACE VIEW v_remediation_payload_context AS
SELECT 
    -- 1. Stream Anchors
    ctx.Transaction_ID,
    ctx.Patient_Token,
    ctx.Timestamp AS rejection_timestamp,
    ctx.Reject_Code,
    ctx.Reject_Message,
    
    -- 2. Resolved Healthcare Professional Metrics (Egress Targets)
    hcp.First_Name,
    hcp.Last_Name,
    hcp.Secure_Fax,
    hcp.Practice_State,
    
    -- 3. Insurance Plan Entity Context & Compliance Channel Flags
    pyr.Payer_Name,
    pyr.Market_Channel,
    
    -- 4. Dynamic Live Scraped Policy Logic
    pol.Extracted_Clinical_Criteria,
    pol.Formulary_Status_Code

FROM copay_transactions ctx
-- Join 1: Resolve clinician office routing targets
LEFT JOIN hcp_master hcp 
    ON ctx.Prescriber_NPI = hcp.Prescriber_NPI
-- Join 2: Identify insurance brand details & market channels
LEFT JOIN payer_master pyr 
    ON ctx.Payer_BIN = pyr.Payer_BIN
-- Join 3: Match current scraped policy rules based on Payer and NDC Asset
LEFT JOIN payer_policy_rules pol 
    ON ctx.Payer_BIN = pol.Payer_BIN 
    AND ctx.NDC_Code = pol.NDC_Code
-- Filter: Only process active, actionable raw counter exceptions
WHERE ctx.Response_Status = 'R';
________________________________________
4. Key Takeaways for the Technical Review
	Zero Duplication Risk: By binding the Patient_Token as an indexed relational parameter across both streams, your developers can confidently scale the system to millions of records without splitting the patient's record across multiple files.
	Instant Payload Compilation: Because this database design relies entirely on pre-indexed foreign key matches (NPI and BIN), the system view will execute and assemble the complete data profile in less than 5 milliseconds, giving your zero-temperature LLM agent the maximum amount of time to complete its processing loop well within your $<2\text{-hour}$ SLA window.
This completes the architectural loop for the development sprint! Do you want to dive deeper into configuring the downstream API webhook schema definitions next?


    """

if __name__ == "__main__":
    mcp.run()