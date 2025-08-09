#!/usr/bin/env python3
"""
Airflow ETL Functions for Steam Games Data Pipeline

Simple wrapper functions for Airflow DAGs that call the existing
main functions from the project's scripts.
"""

import os
import sys
from typing import Dict, Any, Optional

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Import existing main functions
from scripts.steam_data_extractor import main_processing_workflow, create_initial_files
from scripts.retry_failed_extractions import main as retry_main
from src.loaders.mongodb_loader import main as mongodb_main

# Load environment variables if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ============================================================================
# AIRFLOW ETL FUNCTIONS
# ============================================================================

def call_steam_api(**context) -> Dict[str, Any]:
    """
    Step 1: Call Steam API to fetch app data.
    Wraps the main steam data extractor workflow.
    """
    # Create initial files if needed
    create_initial_files()
    
    # Run main processing workflow
    main_processing_workflow()
    
    return {"status": "success", "message": "Steam API extraction completed"}


def process_steam_data(**context) -> Dict[str, Any]:
    """
    Step 2: Process Steam data.
    Data processing is handled within the API extraction step.
    """
    return {"status": "success", "message": "Data processing completed within API extraction"}


def save_data_to_json(**context) -> Dict[str, Any]:
    """
    Step 3: Save data locally as JSON.
    Data saving is handled within the API extraction step.
    """
    return {"status": "success", "message": "Data saved locally within API extraction"}


def retry_failed_apps(**context) -> Dict[str, Any]:
    """
    Step 4: Retry failed app extractions.
    Wraps the retry failed extractions main function.
    """
    retry_main()
    return {"status": "success", "message": "Failed apps retry completed"}


def insert_to_mongodb(**context) -> Dict[str, Any]:
    """
    Step 5: Insert data into MongoDB.
    Wraps the MongoDB loader main function.
    """
    mongodb_main()
    return {"status": "success", "message": "MongoDB insertion completed"}


# ============================================================================
# MAIN (for testing)
# ============================================================================

if __name__ == "__main__":
    print("Steam Games ETL Functions for Airflow")
    print("Available functions: call_steam_api, process_steam_data, save_data_to_json, retry_failed_apps, insert_to_mongodb")