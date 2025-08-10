#!/usr/bin/env python3
"""
Airflow ETL Functions for Steam Games Data Pipeline

Simple wrapper functions for Airflow DAGs that call the existing
main functions from the project's scripts.
"""

import os
from typing import Dict, Any, Optional

# Import existing main functions
from scripts.steam_data_extractor import main_processing_workflow, create_initial_files
from src.processors.retry_failed_extractions import main as retry_main
from src.loaders.mongodb_loader import main as mongodb_main

# Import main wrapper functions from src
from src.extractors.steam_data_extractor import run_steam_extraction
from src.processors.steam_data_transformer import run_steam_processing
from src.utils.json_saver import run_steam_saving

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
    """
    return run_steam_extraction()


def process_steam_data(**context) -> Dict[str, Any]:
    """
    Step 2: Process Steam data.
    """
    return run_steam_processing()


def save_data_to_json(**context) -> Dict[str, Any]:
    """
    Step 3: Save data locally as JSON.
    """
    return run_steam_saving()


def retry_failed_apps(**context) -> Dict[str, Any]:
    """
    Step 4: Retry failed app extractions.
    """
    retry_main()
    return {"status": "success", "message": "Failed apps retry completed"}


def insert_to_mongodb(**context) -> Dict[str, Any]:
    """
    Step 5: Insert data into MongoDB.
    """
    mongodb_main()
    return {"status": "success", "message": "MongoDB insertion completed"}


# ============================================================================
# MAIN (for testing)
# ============================================================================

if __name__ == "__main__":
    print("Steam Games ETL Functions for Airflow")
    print("Available functions: call_steam_api, process_steam_data, save_data_to_json, retry_failed_apps, insert_to_mongodb")