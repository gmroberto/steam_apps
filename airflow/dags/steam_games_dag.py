#!/usr/bin/env python3
"""
Steam Games ETL Pipeline DAG

This DAG orchestrates the complete ETL pipeline for Steam games data:
1. Extract data from Steam API
2. Process and transform the data
3. Save data to JSON files
4. Retry failed extractions
5. Load data into MongoDB

The pipeline is designed to run daily and includes proper error handling,
retries, and monitoring capabilities.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Import our ETL functions from functions directory
from airflow_etl_functions import (
    call_steam_api,
    process_steam_data,
    save_data_to_json,
    retry_failed_apps,
    insert_to_mongodb
)

# ============================================================================
# DAG CONFIGURATION
# ============================================================================

# Default arguments for the DAG
default_args = {
    'owner': 'steam-data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# DAG configuration
dag_config = {
    'dag_id': 'steam_games_etl_pipeline',
    'description': 'Complete ETL pipeline for Steam games data',
    'schedule_interval': '0 2 * * *',  # Daily at 2 AM
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': ['steam', 'games', 'etl', 'data-pipeline'],
}

# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id=dag_config['dag_id'],
    description=dag_config['description'],
    default_args=default_args,
    schedule_interval=dag_config['schedule_interval'],
    max_active_runs=dag_config['max_active_runs'],
    concurrency=dag_config['concurrency'],
    tags=dag_config['tags'],
    catchup=False,
) as dag:

    # ========================================================================
    # START AND END TASKS
    # ========================================================================
    
    start_pipeline = DummyOperator(
        task_id='start_pipeline',
        doc_md="Start marker for the Steam games ETL pipeline"
    )
    
    end_pipeline = DummyOperator(
        task_id='end_pipeline',
        doc_md="End marker for the Steam games ETL pipeline"
    )
    
    # ========================================================================
    # EXTRACTION TASKS
    # ========================================================================
    
    extract_steam_data = PythonOperator(
        task_id='extract_steam_data',
        python_callable=call_steam_api,
        doc_md="""
        Extract Steam games data from the Steam API.
        
        This task:
        - Fetches the complete list of Steam apps
        - Extracts detailed information for each app
        - Handles API rate limiting and retries
        - Returns extraction results with metadata
        """,
        retries=2,
        retry_delay=timedelta(minutes=10),
    )
    
    # ========================================================================
    # PROCESSING TASKS
    # ========================================================================
    
    process_steam_data_task = PythonOperator(
        task_id='process_steam_data',
        python_callable=process_steam_data,
        doc_md="""
        Process and transform the extracted Steam data.
        
        This task:
        - Cleans and standardizes the raw data
        - Applies business rules and filters
        - Generates statistics and aggregations
        - Prepares data for storage
        """,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
    
    # ========================================================================
    # STORAGE TASKS
    # ========================================================================
    
    save_to_json = PythonOperator(
        task_id='save_to_json',
        python_callable=save_data_to_json,
        doc_md="""
        Save processed data to JSON files.
        
        This task:
        - Saves data in multiple formats (full, filtered, by category)
        - Creates backups of existing files
        - Generates statistics files
        - Returns file paths and metadata
        """,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )
    
    # ========================================================================
    # RETRY TASKS
    # ========================================================================
    
    retry_failed_extractions = PythonOperator(
        task_id='retry_failed_extractions',
        python_callable=retry_failed_apps,
        doc_md="""
        Retry failed app extractions.
        
        This task:
        - Identifies apps that failed during extraction
        - Retries extraction with different strategies
        - Updates the main dataset with recovered data
        - Logs retry results and statistics
        """,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
    
    # ========================================================================
    # LOADING TASKS
    # ========================================================================
    
    load_to_mongodb = PythonOperator(
        task_id='load_to_mongodb',
        python_callable=insert_to_mongodb,
        doc_md="""
        Load processed data into MongoDB.
        
        This task:
        - Connects to MongoDB using configured credentials
        - Inserts data in batches for performance
        - Handles duplicate data and conflicts
        - Provides insertion statistics and logs
        """,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )
    
    # ========================================================================
    # TASK DEPENDENCIES
    # ========================================================================
    
    # Main pipeline flow
    start_pipeline >> extract_steam_data >> process_steam_data_task >> save_to_json
    
    # Parallel retry and loading after storage
    save_to_json >> [retry_failed_extractions, load_to_mongodb]
    
    # Both retry and loading must complete before ending
    [retry_failed_extractions, load_to_mongodb] >> end_pipeline

# ============================================================================
# DAG DOCUMENTATION
# ============================================================================

dag.doc_md = """
# Steam Games ETL Pipeline

## Overview
This DAG orchestrates the complete ETL (Extract, Transform, Load) pipeline for Steam games data.

## Pipeline Steps

### 1. Extraction (`extract_steam_data`)
- Fetches complete Steam app list from Steam API
- Extracts detailed information for each app
- Handles API rate limiting and retries
- Stores raw data for processing

### 2. Processing (`process_steam_data`)
- Cleans and standardizes raw data
- Applies business rules and filters
- Generates statistics and aggregations
- Prepares data for storage

### 3. Storage (`save_to_json`)
- Saves processed data to JSON files
- Creates multiple formats (full, filtered, by category)
- Generates backup files
- Creates statistics reports

### 4. Retry (`retry_failed_extractions`)
- Identifies and retries failed extractions
- Uses different strategies for recovery
- Updates dataset with recovered data

### 5. Loading (`load_to_mongodb`)
- Loads processed data into MongoDB
- Handles batch insertion for performance
- Manages duplicates and conflicts

## Schedule
- Runs daily at 2:00 AM
- Maximum 1 active run at a time
- Includes retry logic for failed tasks

## Monitoring
- Email notifications on failures
- Task completion logging
- Performance metrics tracking
- Error handling and recovery

## Configuration
- Uses environment variables for sensitive data
- YAML configuration for application settings
- Configurable retry policies and timeouts
"""

# Export the DAG
globals()[dag_config['dag_id']] = dag
