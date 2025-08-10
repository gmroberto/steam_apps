#!/usr/bin/env python3
"""
Test Steam Games ETL Pipeline DAG

This DAG orchestrates the test ETL pipeline for Steam games data (100 documents):
1. Extract data from Steam API (limited to 100 apps)
2. Process and transform the data
3. Save data to JSON files (test directory)
4. Load data into MongoDB (test collection)

The pipeline is designed for testing and development with limited scope.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Import our test ETL functions
from tests.test_etl_pipeline import (
    test_extract_steam_data,
    test_process_steam_data,
    test_save_to_json,
    test_load_to_mongodb,
    run_test_etl_pipeline
)

# ============================================================================
# DAG CONFIGURATION
# ============================================================================

# Default arguments for the test DAG
default_args = {
    'owner': 'steam-data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'catchup': False,
}

# Test DAG configuration
dag_config = {
    'dag_id': 'test_steam_games_etl_pipeline',
    'description': 'Test ETL pipeline for Steam games data (100 documents)',
    'schedule_interval': None,  # Manual trigger only for testing
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': ['steam', 'games', 'etl', 'test', 'data-pipeline'],
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
    
    start_test_pipeline = DummyOperator(
        task_id='start_test_pipeline',
        doc_md="Start marker for the test Steam games ETL pipeline"
    )
    
    end_test_pipeline = DummyOperator(
        task_id='end_test_pipeline',
        doc_md="End marker for the test Steam games ETL pipeline"
    )
    
    # ========================================================================
    # OPTION 1: INDIVIDUAL TEST TASKS
    # ========================================================================
    
    test_extract_steam_data_task = PythonOperator(
        task_id='test_extract_steam_data',
        python_callable=test_extract_steam_data,
        doc_md="""
        Test extraction of Steam games data (100 apps).
        
        This task:
        - Fetches Steam app list and limits to 100 apps
        - Extracts detailed information for test apps
        - Uses faster API delays for testing
        - Returns extraction results with metadata
        """,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
    
    test_process_steam_data_task = PythonOperator(
        task_id='test_process_steam_data',
        python_callable=test_process_steam_data,
        doc_md="""
        Test processing and transformation of Steam data.
        
        This task:
        - Cleans and standardizes the raw test data
        - Applies business rules and filters
        - Generates statistics and aggregations
        - Prepares data for storage
        """,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )
    
    test_save_to_json_task = PythonOperator(
        task_id='test_save_to_json',
        python_callable=test_save_to_json,
        doc_md="""
        Test saving processed data to JSON files.
        
        This task:
        - Saves data to test output directory
        - Creates test-specific filenames
        - Generates test statistics files
        - Returns file paths and metadata
        """,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    
    test_load_to_mongodb_task = PythonOperator(
        task_id='test_load_to_mongodb',
        python_callable=test_load_to_mongodb,
        doc_md="""
        Test loading processed data into MongoDB test collection.
        
        This task:
        - Connects to MongoDB test database
        - Inserts data in batches for performance
        - Uses test collection to avoid conflicts
        - Provides insertion statistics and logs
        """,
        retries=1,
        retry_delay=timedelta(minutes=3),
    )
    
    # ========================================================================
    # OPTION 2: COMPLETE TEST PIPELINE TASK
    # ========================================================================
    
    run_complete_test_pipeline = PythonOperator(
        task_id='run_complete_test_pipeline',
        python_callable=run_test_etl_pipeline,
        doc_md="""
        Run the complete test ETL pipeline in a single task.
        
        This task:
        - Executes all ETL steps sequentially
        - Processes 100 Steam apps
        - Handles all error cases internally
        - Returns comprehensive test results
        """,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
    

    # ========================================================================
    # TASK DEPENDENCIES - OPTION 2: COMPLETE PIPELINE (RECOMMENDED)
    # ========================================================================
    
    # Use the complete pipeline approach for proper data flow
    start_test_pipeline >> run_complete_test_pipeline >> end_test_pipeline

# ============================================================================
# DAG DOCUMENTATION
# ============================================================================

dag.doc_md = """
# Test Steam Games ETL Pipeline

## Overview
This DAG orchestrates the test ETL (Extract, Transform, Load) pipeline for Steam games data with limited scope.

## Test Configuration

- **Scope**: 100 Steam apps (vs full dataset)
- **API Delay**: 0.1s between requests (vs 0.5s in production)
- **Output**: Test directory (`data/test_output/`)
- **Database**: Test collection (`steam_games_test.steam_game_details_test`)
- **Schedule**: Manual trigger only (no automatic scheduling)

## Pipeline Steps

### 1. Test Extraction (`test_extract_steam_data`)
- Fetches Steam app list and limits to 100 apps
- Extracts detailed information for test apps
- Uses faster API delays for testing
- Stores raw data for processing

### 2. Test Processing (`test_process_steam_data`)
- Cleans and standardizes raw test data
- Applies business rules and filters
- Generates statistics and aggregations
- Prepares data for storage

### 3. Test Storage (`test_save_to_json`)
- Saves processed data to test JSON files
- Creates test-specific filenames
- Generates test statistics files
- Uses test output directory

### 4. Test Loading (`test_load_to_mongodb`)
- Loads processed data into MongoDB test collection
- Uses test database to avoid conflicts
- Handles batch insertion for performance
- Provides insertion statistics

## Usage

### Manual Trigger
```bash
airflow dags trigger test_steam_games_etl_pipeline
```

### Testing Individual Steps
The DAG supports both approaches:
1. **Individual Tasks**: Each step runs as a separate task (current setup)
2. **Complete Pipeline**: All steps run in a single task (commented out)

### Monitoring
- Email notifications on failures
- Task completion logging
- Performance metrics tracking
- Error handling and recovery

## Configuration
- Uses environment variables for sensitive data
- YAML configuration for application settings
- Test-specific settings for faster execution
- Separate test database/collection to avoid conflicts

## Differences from Production DAG

| Aspect | Test DAG | Production DAG |
|--------|----------|----------------|
| Scope | 100 apps | Full dataset |
| API Delay | 0.1s | 0.5s |
| Schedule | Manual | Daily at 2 AM |
| Output | Test directory | Production directory |
| Database | Test collection | Production collection |
| Retries | 1-2 | 2-3 |
| Retry Delay | 1-5 min | 5-10 min |
"""

# Export the DAG
globals()[dag_config['dag_id']] = dag
