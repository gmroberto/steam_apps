#!/usr/bin/env python3
"""
Test ETL Pipeline for Steam Games Data

This script mimics the airflow_etl_functions but runs for only 100 documents
to test the complete ETL pipeline: API calls, processing, and MongoDB insertion.
"""

import os
import sys
import time
from datetime import datetime
from typing import Dict, Any, List

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Import ETL functions from src
from src.extractors.steam_data_extractor import SteamDataExtractor
from src.processors.steam_data_transformer import SteamDataTransformer
from src.utils.json_saver import JsonSaver
from src.loaders.mongodb_loader import MongoDBInserter
from config.config_manager import get_config

# Load environment variables if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ============================================================================
# TEST CONFIGURATION
# ============================================================================

# Test configuration - limit to 100 documents
TEST_CONFIG = {
    'max_apps': 100,
    'delay_between_requests': 0.1,  # Faster for testing
    'output_dir': 'data/test_output',
    'test_filename': 'test_steam_data.json'
}

# ============================================================================
# TEST ETL FUNCTIONS
# ============================================================================

def test_extract_steam_data() -> Dict[str, Any]:
    """
    Test Step 1: Extract Steam data (limited to 100 apps).
    
    Returns:
        Dict[str, Any]: Extraction results with metadata
    """
    print("=" * 60)
    print("TESTING: Steam API Extraction (100 apps)")
    print("=" * 60)
    
    try:
        # Initialize extractor with test configuration
        extractor = SteamDataExtractor(
            timeout=get_config('steam_api_client.timeout', 30),
            delay=TEST_CONFIG['delay_between_requests']
        )
        
        # Extract app list
        print("Extracting Steam app list...")
        app_list = extractor.extract_app_list()
        
        if not app_list:
            return {"status": "error", "message": "Failed to extract app list"}
        
        # Limit to 100 apps for testing
        test_apps = app_list[:TEST_CONFIG['max_apps']]
        print(f"Limited to {len(test_apps)} apps for testing")
        
        # Extract details for test apps
        print(f"Extracting details for {len(test_apps)} apps...")
        app_ids = [app['appid'] for app in test_apps]
        
        extracted_details, failed_app_ids, non_existent_apps = extractor.extract_multiple_app_details(
            app_ids, 
            delay_between_requests=TEST_CONFIG['delay_between_requests']
        )
        
        # Prepare results
        results = {
            'status': 'success',
            'total_apps_requested': len(test_apps),
            'successfully_extracted': len(extracted_details),
            'failed_extractions': len(failed_app_ids),
            'non_existent_apps': len(non_existent_apps),
            'extracted_data': extracted_details,
            'failed_app_ids': failed_app_ids,
            'non_existent_app_ids': non_existent_apps,
            'timestamp': datetime.now().isoformat()
        }
        
        print(f"✅ Extraction completed:")
        print(f"   - Successfully extracted: {results['successfully_extracted']}")
        print(f"   - Failed extractions: {results['failed_extractions']}")
        print(f"   - Non-existent apps: {results['non_existent_apps']}")
        
        return results
        
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        return {"status": "error", "message": str(e)}


def test_process_steam_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Test Step 2: Process and transform Steam data.
    
    Args:
        raw_data (Dict[str, Any]): Raw data from extraction
        
    Returns:
        Dict[str, Any]: Processing results with metadata
    """
    print("=" * 60)
    print("TESTING: Steam Data Processing")
    print("=" * 60)
    
    try:
        if raw_data.get('status') != 'success':
            return {"status": "error", "message": "No valid data to process"}
        
        # Initialize transformer
        transformer = SteamDataTransformer()
        
        # Process the raw data
        print("Processing raw Steam data...")
        processed_data = transformer.process_raw_app_data(raw_data['extracted_data'])
        
        # Generate statistics
        print("Generating statistics...")
        statistics = transformer.aggregate_statistics(processed_data)
        
        # Prepare results
        results = {
            'status': 'success',
            'processed_apps': len(processed_data),
            'statistics': statistics,
            'processed_data': processed_data,
            'timestamp': datetime.now().isoformat()
        }
        
        print(f"✅ Processing completed:")
        print(f"   - Processed apps: {results['processed_apps']}")
        print(f"   - Game types: {statistics.get('game_types', {}).get('count', 0)}")
        print(f"   - Free games: {statistics.get('free_games', {}).get('count', 0)}")
        
        return results
        
    except Exception as e:
        print(f"❌ Processing failed: {e}")
        return {"status": "error", "message": str(e)}


def test_save_to_json(processed_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Test Step 3: Save processed data to JSON.
    
    Args:
        processed_data (Dict[str, Any]): Processed data to save
        
    Returns:
        Dict[str, Any]: Saving results with file paths
    """
    print("=" * 60)
    print("TESTING: Data Saving to JSON")
    print("=" * 60)
    
    try:
        if processed_data.get('status') != 'success':
            return {"status": "error", "message": "No valid data to save"}
        
        # Initialize saver with test output directory
        saver = JsonSaver(
            base_output_dir=TEST_CONFIG['output_dir'],
            indent=2,
            ensure_ascii=False
        )
        
        # Save the processed data
        print(f"Saving data to {TEST_CONFIG['output_dir']}...")
        file_path = saver.save_processed_data(
            processed_data['processed_data'],
            filename=TEST_CONFIG['test_filename'],
            add_timestamp=True,
            add_metadata=True,
            create_backup=False
        )
        
        # Save statistics separately
        stats_file_path = saver.save_statistics(
            processed_data['statistics'],
            filename='test_steam_statistics.json'
        )
        
        # Prepare results
        results = {
            'status': 'success',
            'main_data_file': file_path,
            'statistics_file': stats_file_path,
            'output_directory': TEST_CONFIG['output_dir'],
            'timestamp': datetime.now().isoformat()
        }
        
        print(f"✅ Saving completed:")
        print(f"   - Main data file: {file_path}")
        print(f"   - Statistics file: {stats_file_path}")
        
        return results
        
    except Exception as e:
        print(f"❌ Saving failed: {e}")
        return {"status": "error", "message": str(e)}


def test_load_to_mongodb(processed_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Test Step 4: Load processed data into MongoDB.
    
    Args:
        processed_data (Dict[str, Any]): Processed data to load
        
    Returns:
        Dict[str, Any]: Loading results with statistics
    """
    print("=" * 60)
    print("TESTING: MongoDB Data Loading")
    print("=" * 60)
    
    try:
        if processed_data.get('status') != 'success':
            return {"status": "error", "message": "No valid data to load"}
        
        # Get MongoDB configuration
        connection_string = get_config('mongodb.connection_string', 'mongodb://localhost:27017/')
        database_name = get_config('mongodb.database_name', 'steam_games_test')
        collection_name = get_config('mongodb.collection_name', 'steam_game_details_test')
        
        # Initialize MongoDB inserter
        inserter = MongoDBInserter(
            connection_string=connection_string,
            database_name=database_name,
            collection_name=collection_name
        )
        
        # Connect to MongoDB
        print(f"Connecting to MongoDB: {database_name}.{collection_name}")
        if not inserter.connect():
            return {"status": "error", "message": "Failed to connect to MongoDB"}
        
        # Prepare documents for insertion
        documents = []
        for app_id, app_data in processed_data['processed_data'].items():
            if app_id not in ['processing_metadata', 'save_metadata', 'updated_at']:
                doc = app_data.copy()
                doc['_id'] = int(app_id)  # Use app_id as _id
                doc['test_run'] = True
                doc['test_timestamp'] = datetime.now().isoformat()
                documents.append(doc)
        
        # Insert documents in batches
        print(f"Inserting {len(documents)} documents...")
        chunk_size = get_config('mongodb.chunk_size', 100)
        total_inserted = 0
        total_errors = 0
        
        for i in range(0, len(documents), chunk_size):
            batch = documents[i:i + chunk_size]
            inserted, errors = inserter.insert_documents_batch(batch)
            total_inserted += inserted
            total_errors += errors
            print(f"   Batch {i//chunk_size + 1}: {inserted} inserted, {errors} errors")
        
        # Close connection
        inserter.close()
        
        # Prepare results
        results = {
            'status': 'success',
            'total_documents': len(documents),
            'successfully_inserted': total_inserted,
            'insertion_errors': total_errors,
            'database': database_name,
            'collection': collection_name,
            'timestamp': datetime.now().isoformat()
        }
        
        print(f"✅ MongoDB loading completed:")
        print(f"   - Total documents: {results['total_documents']}")
        print(f"   - Successfully inserted: {results['successfully_inserted']}")
        print(f"   - Insertion errors: {results['insertion_errors']}")
        
        return results
        
    except Exception as e:
        print(f"❌ MongoDB loading failed: {e}")
        return {"status": "error", "message": str(e)}


# ============================================================================
# MAIN TEST FUNCTION
# ============================================================================

def run_test_etl_pipeline() -> Dict[str, Any]:
    """
    Run the complete test ETL pipeline for 100 documents.
    
    Returns:
        Dict[str, Any]: Complete test results
    """
    print("🚀 Starting Test ETL Pipeline (100 documents)")
    print("=" * 80)
    
    start_time = time.time()
    test_results = {
        'test_config': TEST_CONFIG,
        'start_time': datetime.now().isoformat(),
        'steps': {}
    }
    
    try:
        # Step 1: Extract data
        extraction_results = test_extract_steam_data()
        test_results['steps']['extraction'] = extraction_results
        
        if extraction_results.get('status') != 'success':
            print("❌ Extraction failed, stopping pipeline")
            return test_results
        
        # Step 2: Process data
        processing_results = test_process_steam_data(extraction_results)
        test_results['steps']['processing'] = processing_results
        
        if processing_results.get('status') != 'success':
            print("❌ Processing failed, stopping pipeline")
            return test_results
        
        # Step 3: Save to JSON
        saving_results = test_save_to_json(processing_results)
        test_results['steps']['saving'] = saving_results
        
        if saving_results.get('status') != 'success':
            print("❌ Saving failed, stopping pipeline")
            return test_results
        
        # Step 4: Load to MongoDB
        loading_results = test_load_to_mongodb(processing_results)
        test_results['steps']['loading'] = loading_results
        
        # Calculate total time
        end_time = time.time()
        total_time = end_time - start_time
        
        test_results.update({
            'end_time': datetime.now().isoformat(),
            'total_duration_seconds': total_time,
            'overall_status': 'success' if all(
                step.get('status') == 'success' 
                for step in test_results['steps'].values()
            ) else 'partial_success'
        })
        
        print("=" * 80)
        print("🎉 Test ETL Pipeline Completed!")
        print(f"⏱️  Total duration: {total_time:.2f} seconds")
        print(f"📊 Overall status: {test_results['overall_status']}")
        print("=" * 80)
        
        return test_results
        
    except Exception as e:
        print(f"❌ Test pipeline failed with exception: {e}")
        test_results.update({
            'end_time': datetime.now().isoformat(),
            'total_duration_seconds': time.time() - start_time,
            'overall_status': 'error',
            'error': str(e)
        })
        return test_results


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    # Run the test pipeline
    results = run_test_etl_pipeline()
    
    # Print summary
    print("\n📋 Test Summary:")
    print(f"   Overall Status: {results['overall_status']}")
    print(f"   Duration: {results.get('total_duration_seconds', 0):.2f} seconds")
    
    for step_name, step_results in results.get('steps', {}).items():
        status = step_results.get('status', 'unknown')
        print(f"   {step_name.title()}: {status}")
    
    # Exit with appropriate code
    if results['overall_status'] == 'success':
        print("\n✅ All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)
