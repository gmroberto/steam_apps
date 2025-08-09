#!/usr/bin/env python3
"""
MongoDB Insertion Script for Steam App Details

This script reads steam_apps_details.json and inserts the data into MongoDB.
Configure the parameters in the CONFIGURATION section below or use environment variables.
"""

import json
import logging
import os
import sys
from typing import Dict, List, Any, Iterator
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConnectionFailure, ServerSelectionTimeoutError
import time
from src.utils.config_manager import get_config

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv not installed, environment variables will be read from system
    pass

# ============================================================================
# CONFIGURATION - Unified system: config.yml with environment variable placeholders
# ============================================================================

# MongoDB configuration (loaded from config.yml with env var placeholders)
CONNECTION_STRING = get_config('mongodb.connection_string', 'mongodb://localhost:27017/')
DATABASE_NAME = get_config('mongodb.database_name', 'steam_games')
COLLECTION_NAME = get_config('mongodb.collection_name', 'steam_game_details')

# Performance and file settings (loaded from config.yml with env var placeholders)
CHUNK_SIZE = get_config('mongodb.chunk_size', 1000)
SERVER_SELECTION_TIMEOUT = get_config('mongodb.server_timeout', 5000)
INPUT_FILE = get_config('mongodb.input_file', 'steam_apps_details.json')
LOG_FILE_NAME = get_config('mongodb.log_file', 'mongodb_insert.log')

# Safety setting (loaded from config.yml with env var placeholders)
DROP_COLLECTION = get_config('mongodb.drop_collection', False)

# ============================================================================


class MongoDBInserter:
    """Handles MongoDB insertion operations for Steam app details."""
    
    def __init__(self, connection_string: str, database_name: str = "steam_data", 
                 collection_name: str = "app_details"):
        """
        Initialize MongoDB connection.
        
        Args:
            connection_string: MongoDB connection string
            database_name: Name of the database to use
            collection_name: Name of the collection to use
        """
        self.connection_string = connection_string
        self.database_name = database_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
        
    def connect(self) -> bool:
        """
        Establish connection to MongoDB.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.client = MongoClient(self.connection_string, serverSelectionTimeoutMS=SERVER_SELECTION_TIMEOUT)
            # Test the connection
            self.client.server_info()
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            logging.info(f"Successfully connected to MongoDB: {self.database_name}.{self.collection_name}")
            return True
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error connecting to MongoDB: {e}")
            return False
    
    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logging.info("MongoDB connection closed")
    
    def insert_documents_batch(self, documents: List[Dict[str, Any]]) -> tuple[int, int]:
        """
        Insert a batch of documents into MongoDB.
        
        Args:
            documents: List of documents to insert
            
        Returns:
            tuple: (successful_inserts, failed_inserts)
        """
        if not documents:
            return 0, 0
            
        try:
            result = self.collection.insert_many(documents, ordered=False)
            successful = len(result.inserted_ids)
            failed = len(documents) - successful
            logging.info(f"Batch insert: {successful} successful, {failed} failed")
            return successful, failed
            
        except BulkWriteError as e:
            successful = e.details.get('nInserted', 0)
            failed = len(documents) - successful
            logging.warning(f"Bulk write error: {successful} successful, {failed} failed")
            for error in e.details.get('writeErrors', []):
                logging.warning(f"Write error: {error}")
            return successful, failed
            
        except Exception as e:
            logging.error(f"Error inserting batch: {e}")
            return 0, len(documents)


def load_json_data(file_path: str) -> Dict[str, Any]:
    """
    Load JSON data from file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Dict containing the loaded JSON data
        
    Raises:
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If file contains invalid JSON
    """
    logging.info(f"Loading JSON data from {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logging.info(f"Successfully loaded {len(data)} app records")
        return data
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in file {file_path}: {e}")
        raise
    except Exception as e:
        logging.error(f"Error loading file {file_path}: {e}")
        raise


def prepare_documents(steam_data: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """
    Convert Steam data to MongoDB documents.
    
    Args:
        steam_data: Dictionary with app_id as key and app details as value
        
    Yields:
        Dict: MongoDB document with app_id as a field
    """
    for app_id, app_details in steam_data.items():
        # Add the app_id as a field in the document
        document = {
            'app_id': int(app_id),
            **app_details
        }
        yield document


def chunk_data(data_iterator: Iterator[Dict[str, Any]], chunk_size: int) -> Iterator[List[Dict[str, Any]]]:
    """
    Split data into chunks of specified size.
    
    Args:
        data_iterator: Iterator of documents
        chunk_size: Size of each chunk
        
    Yields:
        List of documents (chunk)
    """
    chunk = []
    for item in data_iterator:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    
    # Yield remaining items if any
    if chunk:
        yield chunk


def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_FILE_NAME)
        ]
    )


def validate_configuration():
    """
    Validate the configuration parameters.
    
    Raises:
        ValueError: If any configuration parameter is invalid
    """
    if not CONNECTION_STRING:
        raise ValueError("CONNECTION_STRING cannot be empty")
    
    if CHUNK_SIZE <= 0:
        raise ValueError("CHUNK_SIZE must be greater than 0")
    
    if not INPUT_FILE:
        raise ValueError("INPUT_FILE cannot be empty")
    
    if not DATABASE_NAME:
        raise ValueError("DATABASE_NAME cannot be empty")
    
    if not COLLECTION_NAME:
        raise ValueError("COLLECTION_NAME cannot be empty")


def main():
    """Main execution function."""
    setup_logging()
    
    # Validate configuration
    try:
        validate_configuration()
        logging.info("Configuration validation passed")
        logging.info(f"Connection String: {CONNECTION_STRING}")
        logging.info(f"Chunk Size: {CHUNK_SIZE}")
        logging.info(f"Input File: {INPUT_FILE}")
        logging.info(f"Database: {DATABASE_NAME}")
        logging.info(f"Collection: {COLLECTION_NAME}")
        logging.info(f"Drop Collection: {DROP_COLLECTION}")
    except ValueError as e:
        logging.error(f"Configuration error: {e}")
        sys.exit(1)
    
    # Initialize MongoDB inserter
    inserter = MongoDBInserter(
        connection_string=CONNECTION_STRING,
        database_name=DATABASE_NAME,
        collection_name=COLLECTION_NAME
    )
    
    try:
        # Connect to MongoDB
        if not inserter.connect():
            logging.error("Failed to establish MongoDB connection")
            sys.exit(1)
        
        # Drop collection if requested
        if DROP_COLLECTION:
            logging.warning("Dropping existing collection...")
            inserter.collection.drop()
            logging.info("Collection dropped successfully")
        
        # Load JSON data
        try:
            steam_data = load_json_data(INPUT_FILE)
        except Exception as e:
            logging.error(f"Failed to load JSON data: {e}")
            sys.exit(1)
        
        # Process and insert data
        total_documents = len(steam_data)
        total_successful = 0
        total_failed = 0
        
        logging.info(f"Starting insertion of {total_documents} documents in chunks of {CHUNK_SIZE}")
        start_time = time.time()
        
        # Convert to documents and process in chunks
        documents = prepare_documents(steam_data)
        chunks = chunk_data(documents, CHUNK_SIZE)
        
        chunk_count = 0
        for chunk in chunks:
            chunk_count += 1
            logging.info(f"Processing chunk {chunk_count} ({len(chunk)} documents)")
            
            successful, failed = inserter.insert_documents_batch(chunk)
            total_successful += successful
            total_failed += failed
            
            # Progress update
            processed = total_successful + total_failed
            progress = (processed / total_documents) * 100
            logging.info(f"Progress: {processed}/{total_documents} ({progress:.1f}%)")
        
        # Final statistics
        end_time = time.time()
        duration = end_time - start_time
        
        logging.info("=" * 50)
        logging.info("INSERTION COMPLETED")
        logging.info(f"Total documents processed: {total_successful + total_failed}")
        logging.info(f"Successfully inserted: {total_successful}")
        logging.info(f"Failed insertions: {total_failed}")
        logging.info(f"Success rate: {(total_successful / (total_successful + total_failed) * 100):.2f}%")
        logging.info(f"Total time: {duration:.2f} seconds")
        logging.info(f"Average rate: {(total_successful + total_failed) / duration:.2f} documents/second")
        logging.info("=" * 50)
        
        if total_failed > 0:
            logging.warning(f"{total_failed} documents failed to insert. Check logs for details.")
            sys.exit(1)
        else:
            logging.info("All documents inserted successfully!")
            
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        inserter.close()


if __name__ == "__main__":
    main()
