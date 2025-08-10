#!/usr/bin/env python3
"""
Steam Games Data Processor - Simple Docker Entrypoint
============================================================================
Essential initialization for the Docker application:
- Wait for databases to be ready
- Initialize Airflow
- Create admin user
"""

import os
import sys
import time
import subprocess
from typing import Optional

# ============================================================================
# CONFIGURATION
# ============================================================================

def get_env_var(key: str, default: str = None) -> str:
    """Get environment variable with default."""
    return os.getenv(key, default)

# Database Configuration
POSTGRES_USER = get_env_var('POSTGRES_USER', 'airflow')
POSTGRES_PASSWORD = get_env_var('POSTGRES_PASSWORD', 'airflow')
POSTGRES_DB = get_env_var('POSTGRES_DB', 'airflow')
POSTGRES_HOST = get_env_var('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = get_env_var('POSTGRES_PORT', '5432')

# MongoDB Configuration
MONGODB_ROOT_USERNAME = get_env_var('MONGODB_ROOT_USERNAME', 'admin')
MONGODB_ROOT_PASSWORD = get_env_var('MONGODB_ROOT_PASSWORD', 'admin123')
MONGODB_DATABASE_NAME = get_env_var('MONGODB_DATABASE_NAME', 'steam_games')

# Airflow Configuration
AIRFLOW_ADMIN_USER = get_env_var('AIRFLOW_ADMIN_USER', 'admin')
AIRFLOW_ADMIN_PASSWORD = get_env_var('AIRFLOW_ADMIN_PASSWORD', 'admin')
AIRFLOW_ADMIN_EMAIL = get_env_var('AIRFLOW_ADMIN_EMAIL', 'admin@example.com')

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def log(message: str):
    """Simple logging."""
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")

def run_command(command: list, check: bool = True) -> subprocess.CompletedProcess:
    """Run shell command with error handling."""
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=check)
        return result
    except subprocess.CalledProcessError as e:
        log(f"Command failed: {' '.join(command)}")
        if check:
            raise
        return e

def wait_for_postgres(max_attempts: int = 30):
    """Wait for PostgreSQL to be ready."""
    log("Waiting for PostgreSQL...")
    
    for attempt in range(max_attempts):
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                database=POSTGRES_DB,
                connect_timeout=5
            )
            conn.close()
            log("PostgreSQL is ready!")
            return True
        except Exception:
            pass
        
        log(f"Attempt {attempt + 1}/{max_attempts}: PostgreSQL not ready, waiting 10 seconds...")
        time.sleep(10)
    
    log("PostgreSQL did not become ready")
    return False

def wait_for_mongodb(max_attempts: int = 30):
    """Wait for MongoDB to be ready."""
    log("Waiting for MongoDB...")
    
    for attempt in range(max_attempts):
        try:
            import pymongo
            client = pymongo.MongoClient(
                f"mongodb://{MONGODB_ROOT_USERNAME}:{MONGODB_ROOT_PASSWORD}@mongodb:27017/",
                serverSelectionTimeoutMS=5000
            )
            client.admin.command('ping')
            client.close()
            log("MongoDB is ready!")
            return True
        except Exception:
            pass
        
        log(f"Attempt {attempt + 1}/{max_attempts}: MongoDB not ready, waiting 10 seconds...")
        time.sleep(10)
    
    log("MongoDB did not become ready")
    return False

# ============================================================================
# AIRFLOW INITIALIZATION
# ============================================================================

def init_airflow():
    """Initialize Airflow database and create admin user."""
    log("Initializing Airflow...")
    
    try:
        # Initialize database
        log("Initializing Airflow database...")
        run_command(["airflow", "db", "init"])
        
        # Create admin user
        log(f"Creating Airflow admin user: {AIRFLOW_ADMIN_USER}")
        run_command([
            "airflow", "users", "create",
            "--username", AIRFLOW_ADMIN_USER,
            "--password", AIRFLOW_ADMIN_PASSWORD,
            "--firstname", "Admin",
            "--lastname", "User",
            "--role", "Admin",
            "--email", AIRFLOW_ADMIN_EMAIL
        ])
        
        # Create data directories
        log("Creating data directories...")
        data_dirs = ["/app/data/raw", "/app/data/processed", "/app/data/logs"]
        for directory in data_dirs:
            os.makedirs(directory, exist_ok=True)
        
        log("Airflow initialization completed!")
        
    except Exception as e:
        log(f"Airflow initialization failed: {str(e)}")
        raise

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main entrypoint function."""
    log("Starting Steam Games Data Processor initialization...")
    
    try:
        # Change to app directory
        os.chdir("/app")
        
        # Wait for databases
        if not wait_for_postgres():
            sys.exit(1)
        
        if not wait_for_mongodb():
            sys.exit(1)
        
        # Initialize Airflow
        init_airflow()
        
        log("Initialization completed successfully!")
        log(f"Admin user: {AIRFLOW_ADMIN_USER}")
        log(f"Admin password: {AIRFLOW_ADMIN_PASSWORD}")
        log(f"Web UI: http://localhost:{get_env_var('AIRFLOW__WEBSERVER__WEB_SERVER_PORT', '8080')}")
        
    except Exception as e:
        log(f"Initialization failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
