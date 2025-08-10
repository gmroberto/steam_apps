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

def wait_for_postgres(max_attempts: int = 60):
    """Wait for PostgreSQL to be ready."""
    log("Waiting for PostgreSQL to be ready...")
    
    for attempt in range(max_attempts):
        try:
            import psycopg2
            log(f"Attempt {attempt + 1}/{max_attempts}: Trying to connect to PostgreSQL...")
            
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                database=POSTGRES_DB,
                connect_timeout=10
            )
            
            # Test the connection with a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            
            log("PostgreSQL is ready and accepting connections!")
            return True
            
        except Exception as e:
            log(f"Attempt {attempt + 1}/{max_attempts}: PostgreSQL not ready - {str(e)}")
            if attempt < max_attempts - 1:  # Don't sleep on the last attempt
                log("Waiting 15 seconds before next attempt...")
                time.sleep(15)
    
    log("ERROR: PostgreSQL did not become ready after all attempts")
    return False

def wait_for_mongodb(max_attempts: int = 60):
    """Wait for MongoDB to be ready."""
    log("Waiting for MongoDB to be ready...")
    
    for attempt in range(max_attempts):
        try:
            import pymongo
            log(f"Attempt {attempt + 1}/{max_attempts}: Trying to connect to MongoDB...")
            
            client = pymongo.MongoClient(
                f"mongodb://{MONGODB_ROOT_USERNAME}:{MONGODB_ROOT_PASSWORD}@mongodb:27017/",
                serverSelectionTimeoutMS=10000
            )
            client.admin.command('ping')
            client.close()
            
            log("MongoDB is ready and accepting connections!")
            return True
            
        except Exception as e:
            log(f"Attempt {attempt + 1}/{max_attempts}: MongoDB not ready - {str(e)}")
            if attempt < max_attempts - 1:  # Don't sleep on the last attempt
                log("Waiting 15 seconds before next attempt...")
                time.sleep(15)
    
    log("ERROR: MongoDB did not become ready after all attempts")
    return False

# ============================================================================
# AIRFLOW INITIALIZATION
# ============================================================================

def init_airflow():
    """Initialize Airflow database and create admin user."""
    log("Initializing Airflow...")
    
    try:
        # Initialize database
        log("Step 3.1: Initializing Airflow database...")
        result = run_command(["airflow", "db", "init"], check=False)
        if result.returncode != 0:
            log(f"Database initialization output: {result.stdout}")
            log(f"Database initialization errors: {result.stderr}")
            if "already exists" not in result.stderr:
                raise Exception(f"Airflow database initialization failed: {result.stderr}")
            else:
                log("Database already exists, continuing...")
        else:
            log("Airflow database initialized successfully!")
        
        # Create admin user
        log(f"Step 3.2: Creating Airflow admin user: {AIRFLOW_ADMIN_USER}")
        result = run_command([
            "airflow", "users", "create",
            "--username", AIRFLOW_ADMIN_USER,
            "--password", AIRFLOW_ADMIN_PASSWORD,
            "--firstname", "Admin",
            "--lastname", "User",
            "--role", "Admin",
            "--email", AIRFLOW_ADMIN_EMAIL
        ], check=False)
        
        if result.returncode != 0:
            log(f"User creation output: {result.stdout}")
            log(f"User creation errors: {result.stderr}")
            if "already exists" not in result.stderr:
                raise Exception(f"Airflow user creation failed: {result.stderr}")
            else:
                log("Admin user already exists, continuing...")
        else:
            log("Airflow admin user created successfully!")
        
        # Create data directories
        log("Step 3.3: Creating data directories...")
        data_dirs = ["/app/data/raw", "/app/data/processed", "/app/data/logs"]
        for directory in data_dirs:
            os.makedirs(directory, exist_ok=True)
            log(f"Created directory: {directory}")
        
        log("Airflow initialization completed successfully!")
        
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
        log("Changed to /app directory")
        
        # Wait for PostgreSQL first (Airflow depends on it)
        log("Step 1: Waiting for PostgreSQL to be ready...")
        if not wait_for_postgres():
            log("CRITICAL: PostgreSQL is not available. Cannot proceed with Airflow initialization.")
            sys.exit(1)
        
        # Wait for MongoDB (for Steam data storage)
        log("Step 2: Waiting for MongoDB to be ready...")
        if not wait_for_mongodb():
            log("WARNING: MongoDB is not available. Steam data storage will not work.")
            log("Continuing with Airflow initialization...")
        
        # Initialize Airflow
        log("Step 3: Initializing Airflow...")
        init_airflow()
        
        log("=" * 60)
        log("INITIALIZATION COMPLETED SUCCESSFULLY!")
        log("=" * 60)
        log(f"Airflow Admin User: {AIRFLOW_ADMIN_USER}")
        log(f"Airflow Admin Password: {AIRFLOW_ADMIN_PASSWORD}")
        log(f"Airflow Web UI: http://localhost:{get_env_var('AIRFLOW__WEBSERVER__WEB_SERVER_PORT', '8080')}")
        log(f"PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}")
        log(f"MongoDB: mongodb:27017")
        log("=" * 60)
        
    except Exception as e:
        log(f"CRITICAL ERROR: Initialization failed: {str(e)}")
        log("Please check the logs above for more details.")
        sys.exit(1)

if __name__ == "__main__":
    main()
