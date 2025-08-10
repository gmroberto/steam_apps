# Steam Games Data Processing Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for processing Steam games data using Apache Airflow, MongoDB, and Docker. This project extracts data from the Steam API, processes it, and stores it in a MongoDB database with full orchestration through Airflow DAGs.

## 🎯 Project Overview

This project provides a complete solution for:
- **Data Extraction**: Fetching Steam games data from the Steam API
- **Data Processing**: Transforming and cleaning the extracted data
- **Data Loading**: Storing processed data in MongoDB
- **Orchestration**: Managing the entire pipeline with Apache Airflow
- **Retry Logic**: Handling failed extractions with intelligent retry mechanisms
- **Configuration Management**: Enterprise-grade configuration with environment variable support

## 🏗️ Architecture

The project follows a modular, microservices-based architecture:

```
steam_apps/
├── airflow/                    # Airflow orchestration
│   ├── dags/                  # DAG definitions
│   └── functions/             # ETL functions for Airflow
├── config/                    # Configuration management
│   ├── config.yml            # Application configuration
│   ├── env.template          # Environment variables template
│   └── requirements.txt      # Python dependencies
├── src/                      # Core application code
│   ├── extractors/           # Data extraction modules
│   ├── processors/           # Data processing modules
│   ├── loaders/              # Data loading modules
│   └── utils/                # Utility functions
├── tests/                    # Test suite
├── scripts/                  # Utility scripts
├── docker-compose.yml        # Docker orchestration
└── Dockerfile               # Application container
```

### Core Components

1. **SteamApiClient** (`src/extractors/steam_api_client.py`)
   - Pure HTTP client for Steam API interactions
   - Handles rate limiting, retries, and error handling
   - Configurable timeouts and delays

2. **SteamDataExtractor** (`src/extractors/steam_data_extractor.py`)
   - Pure data extraction without processing
   - Batch processing capabilities
   - Progress tracking and failure handling

3. **SteamDataProcessor** (`src/processors/steam_data_processor.py`)
   - Business logic for data processing
   - Single-responsibility functions
   - Comprehensive error handling

4. **MongoDBLoader** (`src/loaders/mongodb_loader.py`)
   - Data loading into MongoDB
   - Batch insertion with configurable chunk sizes
   - Connection management and error recovery

5. **FileManager** (`src/utils/file_operations.py`)
   - File I/O operations
   - JSON serialization/deserialization
   - Data persistence utilities

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)
- Git

### 1. Clone the Repository

```bash
git clone <repository-url>
cd steam_apps
```

### 2. Environment Setup

```bash
# Copy environment template
cp config/env.template .env

# Edit .env file with your configuration
# See Configuration section for details
```

### 3. Start with Docker Compose

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

### 4. Access Services

- **Airflow Web UI**: http://localhost:8080
  - Username: `admin`
  - Password: `admin`
- **MongoDB**: `mongodb://localhost:27017`
- **PostgreSQL**: `postgresql://localhost:5432`

### 5. Run the Pipeline

1. Open Airflow Web UI
2. Navigate to DAGs
3. Enable the `steam_games_etl_pipeline` DAG
4. Trigger the DAG manually or wait for scheduled execution

## ⚙️ Configuration

### Environment Variables

The project uses a two-layer configuration system:

1. **Environment Variables** (`.env` file) - Sensitive data
2. **YAML Configuration** (`config/config.yml`) - Application settings

#### Required Environment Variables

```bash
# MongoDB Configuration
MONGODB_CONNECTION_STRING=mongodb://admin:admin123@mongodb:27017/steam_games?authSource=admin
MONGODB_DATABASE_NAME=steam_games
MONGODB_COLLECTION_NAME=steam_game_details

# PostgreSQL Configuration (Airflow)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Airflow Configuration
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
```

#### Optional Environment Variables

```bash
# Steam API Configuration
STEAM_API_TIMEOUT=30
STEAM_API_DELAY=0.5

# MongoDB Performance Settings
MONGODB_CHUNK_SIZE=1000
MONGODB_SERVER_TIMEOUT=5000

# Test Environment
TEST_ENVIRONMENT=false
TEST_MONGODB_DATABASE_NAME=steam_games_test
```

### Configuration File

The `config/config.yml` file contains all application settings with environment variable placeholder support:

```yaml
steam_api_client:
  timeout: "${STEAM_API_TIMEOUT:30}"
  delay: "${STEAM_API_DELAY:0.5}"
  retry:
    attempts: 8
    initial_delay: 1
    backoff_multiplier: 2

mongodb:
  connection_string: "${MONGODB_CONNECTION_STRING}"
  database_name: "${MONGODB_DATABASE_NAME}"
  chunk_size: "${MONGODB_CHUNK_SIZE:1000}"
```

## 📊 Data Pipeline

### ETL Process Flow

1. **Extract**
   - Fetch complete Steam app list
   - Extract detailed information for each app
   - Handle API rate limiting and retries

2. **Transform**
   - Clean and validate data
   - Transform data structure
   - Handle missing or invalid data

3. **Load**
   - Batch insert into MongoDB
   - Handle connection errors
   - Provide insertion statistics

4. **Retry**
   - Identify failed extractions
   - Retry with exponential backoff
   - Track non-existent apps

### Airflow DAG Structure

```
start_pipeline
    ↓
extract_steam_data
    ↓
process_steam_data
    ↓
save_data_to_json
    ↓
retry_failed_apps
    ↓
insert_to_mongodb
    ↓
end_pipeline
```

## 🧪 Testing

### Run Tests

```bash
# Run all tests
python -m pytest tests/

# Run specific test file
python -m pytest tests/test_etl_pipeline.py

# Run with coverage
python -m pytest --cov=src tests/
```

### Test Environment

The project includes a separate test environment configuration:

```bash
# Enable test environment
TEST_ENVIRONMENT=true

# Test-specific settings
TEST_MONGODB_DATABASE_NAME=steam_games_test
TEST_STEAM_API_DELAY=0.1
TEST_MONGODB_CHUNK_SIZE=100
```

## 🔧 Development

### Local Development Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r config/requirements.txt

# Set up environment variables
cp config/env.template .env
# Edit .env file

# Run application locally
python scripts/entrypoint.py
```

### Code Structure

The codebase follows clean architecture principles:

- **Single Responsibility**: Each class/function has one clear purpose
- **Dependency Injection**: Configuration is injected rather than hardcoded
- **Error Handling**: Comprehensive error handling with proper logging
- **Type Hints**: Full type annotation support
- **Documentation**: Comprehensive docstrings and comments

### Key Design Patterns

1. **Factory Pattern**: Configuration loading
2. **Strategy Pattern**: Different processing strategies
3. **Observer Pattern**: Progress callbacks
4. **Retry Pattern**: Exponential backoff for API calls

## 📈 Monitoring and Logging

### Airflow Monitoring

- **Web UI**: Real-time DAG monitoring
- **Task Logs**: Detailed execution logs
- **Metrics**: Performance and success rates
- **Alerts**: Email notifications on failures

### Application Logging

```python
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

### MongoDB Monitoring

- **Connection Health**: Health checks for database connectivity
- **Insertion Statistics**: Batch processing metrics
- **Error Tracking**: Failed insertion logging

## 🔒 Security

### Environment Variables

- Sensitive data stored in `.env` file
- `.env` file excluded from version control
- Environment variable validation

### Database Security

- MongoDB authentication enabled
- PostgreSQL authentication for Airflow
- Network isolation with Docker networks

### API Security

- Rate limiting to respect Steam API limits
- Request timeouts to prevent hanging connections
- Retry logic with exponential backoff

## 🚀 Deployment

### Production Deployment

1. **Environment Setup**
   ```bash
   # Production environment variables
   ENVIRONMENT=production
   DEBUG=false
   MONGODB_DROP_COLLECTION=false
   ```

2. **Docker Deployment**
   ```bash
   # Build production image
   docker build -t steam-games-processor:latest .

   # Deploy with docker-compose
   docker-compose -f docker-compose.prod.yml up -d
   ```

3. **Airflow Configuration**
   - Set `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True`
   - Configure email notifications
   - Set up monitoring and alerting

### Scaling

- **Horizontal Scaling**: Multiple Airflow workers
- **Vertical Scaling**: Increase container resources
- **Database Scaling**: MongoDB replica sets
- **Load Balancing**: Reverse proxy for Airflow UI

## 📚 API Reference

### SteamApiClient

```python
from src.extractors.steam_api_client import SteamApiClient

client = SteamApiClient(timeout=30, delay=0.5)
apps = client.get_app_list()
details = client.get_app_details(app_id)
```

### SteamDataExtractor

```python
from src.extractors.steam_data_extractor import SteamDataExtractor

extractor = SteamDataExtractor()
app_list = extractor.extract_app_list()
details = extractor.extract_multiple_app_details(app_ids)
```

### MongoDBLoader

```python
from src.loaders.mongodb_loader import MongoDBLoader

loader = MongoDBLoader()
loader.insert_data(data, chunk_size=1000)
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

### Code Style

- Follow PEP 8 guidelines
- Use type hints
- Add comprehensive docstrings
- Write unit tests for new features

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

### Common Issues

1. **MongoDB Connection Issues**
   - Check connection string format
   - Verify network connectivity
   - Ensure authentication credentials

2. **Steam API Rate Limiting**
   - Increase delay between requests
   - Check API endpoint availability
   - Verify request format

3. **Airflow DAG Failures**
   - Check task logs in Airflow UI
   - Verify environment variables
   - Ensure all dependencies are installed

### Getting Help

- Check the logs: `docker-compose logs -f`
- Review Airflow task logs in the web UI
- Check MongoDB connection: `docker-compose exec mongodb mongosh`
- Verify configuration: `python -c "from config.config_manager import get_config; print(get_config())"`

## 🔄 Changelog

### Version 1.0.0
- Initial release with complete ETL pipeline
- Airflow orchestration
- MongoDB integration
- Comprehensive configuration management
- Docker containerization
- Full test suite

---

**Note**: This project is designed for educational and research purposes. Please respect Steam's API terms of service and rate limits when using this pipeline.
