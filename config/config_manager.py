"""
Configuration Loader for Steam Games Data Processor

This module provides utilities to load configuration from config.yml file
with environment variable placeholder support and fallback to default values.

Supports placeholders in YAML like:
  database_url: "${MONGODB_CONNECTION_STRING}"
  timeout: "${MONGODB_SERVER_TIMEOUT:5000}"  # with default value
"""

import os
import re
import yaml
from typing import Dict, Any, Union

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv not installed, environment variables will be read from system
    pass


class ConfigLoader:
    """
    Loads configuration from config.yml file with environment variable placeholder support.
    
    Supports placeholders in YAML like:
    - "${ENV_VAR}" - required environment variable
    - "${ENV_VAR:default}" - environment variable with default value
    - "${ENV_VAR:}" - environment variable with empty string default
    """
    
    def __init__(self, config_file: str = "config.yml"):
        """
        Initialize the configuration loader.
        
        Args:
            config_file (str): Path to the YAML configuration file
        """
        self.config_file = config_file
        self._config = None
        self._env_placeholder_pattern = re.compile(r'\$\{([^}]+)\}')
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from YAML file and resolve environment variable placeholders."""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    raw_config = yaml.safe_load(f) or {}
                # Resolve environment variable placeholders
                self._config = self._resolve_placeholders(raw_config)
            else:
                print(f"Warning: Configuration file '{self.config_file}' not found. Using defaults.")
                self._config = {}
        except yaml.YAMLError as e:
            print(f"Error parsing YAML configuration file: {e}")
            print("Using default configuration.")
            self._config = {}
        except Exception as e:
            print(f"Error loading configuration file: {e}")
            print("Using default configuration.")
            self._config = {}
    
    def _resolve_placeholders(self, obj: Any) -> Any:
        """
        Recursively resolve environment variable placeholders in configuration.
        
        Args:
            obj: Configuration object (dict, list, string, or other)
            
        Returns:
            Configuration object with placeholders resolved
        """
        if isinstance(obj, dict):
            return {key: self._resolve_placeholders(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._resolve_placeholders(item) for item in obj]
        elif isinstance(obj, str):
            return self._resolve_string_placeholders(obj)
        else:
            return obj
    
    def _resolve_string_placeholders(self, text: str) -> Union[str, int, float, bool]:
        """
        Resolve environment variable placeholders in a string.
        
        Supports formats:
        - "${ENV_VAR}" - required environment variable
        - "${ENV_VAR:default}" - environment variable with default value
        - "${ENV_VAR:}" - environment variable with empty string default
        
        Args:
            text: String that may contain placeholders
            
        Returns:
            String with placeholders resolved, with type conversion for numbers and booleans
        """
        def replace_placeholder(match):
            placeholder = match.group(1)
            
            # Check if placeholder has a default value
            if ':' in placeholder:
                env_var, default_value = placeholder.split(':', 1)
            else:
                env_var, default_value = placeholder, None
            
            # Get environment variable value
            env_value = os.getenv(env_var.strip())
            
            if env_value is not None:
                result = env_value
            elif default_value is not None:
                result = default_value
            else:
                raise ValueError(f"Required environment variable '{env_var}' not found and no default provided")
            
            return result
        
        # Replace all placeholders
        try:
            resolved = self._env_placeholder_pattern.sub(replace_placeholder, text)
            
            # If the entire string was a placeholder, try to convert type
            if text.startswith('${') and text.endswith('}') and not self._env_placeholder_pattern.search(resolved):
                return self._convert_type(resolved)
            
            return resolved
        except ValueError as e:
            print(f"Error resolving placeholder in '{text}': {e}")
            return text
    
    def _convert_type(self, value: str) -> Union[str, int, float, bool]:
        """
        Convert string value to appropriate type (int, float, bool, or keep as string).
        
        Args:
            value: String value to convert
            
        Returns:
            Converted value with appropriate type
        """
        if not isinstance(value, str):
            return value
        
        # Try boolean conversion first
        lower_value = value.lower()
        if lower_value in ('true', 'yes', '1', 'on'):
            return True
        elif lower_value in ('false', 'no', '0', 'off', ''):
            return False
        
        # Try integer conversion
        try:
            if '.' not in value and 'e' not in lower_value:
                return int(value)
        except ValueError:
            pass
        
        # Try float conversion
        try:
            return float(value)
        except ValueError:
            pass
        
        # Keep as string
        return value
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key_path (str): Dot-separated path to the configuration value
            default (Any): Default value if key is not found
            
        Returns:
            Any: Configuration value or default
            
        Examples:
            config.get('file_manager.encoding', 'utf-8')
            config.get('steam_api_client.retry.attempts', 8)
        """
        keys = key_path.split('.')
        value = self._config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """
        Get entire configuration section.
        
        Args:
            section (str): Section name
            
        Returns:
            Dict[str, Any]: Section configuration or empty dict
        """
        return self._config.get(section, {})
    
    def reload(self) -> None:
        """Reload configuration from file."""
        self._load_config()


# Global configuration instance
config = ConfigLoader()


def get_config(key_path: str, default: Any = None) -> Any:
    """
    Convenience function to get configuration value.
    
    Args:
        key_path (str): Dot-separated path to the configuration value
        default (Any): Default value if key is not found
        
    Returns:
        Any: Configuration value or default
    """
    return config.get(key_path, default)


def get_config_section(section: str) -> Dict[str, Any]:
    """
    Convenience function to get configuration section.
    
    Args:
        section (str): Section name
        
    Returns:
        Dict[str, Any]: Section configuration or empty dict
    """
    return config.get_section(section)


# ============================================================================
# AIRFLOW CONFIGURATION UTILITIES
# ============================================================================

def get_airflow_config(key_path: str, default: Any = None) -> Any:
    """
    Get Airflow configuration value using dot notation.
    
    Args:
        key_path (str): Dot-separated path to the Airflow configuration value
        default (Any): Default value if key is not found
        
    Returns:
        Any: Airflow configuration value or default
        
    Examples:
        get_airflow_config('core.executor', 'LocalExecutor')
        get_airflow_config('webserver.port', 8080)
    """
    return get_config(f'airflow.{key_path}', default)


def get_dag_config(dag_name: str) -> Dict[str, Any]:
    """
    Get configuration for a specific DAG.
    
    Args:
        dag_name (str): Name of the DAG (e.g., 'steam_games_etl')
        
    Returns:
        Dict[str, Any]: DAG configuration
    """
    return get_config(f'airflow.dags.{dag_name}', {})


def get_airflow_connections() -> Dict[str, str]:
    """
    Get all Airflow connections configuration.
    
    Returns:
        Dict[str, str]: Dictionary of connection names and their values
    """
    return get_config('airflow.connections', {})


def get_airflow_variables() -> Dict[str, Any]:
    """
    Get all Airflow variables configuration.
    
    Returns:
        Dict[str, Any]: Dictionary of variable names and their values
    """
    return get_config('airflow.variables', {})


def get_airflow_database_config() -> Dict[str, Any]:
    """
    Get Airflow database configuration.
    
    Returns:
        Dict[str, Any]: Database configuration
    """
    return get_config('airflow.database', {})


def get_airflow_webserver_config() -> Dict[str, Any]:
    """
    Get Airflow webserver configuration.
    
    Returns:
        Dict[str, Any]: Webserver configuration
    """
    return get_config('airflow.webserver', {})


def get_airflow_scheduler_config() -> Dict[str, Any]:
    """
    Get Airflow scheduler configuration.
    
    Returns:
        Dict[str, Any]: Scheduler configuration
    """
    return get_config('airflow.scheduler', {})


def get_airflow_logging_config() -> Dict[str, Any]:
    """
    Get Airflow logging configuration.
    
    Returns:
        Dict[str, Any]: Logging configuration
    """
    return get_config('airflow.logging', {})


def get_airflow_celery_config() -> Dict[str, Any]:
    """
    Get Airflow Celery configuration (for distributed processing).
    
    Returns:
        Dict[str, Any]: Celery configuration
    """
    return get_config('airflow.celery', {})


def is_celery_enabled() -> bool:
    """
    Check if Celery executor is enabled.
    
    Returns:
        bool: True if Celery executor is enabled
    """
    executor = get_airflow_config('core.executor', 'LocalExecutor')
    return executor == 'CeleryExecutor'


def is_airflow_production() -> bool:
    """
    Check if running in production environment.
    
    Returns:
        bool: True if production environment
    """
    environment = get_config('environment.environment', 'development')
    return environment == 'production'


def get_airflow_home() -> str:
    """
    Get Airflow home directory.
    
    Returns:
        str: Airflow home directory path
    """
    return os.getenv('AIRFLOW_HOME', './airflow')


def get_sql_alchemy_conn() -> str:
    """
    Get SQL Alchemy connection string for Airflow metadata database.
    
    Returns:
        str: SQL Alchemy connection string
    """
    return get_airflow_config('database.sql_alchemy_conn', 
                             'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')


def get_webserver_port() -> int:
    """
    Get Airflow webserver port.
    
    Returns:
        int: Webserver port
    """
    return get_airflow_config('webserver.port', 8080)


def get_secret_key() -> str:
    """
    Get Airflow webserver secret key.
    
    Returns:
        str: Secret key
    """
    return get_airflow_config('webserver.secret_key', 'your-secret-key-here')


def get_admin_credentials() -> Dict[str, str]:
    """
    Get Airflow admin user credentials.
    
    Returns:
        Dict[str, str]: Admin username and password
    """
    return {
        'username': os.getenv('AIRFLOW_ADMIN_USER', 'admin'),
        'password': os.getenv('AIRFLOW_ADMIN_PASSWORD', 'admin')
    }


def get_postgres_config() -> Dict[str, Any]:
    """
    Get PostgreSQL configuration for Airflow metadata database.
    
    Returns:
        Dict[str, Any]: PostgreSQL configuration
    """
    postgres_config = get_airflow_config('database.postgres', {})
    return {
        'user': postgres_config.get('user', 'airflow'),
        'password': postgres_config.get('password', 'airflow'),
        'database': postgres_config.get('database', 'airflow'),
        'host': postgres_config.get('host', 'postgres'),
        'port': postgres_config.get('port', 5432)
    }


def get_redis_config() -> Dict[str, Any]:
    """
    Get Redis configuration for Celery broker.
    
    Returns:
        Dict[str, Any]: Redis configuration
    """
    redis_config = get_airflow_config('celery.redis', {})
    return {
        'host': redis_config.get('host', 'redis'),
        'port': redis_config.get('port', 6379),
        'password': redis_config.get('password', '')
    }


def get_celery_broker_url() -> str:
    """
    Get Celery broker URL.
    
    Returns:
        str: Celery broker URL
    """
    return get_airflow_config('celery.broker_url', 
                             'redis://:${REDIS_PASSWORD}@${REDIS_HOST}:${REDIS_PORT}/0')


def get_celery_result_backend() -> str:
    """
    Get Celery result backend URL.
    
    Returns:
        str: Celery result backend URL
    """
    return get_airflow_config('celery.result_backend', 
                             'db+postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}')


def get_worker_concurrency() -> int:
    """
    Get Celery worker concurrency.
    
    Returns:
        int: Worker concurrency
    """
    return get_airflow_config('celery.worker_concurrency', 16)


def get_logging_level() -> str:
    """
    Get Airflow logging level.
    
    Returns:
        str: Logging level
    """
    return get_airflow_config('logging.level', 'INFO')


def get_base_log_folder() -> str:
    """
    Get Airflow base log folder.
    
    Returns:
        str: Base log folder path
    """
    return get_airflow_config('logging.base_log_folder', './airflow/logs')


def should_load_examples() -> bool:
    """
    Check if Airflow should load example DAGs.
    
    Returns:
        bool: True if examples should be loaded
    """
    return get_airflow_config('core.load_examples', False)


def is_debug_enabled() -> bool:
    """
    Check if Airflow debug mode is enabled.
    
    Returns:
        bool: True if debug mode is enabled
    """
    return get_airflow_config('core.debug', False)


def get_scheduler_heartbeat_sec() -> int:
    """
    Get scheduler heartbeat interval in seconds.
    
    Returns:
        int: Heartbeat interval
    """
    return get_airflow_config('scheduler.heartbeat_sec', 5)


def get_scheduler_max_threads() -> int:
    """
    Get scheduler max threads.
    
    Returns:
        int: Max threads
    """
    return get_airflow_config('scheduler.max_threads', 2)


def get_dag_file_processor_timeout() -> int:
    """
    Get DAG file processor timeout.
    
    Returns:
        int: Timeout in seconds
    """
    return get_airflow_config('scheduler.dag_file_processor_timeout', 50)


def get_dag_dir_list_interval() -> int:
    """
    Get DAG directory list interval.
    
    Returns:
        int: Interval in seconds
    """
    return get_airflow_config('scheduler.dag_dir_list_interval', 300)
