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
    
    def __init__(self, config_file: str = "config/config.yml"):
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
