import requests
import json
from typing import List, Dict, Any, Tuple
from retry import retry


class SteamApiClient:
    """
    A client class to handle all HTTP requests to the Steam API.
    
    This class is responsible for:
    - Making HTTP requests to Steam API endpoints
    - Handling rate limiting and retries
    - Parsing API responses
    - Managing request timeouts and error handling
    """
    
    def __init__(self, default_timeout: int = 30):
        """
        Initialize the Steam API client.
        
        Args:
            default_timeout (int): Default timeout for API requests in seconds
        """
        self.default_timeout = default_timeout
        self.app_list_url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
        self.app_details_url = "https://store.steampowered.com/api/appdetails?appids={}"
    
    def get_app_list(self) -> List[Dict[str, Any]]:
        """
        Fetches all Steam app IDs from the Steam API.
        
        Returns:
            List[Dict[str, Any]]: List of dictionaries containing app information
            Each dict has 'appid' and 'name' keys
        """
        try:
            response = requests.get(self.app_list_url, timeout=self.default_timeout)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            data = response.json()
            apps = data.get('applist', {}).get('apps', [])
            
            return apps
            
        except requests.RequestException as e:
            print(f"Error fetching data from Steam API: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {e}")
            return []
    
    def get_app_details_single(self, app_id: int) -> Dict[str, Any]:
        """
        Fetches detailed information for a specific Steam app (single attempt).
        
        Args:
            app_id (int): The Steam app ID
        
        Returns:
            Dict[str, Any]: App details or empty dict if failed/doesn't exist
        """
        url = self.app_details_url.format(app_id)
        
        try:
            response = requests.get(url, timeout=self.default_timeout)
            
            # Check if we got rate limited (429 status code)
            if response.status_code == 429:
                print(f"Rate limited for app {app_id}, will retry...")
                raise requests.RequestException(f"Rate limited for app {app_id}")
            
            response.raise_for_status()
            data = response.json()
            
            # Check if the app exists and has data
            app_data = data.get(str(app_id), {})
            if app_data.get('success', False):
                return app_data.get('data', {})
            else:
                # App doesn't exist or has no data - this is not an error, so don't retry
                return {}
                
        except requests.RequestException as e:
            print(f"Request error for app {app_id}: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error for app {app_id}: {e}")
            raise
    
    @retry(tries=8, delay=1, backoff=2, max_delay=120)
    def get_app_details_with_retry(self, app_id: int) -> Dict[str, Any]:
        """
        Fetches detailed information for a specific Steam app with retry logic.
        
        Args:
            app_id (int): The Steam app ID
        
        Returns:
            Dict[str, Any]: App details or empty dict if failed/doesn't exist
        """
        return self.get_app_details_single(app_id)
    
    def get_app_details_with_failure_info(self, app_id: int) -> Tuple[Dict[str, Any], bool]:
        """
        Fetches app details and returns both the data and failure status.
        
        Args:
            app_id (int): The Steam app ID
        
        Returns:
            tuple[Dict[str, Any], bool]: Tuple containing:
                - App details or empty dict if failed/doesn't exist
                - True if the app failed due to errors (not just doesn't exist), False otherwise
        """
        try:
            result = self.get_app_details_with_retry(app_id)
            if not result:
                print(f"App {app_id} does not exist or has no data")
                return result, False  # Not a failure, just doesn't exist
            return result, False  # Success
        except Exception as e:
            print(f"Failed to fetch app {app_id} after all retries: {e}")
            return {}, True  # Actual failure
