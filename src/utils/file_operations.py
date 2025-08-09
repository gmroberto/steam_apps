import json
from datetime import datetime
from typing import List, Dict, Any
from src.utils.config_manager import get_config


# ============================================================================
# CONFIGURATION - Loaded from config.yml with fallback defaults
# ============================================================================

# Default file encoding
DEFAULT_ENCODING = get_config('file_manager.encoding', 'utf-8')

# Default JSON formatting
DEFAULT_INDENT = get_config('file_manager.json_indent', 2)
DEFAULT_ENSURE_ASCII = get_config('file_manager.json_ensure_ascii', False)

# Default file names
DEFAULT_STEAM_APPS_DICT_FILE = get_config('file_manager.files.steam_apps_dict', 'steam_apps_dict.json')
DEFAULT_FAILED_APP_IDS_FILE = get_config('file_manager.files.failed_app_ids', 'failed_app_ids.json')
DEFAULT_NON_EXISTENT_APPS_FILE = get_config('file_manager.files.non_existent_apps', 'non_existent_apps.json')
DEFAULT_STEAM_APPS_DETAILS_FILE = get_config('file_manager.files.steam_apps_details', 'steam_apps_details.json')

# ============================================================================


class FileManager:
    """
    A class to handle all file operations for the Steam app processing system.
    
    This class encapsulates all file I/O operations including:
    - Loading and saving JSON files
    - Managing Steam app dictionaries
    - Handling failed app IDs
    - Managing non-existent app IDs
    - Saving intermediate and final results
    """
    
    def __init__(self, encoding: str = DEFAULT_ENCODING, indent: int = DEFAULT_INDENT, ensure_ascii: bool = DEFAULT_ENSURE_ASCII):
        """
        Initialize the FileManager.
        
        Args:
            encoding (str): File encoding to use (default: utf-8)
            indent (int): JSON indentation level (default: 2)
            ensure_ascii (bool): Whether to ensure ASCII encoding in JSON (default: False)
        """
        self.encoding = encoding
        self.indent = indent
        self.ensure_ascii = ensure_ascii
    
    def load_json_file(self, filename: str, remove_timestamp: bool = True) -> Dict[str, Any]:
        """
        Loads a JSON file and returns its contents.
        
        Args:
            filename (str): Name of the JSON file to load
            remove_timestamp (bool): Whether to remove 'updated_at' timestamp from data
        
        Returns:
            Dict[str, Any]: Contents of the JSON file or empty dict if file doesn't exist
        """
        try:
            with open(filename, 'r', encoding=self.encoding) as f:
                data = json.load(f)
                # Remove timestamp from data for processing if requested
                if remove_timestamp:
                    data.pop('updated_at', None)
                return data
        except FileNotFoundError:
            print(f"No existing file found at {filename}. Starting fresh.")
            return {}
        except json.JSONDecodeError as e:
            print(f"Error reading existing file {filename}: {e}. Starting fresh.")
            return {}
    
    def save_json_file(self, data: Dict[str, Any], filename: str, add_timestamp: bool = True) -> bool:
        """
        Saves data to a JSON file with optional timestamp.
        
        Args:
            data (Dict[str, Any]): Data to save
            filename (str): Name of the output JSON file
            add_timestamp (bool): Whether to add 'updated_at' timestamp to data
        
        Returns:
            bool: True if successful, False otherwise
        """
        # Add timestamp if requested
        data_to_save = data.copy()
        if add_timestamp:
            data_to_save['updated_at'] = datetime.now().isoformat()
        
        try:
            with open(filename, 'w', encoding=self.encoding) as f:
                json.dump(data_to_save, f, indent=self.indent, ensure_ascii=self.ensure_ascii)
            return True
        except IOError as e:
            print(f"Error writing to file {filename}: {e}")
            return False
    
    def load_steam_apps_dict(self, filename: str = DEFAULT_STEAM_APPS_DICT_FILE) -> Dict[str, str]:
        """
        Loads the Steam apps dictionary from a JSON file.
        
        Args:
            filename (str): Name of the JSON file to load
        
        Returns:
            Dict[str, str]: Dictionary with app IDs as keys and names as values
        """
        try:
            with open(filename, 'r', encoding=self.encoding) as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"File {filename} not found. Please run the script first to create it.")
            return {}
        except json.JSONDecodeError as e:
            print(f"Error parsing {filename}: {e}")
            return {}
    
    def load_failed_app_ids(self, filename: str = DEFAULT_FAILED_APP_IDS_FILE) -> List[int]:
        """
        Loads failed app IDs from a JSON file.
        
        Args:
            filename (str): Name of the JSON file to load
        
        Returns:
            List[int]: List of failed app IDs
        """
        try:
            with open(filename, 'r', encoding=self.encoding) as f:
                data = json.load(f)
                return data.get('failed_app_ids', [])
        except FileNotFoundError:
            print(f"No failed app IDs file found at {filename}")
            return []
        except json.JSONDecodeError as e:
            print(f"Error reading failed app IDs file {filename}: {e}")
            return []
    
    def save_failed_app_ids(self, failed_app_ids: List[int], filename: str = DEFAULT_FAILED_APP_IDS_FILE) -> None:
        """
        Saves failed app IDs to a JSON file with metadata.
        
        Args:
            failed_app_ids (List[int]): List of app IDs that failed after all retries
            filename (str): Name of the output JSON file
        """
        failed_data = {
            'failed_app_ids': failed_app_ids,
            'count': len(failed_app_ids),
            'exported_at': datetime.now().isoformat(),
            'description': 'App IDs that failed after all retry attempts'
        }
        
        try:
            with open(filename, 'w', encoding=self.encoding) as f:
                json.dump(failed_data, f, indent=self.indent, ensure_ascii=self.ensure_ascii)
            print(f"Failed app IDs saved to {filename}")
            print(f"Total failed apps: {len(failed_app_ids)}")
        except IOError as e:
            print(f"Error saving failed app IDs to {filename}: {e}")
    
    def save_failed_app_ids_accumulative(self, new_failed_app_ids: List[int], filename: str = DEFAULT_FAILED_APP_IDS_FILE) -> None:
        """
        Saves failed app IDs to a JSON file, accumulating with existing failed app IDs.
        
        Args:
            new_failed_app_ids (List[int]): List of new failed app IDs to add
            filename (str): Name of the output JSON file
        """
        # Load existing failed app IDs
        existing_failed_app_ids = self.load_failed_app_ids(filename)
        
        # Combine existing and new failed app IDs, removing duplicates
        all_failed_app_ids = list(set(existing_failed_app_ids + new_failed_app_ids))
        
        # Save the combined list
        self.save_failed_app_ids(all_failed_app_ids, filename)
    
    def load_non_existent_apps(self, filename: str = DEFAULT_NON_EXISTENT_APPS_FILE) -> List[int]:
        """
        Loads non-existent app IDs from a JSON file.
        
        Args:
            filename (str): Name of the JSON file to load
        
        Returns:
            List[int]: List of non-existent app IDs
        """
        try:
            with open(filename, 'r', encoding=self.encoding) as f:
                data = json.load(f)
                return data.get('non_existent_app_ids', [])
        except FileNotFoundError:
            print(f"No non-existent apps file found at {filename}")
            return []
        except json.JSONDecodeError as e:
            print(f"Error reading non-existent apps file {filename}: {e}")
            return []
    
    def save_non_existent_apps(self, non_existent_apps: List[int], filename: str = DEFAULT_NON_EXISTENT_APPS_FILE) -> None:
        """
        Saves non-existent app IDs to a JSON file with metadata.
        
        Args:
            non_existent_apps (List[int]): List of app IDs that don't exist or have no data
            filename (str): Name of the output JSON file
        """
        non_existent_data = {
            'non_existent_app_ids': non_existent_apps,
            'count': len(non_existent_apps),
            'exported_at': datetime.now().isoformat(),
            'description': 'App IDs that do not exist or have no data in Steam'
        }
        
        try:
            with open(filename, 'w', encoding=self.encoding) as f:
                json.dump(non_existent_data, f, indent=self.indent, ensure_ascii=self.ensure_ascii)
            print(f"Non-existent app IDs saved to {filename}")
            print(f"Total non-existent apps: {len(non_existent_apps)}")
        except IOError as e:
            print(f"Error saving non-existent app IDs to {filename}: {e}")
    
    def save_non_existent_apps_accumulative(self, new_non_existent_apps: List[int], filename: str = DEFAULT_NON_EXISTENT_APPS_FILE) -> None:
        """
        Saves non-existent app IDs to a JSON file, accumulating with existing non-existent app IDs.
        
        Args:
            new_non_existent_apps (List[int]): List of new non-existent app IDs to add
            filename (str): Name of the output JSON file
        """
        # Load existing non-existent app IDs
        existing_non_existent_apps = self.load_non_existent_apps(filename)
        
        # Combine existing and new non-existent app IDs, removing duplicates
        all_non_existent_apps = list(set(existing_non_existent_apps + new_non_existent_apps))
        
        # Save the combined list
        self.save_non_existent_apps(all_non_existent_apps, filename)
    
    def save_intermediate_results(self, all_app_details: Dict[str, Any], output_file: str, 
                                current_count: int, non_existent_apps: List[int] = None, 
                                failed_app_ids: List[int] = None) -> None:
        """
        Saves intermediate results to files.
        
        Args:
            all_app_details (Dict[str, Any]): App details to save
            output_file (str): Name of the output file
            current_count (int): Current number of processed apps
            non_existent_apps (List[int]): List of non-existent apps to save
            failed_app_ids (List[int]): List of failed app IDs to save
        """
        print(f"Saving intermediate results after {current_count} apps...")
        if self.save_json_file(all_app_details, output_file):
            print(f"Intermediate results saved to {output_file}")
        
        # Also save non-existent apps if provided
        if non_existent_apps and len(non_existent_apps) > 0:
            self.save_non_existent_apps_accumulative(non_existent_apps, DEFAULT_NON_EXISTENT_APPS_FILE)
            print(f"Non-existent apps saved to non_existent_apps.json ({len(non_existent_apps)} new apps)")
        
        # Also save failed app IDs if provided
        if failed_app_ids and len(failed_app_ids) > 0:
            self.save_failed_app_ids_accumulative(failed_app_ids, DEFAULT_FAILED_APP_IDS_FILE)
            print(f"Failed app IDs saved to failed_app_ids.json ({len(failed_app_ids)} new apps)")
    
    def save_final_results(self, failed_fetch_ids: List[int], non_existent_apps: List[int]) -> None:
        """
        Saves the final results and summary.
        
        Args:
            failed_fetch_ids (List[int]): App IDs that failed fetching
            non_existent_apps (List[int]): App IDs that don't exist or have no data
        """
        # Save failed app IDs
        if failed_fetch_ids:
            self.save_failed_app_ids(failed_fetch_ids, DEFAULT_FAILED_APP_IDS_FILE)
        
        # Save non-existent app IDs
        if non_existent_apps:
            self.save_non_existent_apps(non_existent_apps, DEFAULT_NON_EXISTENT_APPS_FILE)
    
    def print_completion_summary(self, app_details: Dict[str, Any], failed_fetch_ids: List[int], non_existent_apps: List[int]) -> None:
        """
        Prints the completion summary.
        
        Args:
            app_details (Dict[str, Any]): App details that were successfully fetched
            failed_fetch_ids (List[int]): App IDs that failed fetching
            non_existent_apps (List[int]): App IDs that don't exist or have no data
        """
        print(f"\n" + "="*50)
        print("COMPLETION SUMMARY")
        print("="*50)
        print(f"Successfully fetched details for {len(app_details)} apps")
        print(f"Fetch failures: {len(failed_fetch_ids)} apps")
        print(f"Non-existent apps: {len(non_existent_apps)} apps")
        print("\nFiles created:")
        print("- steam_apps_details.json: App details")
        if failed_fetch_ids:
            print("- failed_app_ids.json: Apps that failed fetching")
        if non_existent_apps:
            print("- non_existent_apps.json: Apps that don't exist or have no data")
