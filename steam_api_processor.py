import time
from datetime import datetime
from typing import List, Dict, Any, Tuple
from steam_api_client import SteamApiClient


class SteamApiProcessor:
    """
    A class to handle all Steam API processing operations.
    
    This class encapsulates all Steam API-related functionality including:
    - Fetching app lists
    - Validating app IDs
    - Fetching detailed app information
    - Processing individual apps with retry logic
    """
    
    def __init__(self, default_timeout: int = 30, default_delay: float = 0.5):
        """
        Initialize the Steam API processor.
        
        Args:
            default_timeout (int): Default timeout for API requests in seconds
            default_delay (float): Default delay between requests in seconds
        """
        self.default_delay = default_delay
        self.api_client = SteamApiClient(default_timeout=default_timeout)
    
    def get_steam_app_list(self) -> List[Dict[str, Any]]:
        """
        Fetches all Steam app IDs from the Steam API.
        
        Returns:
            List[Dict[str, Any]]: List of dictionaries containing app information
            Each dict has 'appid' and 'name' keys
        """
        return self.api_client.get_app_list()
    
    def get_app_ids_only(self) -> List[int]:
        """
        Returns only the app IDs as a list of integers.
        
        Returns:
            List[int]: List of Steam app IDs
        """
        apps = self.get_steam_app_list()
        return [app.get('appid') for app in apps if app.get('appid') is not None]
    
    def validate_apps_locally(self, apps_dict: Dict[str, str], file_manager, details_file: str = "steam_apps_details.json") -> List[int]:
        """
        Validates which apps need to be fetched by comparing local files.
        NO API CALLS - just file reading.
        
        Args:
            apps_dict (Dict[str, str]): Dictionary of apps from steam_apps_dict.json
            file_manager: FileManager instance for file operations
            details_file (str): Path to steam_apps_details.json
        
        Returns:
            List[int]: List of app IDs that need to be fetched (missing from details file)
        """
        # Load existing details
        existing_details = file_manager.load_json_file(details_file) if file_manager else {}
        
        # Load non-existent apps to skip them
        non_existent_apps = file_manager.load_non_existent_apps() if file_manager else []
        non_existent_apps_set = set(non_existent_apps)
        
        # Count apps in each file
        total_apps_in_dict = len([app_id for app_id in apps_dict.keys() if app_id != 'updated_at'])
        total_apps_in_details = len(existing_details) if existing_details else 0
        
        print(f"\n" + "="*50)
        print("FILE COMPARISON SUMMARY")
        print("="*50)
        print(f"Apps in steam_apps_dict.json: {total_apps_in_dict:,}")
        print(f"Apps in steam_apps_details.json: {total_apps_in_details:,}")
        print(f"Apps marked as non-existent: {len(non_existent_apps):,}")
        
        # Find apps that are in dict but not in details and not marked as non-existent
        missing_apps = []
        skipped_non_existent = 0
        
        for app_id in apps_dict.keys():
            if app_id != 'updated_at':
                app_id_int = int(app_id)
                if app_id_int in non_existent_apps_set:
                    skipped_non_existent += 1
                elif app_id not in existing_details:
                    missing_apps.append(app_id_int)
        
        apps_to_process = len(missing_apps)
        apps_already_exist = total_apps_in_dict - apps_to_process - skipped_non_existent
        
        print(f"Apps that already exist: {apps_already_exist:,}")
        print(f"Apps marked as non-existent (skipped): {skipped_non_existent:,}")
        print(f"Apps that need to be processed: {apps_to_process:,}")
        
        if apps_to_process > 0:
            print(f"Processing percentage: {(apps_to_process/total_apps_in_dict)*100:.1f}%")
        else:
            print("All apps already have details or are marked as non-existent! Nothing to process.")
        
        print("="*50)
        
        return missing_apps
    
    def create_steam_apps_dict(self, file_manager, filename: str = "steam_apps_dict.json") -> Dict[str, str]:
        """
        Creates a dictionary-style JSON file with app IDs as keys and app names as values.
        
        Args:
            file_manager: FileManager instance for file operations
            filename (str): Name of the JSON file to create
        
        Returns:
            Dict[str, str]: Dictionary with app IDs as keys and names as values
        """
        # Load existing data if file exists
        apps_dict = file_manager.load_json_file(filename) if file_manager else {}
        if apps_dict:
            print(f"Loaded {len(apps_dict)} existing apps from {filename}")
        
        apps = self.get_steam_app_list()
        
        if not apps:
            print("No apps found or error occurred.")
            return apps_dict
        
        # Process apps and add new ones
        new_apps_count = 0
        for app in apps:
            app_id = app.get('appid')
            name = app.get('name', 'Unknown')
            if app_id is not None:
                app_id_str = str(app_id)
                if app_id_str not in apps_dict:
                    apps_dict[app_id_str] = name
                    new_apps_count += 1
        
        print(f"Added {new_apps_count} new apps to the dictionary")
        
        # Save to JSON file
        if file_manager and file_manager.save_json_file(apps_dict, filename):
            print(f"Successfully created {filename} with {len(apps_dict)} apps")
        
        return apps_dict
    
    def get_app_details_with_retry(self, app_id: int) -> Tuple[Dict[str, Any], bool]:
        """
        Fetches detailed information for a specific Steam app with retry logic.
        
        Args:
            app_id (int): The Steam app ID
        
        Returns:
            Tuple[Dict[str, Any], bool]: Tuple containing:
                - App details or empty dict if failed/doesn't exist
                - True if the app failed due to errors (not just doesn't exist), False otherwise
        """
        return self.api_client.get_app_details_with_failure_info(app_id)
    
    def process_single_app(self, app_id: int, all_app_details: Dict[str, Any], 
                          failed_app_ids: List[int], non_existent_apps: List[int], 
                          is_retry: bool = False) -> None:
        """
        Processes a single app by fetching its details and updating the data structures.
        
        Args:
            app_id (int): The Steam app ID
            all_app_details (Dict[str, Any]): Dictionary to store app details
            failed_app_ids (List[int]): List to store failed app IDs
            non_existent_apps (List[int]): List to store non-existent app IDs
            is_retry (bool): Whether this is a retry attempt (affects logging)
        """
        app_id_str = str(app_id)
        
        # Check if app already exists in the details file (for retry attempts)
        if is_retry and app_id_str in all_app_details:
            print(f"⏭️  App {app_id} already exists in steam_apps_details.json - skipping")
            return
        
        # Fetch app details
        app_details, is_failure = self.get_app_details_with_retry(app_id)
        
        if app_details:
            all_app_details[app_id_str] = app_details
            if is_retry:
                print(f"✓ Successfully processed app {app_id}")
        elif is_failure:
            # Only add to failed list if it's an actual failure, not just non-existent app
            failed_app_ids.append(app_id)
            if is_retry:
                print(f"✗ App {app_id} still failed")
        else:
            # App doesn't exist or has no data - add to non-existent list
            non_existent_apps.append(app_id)
            if is_retry:
                print(f"○ App {app_id} does not exist or has no data")
    
    def _load_existing_app_details(self, output_file: str, file_manager) -> Dict[str, Any]:
        """
        Loads existing app details from file.
        
        Args:
            output_file (str): Name of the output JSON file
            file_manager: FileManager instance for file operations
            
        Returns:
            Dict[str, Any]: Existing app details or empty dict
        """
        all_app_details = file_manager.load_json_file(output_file) if file_manager else {}
        if all_app_details:
            print(f"Loaded {len(all_app_details)} existing app details from {output_file}")
        return all_app_details
    
    def _print_batch_processing_info(self, total_apps: int, delay_between_requests: float, batch_size: int) -> None:
        """
        Prints information about the batch processing configuration.
        
        Args:
            total_apps (int): Total number of apps to process
            delay_between_requests (float): Delay between requests
            batch_size (int): Batch size for intermediate saves
        """
        print(f"Starting to fetch details for {total_apps} apps...")
        print(f"Delay between requests: {delay_between_requests}s")
        print(f"Batch size: {batch_size}")
    
    def _process_app_batch(self, app_ids: List[int], all_app_details: Dict[str, Any], 
                          failed_app_ids: List[int], non_existent_apps: List[int],
                          delay_between_requests: float, batch_size: int, 
                          output_file: str, file_manager, is_retry: bool = False) -> None:
        """
        Processes a batch of apps with progress tracking and intermediate saves.
        
        Args:
            app_ids (List[int]): List of app IDs to process
            all_app_details (Dict[str, Any]): Dictionary to store app details
            failed_app_ids (List[int]): List to store failed app IDs
            non_existent_apps (List[int]): List to store non-existent app IDs
            delay_between_requests (float): Delay between requests
            batch_size (int): Batch size for intermediate saves
            output_file (str): Output file name
            file_manager: FileManager instance
            is_retry (bool): Whether this is a retry operation
        """
        total_apps = len(app_ids)
        
        for i, app_id in enumerate(app_ids, 1):
            # Print progress
            if is_retry:
                print(f"Processing failed app {app_id} ({i}/{total_apps}) - {(i/total_apps)*100:.1f}%")
            else:
                print(f"Processing app {app_id} ({i}/{total_apps}) - {(i/total_apps)*100:.1f}%")
            
            # Process the app
            self.process_single_app(app_id, all_app_details, failed_app_ids, non_existent_apps, is_retry=is_retry)
            
            # Add delay between requests to avoid rate limiting
            if i < total_apps:  # Don't delay after the last request
                time.sleep(delay_between_requests)
            
            # Save intermediate results every batch_size apps
            if i % batch_size == 0 and file_manager:
                file_manager.save_intermediate_results(all_app_details, output_file, i, non_existent_apps, failed_app_ids)
    
    def _save_final_batch_results(self, all_app_details: Dict[str, Any], output_file: str, file_manager) -> None:
        """
        Saves the final batch processing results.
        
        Args:
            all_app_details (Dict[str, Any]): App details to save
            output_file (str): Output file name
            file_manager: FileManager instance
        """
        print(f"Saving final results to {output_file}...")
        if file_manager and file_manager.save_json_file(all_app_details, output_file):
            print(f"Successfully saved {len(all_app_details)} app details to {output_file}")
    
    def fetch_all_app_details(self, app_ids: List[int], delay_between_requests: float = None, 
                             batch_size: int = 100, output_file: str = "steam_apps_details.json",
                             file_manager=None) -> Tuple[Dict[str, Any], List[int], List[int]]:
        """
        Fetches detailed information for all Steam apps with rate limiting and progress tracking.
        
        Args:
            app_ids (List[int]): List of Steam app IDs to fetch
            delay_between_requests (float): Delay between API requests in seconds
            batch_size (int): Number of apps to process before saving intermediate results
            output_file (str): Name of the output JSON file
            file_manager: FileManager instance for file operations
        
        Returns:
            Tuple[Dict[str, Any], List[int], List[int]]: Tuple containing:
                - Dictionary with app IDs as keys and detailed app data as values
                - List of app IDs that failed after all retry attempts
                - List of app IDs that don't exist or have no data
        """
        if delay_between_requests is None:
            delay_between_requests = self.default_delay
            
        # Load existing data
        all_app_details = self._load_existing_app_details(output_file, file_manager)
        
        # Print processing information
        self._print_batch_processing_info(len(app_ids), delay_between_requests, batch_size)
        
        # Initialize result lists
        failed_app_ids_in_batch = []
        non_existent_apps = []
        
        # Process all apps
        self._process_app_batch(app_ids, all_app_details, failed_app_ids_in_batch, non_existent_apps,
                               delay_between_requests, batch_size, output_file, file_manager, is_retry=False)
        
        # Save final results
        self._save_final_batch_results(all_app_details, output_file, file_manager)
        
        return all_app_details, failed_app_ids_in_batch, non_existent_apps
    
    def _load_existing_data_for_retry(self, output_file: str, file_manager) -> Tuple[Dict[str, Any], int]:
        """
        Loads existing app details for retry processing and reports status.
        
        Args:
            output_file (str): Output file name
            file_manager: FileManager instance
            
        Returns:
            Tuple[Dict[str, Any], int]: Existing app details and initial count
        """
        all_app_details = file_manager.load_json_file(output_file) if file_manager else {}
        initial_count = len(all_app_details)
        
        if all_app_details:
            print(f"📁 Loaded {initial_count} existing app details from {output_file}")
            print(f"📝 Will append new data to existing file (no overwrite)")
        else:
            print(f"📁 No existing {output_file} found. Will create new file.")
        
        return all_app_details, initial_count
    
    def _filter_apps_for_retry(self, failed_app_ids: List[int], all_app_details: Dict[str, Any]) -> List[int]:
        """
        Filters failed app IDs to exclude those that already exist in details file.
        
        Args:
            failed_app_ids (List[int]): Original list of failed app IDs
            all_app_details (Dict[str, Any]): Existing app details
            
        Returns:
            List[int]: Filtered list of apps that need to be processed
        """
        apps_to_process = []
        skipped_existing = 0
        
        for app_id in failed_app_ids:
            app_id_str = str(app_id)
            if app_id_str in all_app_details:
                skipped_existing += 1
            else:
                apps_to_process.append(app_id)
        
        total_apps = len(apps_to_process)
        
        print(f"📊 FILTERING SUMMARY:")
        print(f"   Original failed apps: {len(failed_app_ids)}")
        print(f"   Already exist in details file: {skipped_existing}")
        print(f"   Apps to process: {total_apps}")
        
        return apps_to_process
    
    def _print_retry_processing_info(self, total_apps: int, delay_between_requests: float, batch_size: int) -> None:
        """
        Prints information about retry processing configuration.
        
        Args:
            total_apps (int): Total number of apps to retry
            delay_between_requests (float): Delay between requests
            batch_size (int): Batch size for intermediate saves
        """
        print(f"Processing {total_apps} failed apps...")
        print(f"Delay between requests: {delay_between_requests}s")
        print(f"Batch size: {batch_size}")
    
    def _save_retry_final_results(self, all_app_details: Dict[str, Any], output_file: str, 
                                 initial_count: int, file_manager) -> None:
        """
        Saves final results for retry processing with statistics.
        
        Args:
            all_app_details (Dict[str, Any]): App details to save
            output_file (str): Output file name
            initial_count (int): Initial count of apps before retry
            file_manager: FileManager instance
        """
        final_count = len(all_app_details)
        new_added = final_count - initial_count
        print(f"Saving final results to {output_file}...")
        if file_manager and file_manager.save_json_file(all_app_details, output_file):
            print(f"✅ Successfully saved {final_count} app details to {output_file}")
            print(f"📈 Added {new_added} new apps to existing {initial_count} apps")
    
    def process_failed_apps_batch(self, failed_app_ids: List[int], delay_between_requests: float = None, 
                                batch_size: int = 50, output_file: str = "steam_apps_details.json",
                                file_manager=None) -> Tuple[List[int], List[int]]:
        """
        Processes a batch of failed app IDs.
        
        Args:
            failed_app_ids (List[int]): List of failed app IDs to retry
            delay_between_requests (float): Delay between API requests in seconds
            batch_size (int): Number of apps to process before saving intermediate results
            output_file (str): Name of the output JSON file
            file_manager: FileManager instance for file operations
        
        Returns:
            Tuple[List[int], List[int]]: Tuple containing:
                - List of app IDs that still failed after retry
                - List of app IDs that don't exist or have no data
        """
        if delay_between_requests is None:
            delay_between_requests = self.default_delay
        
        # Load existing data
        all_app_details, initial_count = self._load_existing_data_for_retry(output_file, file_manager)
        
        # Filter apps that need processing
        apps_to_process = self._filter_apps_for_retry(failed_app_ids, all_app_details)
        
        if len(apps_to_process) == 0:
            print("🎉 All failed apps already exist in steam_apps_details.json! Nothing to process.")
            return [], []
        
        # Print retry processing info
        self._print_retry_processing_info(len(apps_to_process), delay_between_requests, batch_size)
        
        # Initialize result lists
        new_failed_app_ids = []
        new_non_existent_apps = []
        
        # Process the filtered apps
        self._process_app_batch(apps_to_process, all_app_details, new_failed_app_ids, new_non_existent_apps,
                               delay_between_requests, batch_size, output_file, file_manager, is_retry=True)
        
        # Save final results
        self._save_retry_final_results(all_app_details, output_file, initial_count, file_manager)
        
        return new_failed_app_ids, new_non_existent_apps
