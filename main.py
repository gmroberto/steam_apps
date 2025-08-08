import requests
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Tuple
from retry import retry
from steam_api_processor import SteamApiProcessor
from file_manager import FileManager


# ============================================================================
# GLOBAL INSTANCES
# ============================================================================

# Create global instances
steam_api = SteamApiProcessor()
file_manager = FileManager()


# ============================================================================
# WRAPPER FUNCTIONS FOR BACKWARD COMPATIBILITY
# ============================================================================

# These functions provide backward compatibility by wrapping FileManager methods
def load_json_file(filename: str) -> Dict[str, Any]:
    return file_manager.load_json_file(filename)

def save_json_file(data: Dict[str, Any], filename: str) -> bool:
    return file_manager.save_json_file(data, filename)

def load_steam_apps_dict(filename: str = "steam_apps_dict.json") -> Dict[str, str]:
    return file_manager.load_steam_apps_dict(filename)

def load_failed_app_ids(filename: str = "failed_app_ids.json") -> List[int]:
    return file_manager.load_failed_app_ids(filename)

def save_failed_app_ids(failed_app_ids: List[int], filename: str = "failed_app_ids.json") -> None:
    return file_manager.save_failed_app_ids(failed_app_ids, filename)

def save_failed_app_ids_accumulative(new_failed_app_ids: List[int], filename: str = "failed_app_ids.json") -> None:
    return file_manager.save_failed_app_ids_accumulative(new_failed_app_ids, filename)

def save_non_existent_apps(non_existent_apps: List[int], filename: str = "non_existent_apps.json") -> None:
    return file_manager.save_non_existent_apps(non_existent_apps, filename)

def save_non_existent_apps_accumulative(new_non_existent_apps: List[int], filename: str = "non_existent_apps.json") -> None:
    return file_manager.save_non_existent_apps_accumulative(new_non_existent_apps, filename)

def load_non_existent_apps(filename: str = "non_existent_apps.json") -> List[int]:
    return file_manager.load_non_existent_apps(filename)


# ============================================================================
# DATA PROCESSING FUNCTIONS
# ============================================================================

def create_steam_apps_json(filename: str = "steam_apps.json") -> Dict[str, str]:
    """
    Creates a JSON file with app IDs as keys and app names as values.
    
    Args:
        filename (str): Name of the JSON file to create (default: steam_apps.json)
    
    Returns:
        Dict[str, str]: Dictionary with app IDs as keys and names as values
    """
    # Use the steam_api instance to create the apps dictionary
    return steam_api.create_steam_apps_dict(file_manager, filename)


def validate_apps_locally(apps_dict: Dict[str, str], details_file: str = "steam_apps_details.json") -> List[int]:
    """
    Validates which apps need to be fetched by comparing local files.
    NO API CALLS - just file reading.
    
    Args:
        apps_dict (Dict[str, str]): Dictionary of apps from steam_apps_dict.json
        details_file (str): Path to steam_apps_details.json
    
    Returns:
        List[int]: List of app IDs that need to be fetched (missing from details file)
    """
    # Use the steam_api instance to validate apps locally
    return steam_api.validate_apps_locally(apps_dict, file_manager, details_file)


def process_single_app(app_id: int, app_id_str: str, all_app_details: Dict[str, Any], 
                      failed_app_ids_in_batch: List[int], non_existent_apps: List[int]) -> None:
    """
    Processes a single app by fetching its details and updating the data structures.
    
    Args:
        app_id (int): The Steam app ID
        app_id_str (str): String representation of the app ID (unused, kept for compatibility)
        all_app_details (Dict[str, Any]): Dictionary to store app details
        failed_app_ids_in_batch (List[int]): List to store failed app IDs
        non_existent_apps (List[int]): List to store non-existent app IDs
    """
    # Use the steam_api instance to process the app
    steam_api.process_single_app(app_id, all_app_details, failed_app_ids_in_batch, non_existent_apps, is_retry=False)


def save_intermediate_results(all_app_details: Dict[str, Any], output_file: str, 
                            current_count: int, non_existent_apps: List[int] = None, 
                            failed_app_ids: List[int] = None) -> None:
    """
    Saves intermediate results to a JSON file.
    
    Args:
        all_app_details (Dict[str, Any]): App details to save
        output_file (str): Name of the output file
        current_count (int): Current number of processed apps
        non_existent_apps (List[int]): List of non-existent apps to save
        failed_app_ids (List[int]): List of failed app IDs to save
    """
    # Use the file_manager instance to save intermediate results
    file_manager.save_intermediate_results(all_app_details, output_file, current_count, non_existent_apps, failed_app_ids)


def fetch_all_app_details(app_ids: List[int], delay_between_requests: float = 0.5, 
                         batch_size: int = 100, output_file: str = "steam_apps_details.json") -> Tuple[Dict[str, Any], List[int], List[int]]:
    """
    Fetches detailed information for all Steam apps with rate limiting and progress tracking.
    
    Args:
        app_ids (List[int]): List of Steam app IDs to fetch
        delay_between_requests (float): Delay between API requests in seconds
        batch_size (int): Number of apps to process before saving intermediate results
        output_file (str): Name of the output JSON file
    
    Returns:
        Tuple[Dict[str, Any], List[int], List[int]]: Tuple containing:
            - Dictionary with app IDs as keys and detailed app data as values
            - List of app IDs that failed after all retry attempts
            - List of app IDs that don't exist or have no data
    """
    # Use the steam_api instance to fetch all app details
    return steam_api.fetch_all_app_details(
        app_ids=app_ids,
        delay_between_requests=delay_between_requests,
        batch_size=batch_size,
        output_file=output_file,
        file_manager=file_manager
    )


# ============================================================================
# MAIN EXECUTION FUNCTIONS
# ============================================================================

def setup_configuration() -> Tuple[float, int, int]:
    """
    Sets up the configuration for the app processing.
    
    Returns:
        Tuple[float, int, int]: Configuration values (delay, batch_size, max_apps)
    """
    delay_between_requests = 0.1  # 0.1 second delay between requests
    batch_size = 100  # Save intermediate results every 100 apps
    max_apps = None  # Process all apps (set to a number to limit processing)
    
    return delay_between_requests, batch_size, max_apps


def prepare_app_ids(apps_dict: Dict[str, str], max_apps: int = None) -> List[int]:
    """
    Prepares the list of app IDs to process.
    
    Args:
        apps_dict (Dict[str, str]): Dictionary of apps
        max_apps (int): Maximum number of apps to process (None for all)
    
    Returns:
        List[int]: List of app IDs to process
    """
    # Get app IDs to process (filter out metadata keys like 'updated_at')
    app_ids = [app_id for app_id in apps_dict.keys() if app_id != 'updated_at']
    
    if max_apps:
        app_ids = app_ids[:max_apps]
    
    # Convert string app IDs to integers
    return [int(app_id) for app_id in app_ids]


def run_local_validation_phase(apps_dict: Dict[str, str]) -> List[int]:
    """
    Runs the local validation phase by comparing JSON files.
    NO API CALLS - just file reading.
    
    Args:
        apps_dict (Dict[str, str]): Dictionary of apps from steam_apps_dict.json
    
    Returns:
        List[int]: List of app IDs that need to be fetched
    """
    print("\n" + "="*30)
    print("LOCAL VALIDATION PHASE")
    print("="*30)
    print("Comparing steam_apps_dict.json with steam_apps_details.json...")
    
    missing_app_ids = validate_apps_locally(apps_dict)
    
    if not missing_app_ids:
        print("All apps already have details! Nothing to fetch.")
    else:
        print(f"Found {len(missing_app_ids)} apps that need to be fetched")
    
    return missing_app_ids


def run_fetch_phase(app_ids_to_fetch: List[int], delay_between_requests: float, 
                   batch_size: int) -> Tuple[Dict[str, Any], List[int], List[int]]:
    """
    Runs the fetch phase of the app processing.
    
    Args:
        app_ids_to_fetch (List[int]): List of app IDs to fetch
        delay_between_requests (float): Delay between requests
        batch_size (int): Batch size for intermediate saves
    
    Returns:
        Tuple[Dict[str, Any], List[int], List[int]]: App details, failed fetch IDs, and non-existent apps
    """
    if not app_ids_to_fetch:
        print("No apps to fetch. Skipping fetch phase.")
        return {}, [], []
    
    print("\n" + "="*30)
    print("FETCHING APP DETAILS")
    print("="*30)
    app_details, failed_fetch_ids, non_existent_apps = fetch_all_app_details(
        app_ids_to_fetch, 
        delay_between_requests=delay_between_requests,
        batch_size=batch_size,
        output_file="steam_apps_details.json"
    )
    
    # Save failed fetch app IDs to a file
    if failed_fetch_ids:
        save_failed_app_ids(failed_fetch_ids, "failed_fetch_app_ids.json")
    else:
        print("No fetch failures to save.")
    
    # Save non-existent app IDs to a file
    if non_existent_apps:
        save_non_existent_apps(non_existent_apps, "non_existent_apps.json")
    else:
        print("No non-existent apps to save.")
    
    return app_details, failed_fetch_ids, non_existent_apps


def save_final_results(failed_fetch_ids: List[int], non_existent_apps: List[int]) -> None:
    """
    Saves the final results and summary.
    
    Args:
        failed_fetch_ids (List[int]): App IDs that failed fetching
        non_existent_apps (List[int]): App IDs that don't exist or have no data
    """
    # Use the file_manager instance to save final results
    file_manager.save_final_results(failed_fetch_ids, non_existent_apps)


def print_completion_summary(app_details: Dict[str, Any], failed_fetch_ids: List[int], non_existent_apps: List[int]) -> None:
    """
    Prints the completion summary.
    
    Args:
        app_details (Dict[str, Any]): App details that were successfully fetched
        failed_fetch_ids (List[int]): App IDs that failed fetching
        non_existent_apps (List[int]): App IDs that don't exist or have no data
    """
    # Use the file_manager instance to print completion summary
    file_manager.print_completion_summary(app_details, failed_fetch_ids, non_existent_apps)


def create_initial_files() -> Dict[str, str]:
    """
    Creates the initial JSON files.
    
    Returns:
        Dict[str, str]: Dictionary of apps
    """
    print("Creating JSON files...")
    
    # Create dictionary-style JSON (app_id -> name)
    apps_dict = create_steam_apps_json("steam_apps_dict.json")
    
    print("\nJSON files created successfully!")
    print("Files created:")
    print("- steam_apps_dict.json: Dictionary format (app_id -> name)")
    
    return apps_dict


def _print_workflow_header() -> None:
    """
    Prints the workflow header information.
    """
    print("\n" + "="*50)
    print("STEAM APP DETAILS FETCHER")
    print("="*50)

def _load_and_validate_apps_dict(filename: str) -> Dict[str, str]:
    """
    Loads and validates the apps dictionary.
    
    Args:
        filename (str): Name of the apps dictionary file
        
    Returns:
        Dict[str, str]: Apps dictionary
        
    Raises:
        SystemExit: If no apps dictionary is found
    """
    apps_dict = load_steam_apps_dict(filename)
    
    if not apps_dict:
        print("No apps dictionary found. Exiting.")
        exit(1)
    
    print(f"Found {len(apps_dict)} apps in {filename}")
    return apps_dict

def _apply_max_apps_limit(missing_app_ids: List[int], max_apps: int = None) -> List[int]:
    """
    Applies maximum apps limit if specified.
    
    Args:
        missing_app_ids (List[int]): List of missing app IDs
        max_apps (int): Maximum number of apps to process
        
    Returns:
        List[int]: Limited list of app IDs
    """
    if max_apps and len(missing_app_ids) > max_apps:
        missing_app_ids = missing_app_ids[:max_apps]
        print(f"Limited to {max_apps} apps due to configuration")
    
    return missing_app_ids

def _print_processing_info(missing_app_ids: List[int], delay_between_requests: float, batch_size: int) -> None:
    """
    Prints processing configuration information.
    
    Args:
        missing_app_ids (List[int]): List of apps to process
        delay_between_requests (float): Delay between requests
        batch_size (int): Batch size
    """
    if missing_app_ids:
        print(f"Will process {len(missing_app_ids)} apps with {delay_between_requests}s delay between requests")
        print(f"Batch size: {batch_size}")

def main_processing_workflow() -> None:
    """
    Main workflow for processing Steam apps.
    """
    # Print header
    _print_workflow_header()
    
    # Load and validate apps dictionary
    apps_dict = _load_and_validate_apps_dict("steam_apps_dict.json")
    
    # Setup configuration
    delay_between_requests, batch_size, max_apps = setup_configuration()
    
    try:
        # Run local validation phase (NO API CALLS)
        missing_app_ids = run_local_validation_phase(apps_dict)
        
        # Apply max_apps limit if specified
        missing_app_ids = _apply_max_apps_limit(missing_app_ids, max_apps)
        
        # Print processing info
        _print_processing_info(missing_app_ids, delay_between_requests, batch_size)
        
        # Run fetch phase
        app_details, failed_fetch_ids, non_existent_apps = run_fetch_phase(missing_app_ids, delay_between_requests, batch_size)
        
        # Save final results
        save_final_results(failed_fetch_ids, non_existent_apps)
        
        # Print completion summary
        print_completion_summary(app_details, failed_fetch_ids, non_existent_apps)
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")


if __name__ == "__main__":
    # Create initial files
    apps_dict = create_initial_files()
    
    # Run main processing workflow
    main_processing_workflow()
