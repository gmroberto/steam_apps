import requests
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Tuple
from retry import retry


# ============================================================================
# STEAM API FUNCTIONS
# ============================================================================

def get_steam_app_list() -> List[Dict[str, Any]]:
    """
    Fetches all Steam app IDs from the Steam API.
    
    Returns:
        List[Dict[str, Any]]: List of dictionaries containing app information
        Each dict has 'appid' and 'name' keys
    """
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    
    try:
        response = requests.get(url)
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


def get_app_ids_only() -> List[int]:
    """
    Returns only the app IDs as a list of integers.
    
    Returns:
        List[int]: List of Steam app IDs
    """
    apps = get_steam_app_list()
    return [app.get('appid') for app in apps if app.get('appid') is not None]


def validate_app_id(app_id: int) -> bool:
    """
    Validates if an app ID exists in Steam's database before making detailed requests.
    
    Args:
        app_id (int): The Steam app ID to validate
    
    Returns:
        bool: True if app exists, False otherwise
    """
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
    
    try:
        response = requests.get(url, timeout=10)
        
        # Check if we got rate limited (429 status code)
        if response.status_code == 429:
            print(f"Rate limited for app {app_id}, will retry...")
            raise requests.RequestException(f"Rate limited for app {app_id}")
        
        response.raise_for_status()
        data = response.json()
        
        # Check if the app exists and has data
        app_data = data.get(str(app_id), {})
        return app_data.get('success', False)
        
    except requests.RequestException as e:
        print(f"Request error for app {app_id}: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error for app {app_id}: {e}")
        raise


@retry(tries=9, delay=1, backoff=2, max_delay=60)
def validate_app_id_with_retry(app_id: int) -> bool:
    """
    Wrapper function that applies retry logic to validate_app_id.
    
    Args:
        app_id (int): The Steam app ID to validate
    
    Returns:
        bool: True if app exists, False otherwise
    """
    return validate_app_id(app_id)


def _fetch_app_details_single(app_id: int) -> Dict[str, Any]:
    """
    Fetches detailed information for a specific Steam app (single attempt).
    This function also validates if the app exists before returning data.
    
    Args:
        app_id (int): The Steam app ID
    
    Returns:
        Dict[str, Any]: App details or empty dict if failed/doesn't exist
    """
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
    
    try:
        response = requests.get(url, timeout=30)
        
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
def _fetch_app_details_with_retry(app_id: int) -> Dict[str, Any]:
    """
    Wrapper function that applies retry logic to _fetch_app_details_single.
    
    Args:
        app_id (int): The Steam app ID
    
    Returns:
        Dict[str, Any]: App details or empty dict if failed/doesn't exist
    """
    return _fetch_app_details_single(app_id)


def get_app_details_with_retry(app_id: int, max_retries: int = 3, base_delay: float = 1.0) -> Tuple[Dict[str, Any], bool]:
    """
    Fetches detailed information for a specific Steam app with retry logic.
    This function validates if the app exists and returns empty dict if it doesn't.
    
    Args:
        app_id (int): The Steam app ID
        max_retries (int): Maximum number of retry attempts (deprecated, use decorator)
        base_delay (float): Base delay between retries (deprecated, use decorator)
    
    Returns:
        Tuple[Dict[str, Any], bool]: Tuple containing:
            - App details or empty dict if failed/doesn't exist
            - True if the app failed due to errors (not just doesn't exist), False otherwise
    """
    try:
        result = _fetch_app_details_with_retry(app_id)
        if not result:
            print(f"App {app_id} does not exist or has no data")
            return result, False  # Not a failure, just doesn't exist
        return result, False  # Success
    except Exception as e:
        print(f"Failed to fetch app {app_id} after all retries: {e}")
        return {}, True  # Actual failure


# ============================================================================
# FILE OPERATIONS
# ============================================================================

def load_json_file(filename: str) -> Dict[str, Any]:
    """
    Loads a JSON file and returns its contents.
    
    Args:
        filename (str): Name of the JSON file to load
    
    Returns:
        Dict[str, Any]: Contents of the JSON file or empty dict if file doesn't exist
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Remove timestamp from data for processing
            data.pop('updated_at', None)
            return data
    except FileNotFoundError:
        print(f"No existing file found at {filename}. Starting fresh.")
        return {}
    except json.JSONDecodeError as e:
        print(f"Error reading existing file {filename}: {e}. Starting fresh.")
        return {}


def save_json_file(data: Dict[str, Any], filename: str) -> bool:
    """
    Saves data to a JSON file with timestamp.
    
    Args:
        data (Dict[str, Any]): Data to save
        filename (str): Name of the output JSON file
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Add timestamp
    data_with_timestamp = data.copy()
    data_with_timestamp['updated_at'] = datetime.now().isoformat()
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data_with_timestamp, f, indent=2, ensure_ascii=False)
        return True
    except IOError as e:
        print(f"Error writing to file {filename}: {e}")
        return False


def load_steam_apps_dict(filename: str = "steam_apps_dict.json") -> Dict[str, str]:
    """
    Loads the Steam apps dictionary from a JSON file.
    
    Args:
        filename (str): Name of the JSON file to load
    
    Returns:
        Dict[str, str]: Dictionary with app IDs as keys and names as values
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"File {filename} not found. Please run the script first to create it.")
        return {}
    except json.JSONDecodeError as e:
        print(f"Error parsing {filename}: {e}")
        return {}


def load_failed_app_ids(filename: str = "failed_app_ids.json") -> List[int]:
    """
    Loads failed app IDs from a JSON file.
    
    Args:
        filename (str): Name of the JSON file to load
    
    Returns:
        List[int]: List of failed app IDs
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('failed_app_ids', [])
    except FileNotFoundError:
        print(f"No failed app IDs file found at {filename}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error reading failed app IDs file {filename}: {e}")
        return []


def save_failed_app_ids(failed_app_ids: List[int], filename: str = "failed_app_ids.json") -> None:
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
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(failed_data, f, indent=2, ensure_ascii=False)
        print(f"Failed app IDs saved to {filename}")
        print(f"Total failed apps: {len(failed_app_ids)}")
    except IOError as e:
        print(f"Error saving failed app IDs to {filename}: {e}")


def save_failed_app_ids_accumulative(new_failed_app_ids: List[int], filename: str = "failed_app_ids.json") -> None:
    """
    Saves failed app IDs to a JSON file, accumulating with existing failed app IDs.
    
    Args:
        new_failed_app_ids (List[int]): List of new failed app IDs to add
        filename (str): Name of the output JSON file
    """
    # Load existing failed app IDs
    existing_failed_app_ids = load_failed_app_ids(filename)
    
    # Combine existing and new failed app IDs, removing duplicates
    all_failed_app_ids = list(set(existing_failed_app_ids + new_failed_app_ids))
    
    # Save the combined list
    save_failed_app_ids(all_failed_app_ids, filename)


def save_non_existent_apps(non_existent_apps: List[int], filename: str = "non_existent_apps.json") -> None:
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
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(non_existent_data, f, indent=2, ensure_ascii=False)
        print(f"Non-existent app IDs saved to {filename}")
        print(f"Total non-existent apps: {len(non_existent_apps)}")
    except IOError as e:
        print(f"Error saving non-existent app IDs to {filename}: {e}")


def save_non_existent_apps_accumulative(new_non_existent_apps: List[int], filename: str = "non_existent_apps.json") -> None:
    """
    Saves non-existent app IDs to a JSON file, accumulating with existing non-existent app IDs.
    
    Args:
        new_non_existent_apps (List[int]): List of new non-existent app IDs to add
        filename (str): Name of the output JSON file
    """
    # Load existing non-existent app IDs
    existing_non_existent_apps = load_non_existent_apps(filename)
    
    # Combine existing and new non-existent app IDs, removing duplicates
    all_non_existent_apps = list(set(existing_non_existent_apps + new_non_existent_apps))
    
    # Save the combined list
    save_non_existent_apps(all_non_existent_apps, filename)


def load_non_existent_apps(filename: str = "non_existent_apps.json") -> List[int]:
    """
    Loads non-existent app IDs from a JSON file.
    
    Args:
        filename (str): Name of the JSON file to load
    
    Returns:
        List[int]: List of non-existent app IDs
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('non_existent_app_ids', [])
    except FileNotFoundError:
        print(f"No non-existent apps file found at {filename}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error reading non-existent apps file {filename}: {e}")
        return []


# ============================================================================
# DATA PROCESSING FUNCTIONS
# ============================================================================

def process_app_list_to_dict(apps: List[Dict[str, Any]], existing_dict: Dict[str, str]) -> Tuple[Dict[str, str], int]:
    """
    Processes a list of apps and adds new ones to an existing dictionary.
    
    Args:
        apps (List[Dict[str, Any]]): List of app dictionaries from Steam API
        existing_dict (Dict[str, str]): Existing dictionary to add to
    
    Returns:
        Tuple[Dict[str, str], int]: Updated dictionary and count of new apps added
    """
    apps_dict = existing_dict.copy()
    new_apps_count = 0
    
    for app in apps:
        app_id = app.get('appid')
        name = app.get('name', 'Unknown')
        if app_id is not None:
            app_id_str = str(app_id)
            if app_id_str not in apps_dict:
                apps_dict[app_id_str] = name
                new_apps_count += 1
    
    return apps_dict, new_apps_count


def create_steam_apps_json(filename: str = "steam_apps.json") -> Dict[str, str]:
    """
    Creates a JSON file with app IDs as keys and app names as values.
    
    Args:
        filename (str): Name of the JSON file to create (default: steam_apps.json)
    
    Returns:
        Dict[str, str]: Dictionary with app IDs as keys and names as values
    """
    # Load existing data if file exists
    apps_dict = load_json_file(filename)
    if apps_dict:
        print(f"Loaded {len(apps_dict)} existing apps from {filename}")
    
    apps = get_steam_app_list()
    
    if not apps:
        print("No apps found or error occurred.")
        return apps_dict
    
    # Process apps and add new ones
    apps_dict, new_apps_count = process_app_list_to_dict(apps, apps_dict)
    print(f"Added {new_apps_count} new apps to the dictionary")
    
    # Save to JSON file
    if save_json_file(apps_dict, filename):
        print(f"Successfully created {filename} with {len(apps_dict)} apps")
    
    return apps_dict


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
    # Load existing details
    existing_details = load_json_file(details_file)
    
    # Load non-existent apps to skip them
    non_existent_apps = load_non_existent_apps()
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


def process_single_app(app_id: int, app_id_str: str, all_app_details: Dict[str, Any], 
                      failed_app_ids_in_batch: List[int], non_existent_apps: List[int]) -> None:
    """
    Processes a single app by fetching its details and updating the data structures.
    
    Args:
        app_id (int): The Steam app ID
        app_id_str (str): String representation of the app ID
        all_app_details (Dict[str, Any]): Dictionary to store app details
        failed_app_ids_in_batch (List[int]): List to store failed app IDs
        non_existent_apps (List[int]): List to store non-existent app IDs
    """
    # Fetch app details (includes validation)
    app_details, is_failure = get_app_details_with_retry(app_id)
    
    if app_details:
        all_app_details[app_id_str] = app_details
    elif is_failure:
        # Only add to failed list if it's an actual failure, not just non-existent app
        failed_app_ids_in_batch.append(app_id)
    else:
        # App doesn't exist or has no data - add to non-existent list
        non_existent_apps.append(app_id)


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
    print(f"Saving intermediate results after {current_count} apps...")
    if save_json_file(all_app_details, output_file):
        print(f"Intermediate results saved to {output_file}")
    
    # Also save non-existent apps if provided
    if non_existent_apps and len(non_existent_apps) > 0:
        save_non_existent_apps_accumulative(non_existent_apps, "non_existent_apps.json")
        print(f"Non-existent apps saved to non_existent_apps.json ({len(non_existent_apps)} new apps)")
    
    # Also save failed app IDs if provided
    if failed_app_ids and len(failed_app_ids) > 0:
        save_failed_app_ids_accumulative(failed_app_ids, "failed_app_ids.json")
        print(f"Failed app IDs saved to failed_app_ids.json ({len(failed_app_ids)} new apps)")


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
    # Load existing data if file exists
    all_app_details = load_json_file(output_file)
    if all_app_details:
        print(f"Loaded {len(all_app_details)} existing app details from {output_file}")
    
    total_apps = len(app_ids)
    
    print(f"Starting to fetch details for {total_apps} apps...")
    print(f"Delay between requests: {delay_between_requests}s")
    print(f"Batch size: {batch_size}")
    
    failed_app_ids_in_batch = []
    non_existent_apps = []
    
    for i, app_id in enumerate(app_ids, 1):
        app_id_str = str(app_id)
        
        print(f"Processing app {app_id} ({i}/{total_apps}) - {(i/total_apps)*100:.1f}%")
        
        # Process the app
        process_single_app(app_id, app_id_str, all_app_details, failed_app_ids_in_batch, non_existent_apps)
        
        # Add delay between requests to avoid rate limiting
        if i < total_apps:  # Don't delay after the last request
            time.sleep(delay_between_requests)
        
        # Save intermediate results every batch_size apps
        if i % batch_size == 0:
            save_intermediate_results(all_app_details, output_file, i, non_existent_apps, failed_app_ids_in_batch)
    
    # Save final results
    print(f"Saving final results to {output_file}...")
    if save_json_file(all_app_details, output_file):
        print(f"Successfully saved {len(all_app_details)} app details to {output_file}")
    
    return all_app_details, failed_app_ids_in_batch, non_existent_apps


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
    # Save failed app IDs
    if failed_fetch_ids:
        save_failed_app_ids(failed_fetch_ids, "failed_app_ids.json")
    
    # Save non-existent app IDs
    if non_existent_apps:
        save_non_existent_apps(non_existent_apps, "non_existent_apps.json")


def print_completion_summary(app_details: Dict[str, Any], failed_fetch_ids: List[int], non_existent_apps: List[int]) -> None:
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


def main_processing_workflow() -> None:
    """
    Main workflow for processing Steam apps.
    """
    # Fetch detailed app information
    print("\n" + "="*50)
    print("STEAM APP DETAILS FETCHER")
    print("="*50)
    
    # Load the apps dictionary
    apps_dict = load_steam_apps_dict("steam_apps_dict.json")
    
    if not apps_dict:
        print("No apps dictionary found. Exiting.")
        exit(1)
    
    print(f"Found {len(apps_dict)} apps in steam_apps_dict.json")
    
    # Setup configuration
    delay_between_requests, batch_size, max_apps = setup_configuration()
    
    try:
        # Run local validation phase (NO API CALLS)
        missing_app_ids = run_local_validation_phase(apps_dict)
        
        # Apply max_apps limit if specified
        if max_apps and len(missing_app_ids) > max_apps:
            missing_app_ids = missing_app_ids[:max_apps]
            print(f"Limited to {max_apps} apps due to configuration")
        
        if missing_app_ids:
            print(f"Will process {len(missing_app_ids)} apps with {delay_between_requests}s delay between requests")
            print(f"Batch size: {batch_size}")
        
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
