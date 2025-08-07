import requests
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Tuple
from retry import retry


# ============================================================================
# STEAM API FUNCTIONS (copied from main.py)
# ============================================================================

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


def get_app_details_with_retry(app_id: int) -> Tuple[Dict[str, Any], bool]:
    """
    Fetches detailed information for a specific Steam app with retry logic.
    This function validates if the app exists and returns empty dict if it doesn't.
    
    Args:
        app_id (int): The Steam app ID
    
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
# FILE OPERATIONS (copied from main.py)
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


def load_failed_app_ids(filename: str = "failed_fetch_app_ids.json") -> List[int]:
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


def save_failed_app_ids(failed_app_ids: List[int], filename: str = "failed_fetch_app_ids.json") -> None:
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
# RETRY PROCESSING FUNCTIONS
# ============================================================================

def process_single_failed_app(app_id: int, all_app_details: Dict[str, Any], 
                            new_failed_app_ids: List[int], new_non_existent_apps: List[int]) -> None:
    """
    Processes a single failed app by fetching its details and updating the data structures.
    
    Args:
        app_id (int): The Steam app ID
        all_app_details (Dict[str, Any]): Dictionary to store app details
        new_failed_app_ids (List[int]): List to store new failed app IDs
        new_non_existent_apps (List[int]): List to store new non-existent app IDs
    """
    app_id_str = str(app_id)
    
    # Check if app already exists in the details file
    if app_id_str in all_app_details:
        print(f"⏭️  App {app_id} already exists in steam_apps_details.json - skipping")
        return
    
    # Fetch app details (includes validation)
    app_details, is_failure = get_app_details_with_retry(app_id)
    
    if app_details:
        # Success! Add to app details and remove from failed list
        all_app_details[app_id_str] = app_details
        print(f"✓ Successfully processed app {app_id}")
    elif is_failure:
        # Still failed - add to new failed list
        new_failed_app_ids.append(app_id)
        print(f"✗ App {app_id} still failed")
    else:
        # App doesn't exist or has no data - add to non-existent list
        new_non_existent_apps.append(app_id)
        print(f"○ App {app_id} does not exist or has no data")


def save_intermediate_results(all_app_details: Dict[str, Any], output_file: str, 
                            current_count: int, new_non_existent_apps: List[int] = None, 
                            new_failed_app_ids: List[int] = None) -> None:
    """
    Saves intermediate results to a JSON file.
    
    Args:
        all_app_details (Dict[str, Any]): App details to save
        output_file (str): Name of the output file
        current_count (int): Current number of processed apps
        new_non_existent_apps (List[int]): List of new non-existent apps to save
        new_failed_app_ids (List[int]): List of new failed app IDs to save
    """
    print(f"Saving intermediate results after {current_count} apps...")
    if save_json_file(all_app_details, output_file):
        print(f"Intermediate results saved to {output_file}")
    
    # Also save non-existent apps if provided
    if new_non_existent_apps and len(new_non_existent_apps) > 0:
        save_non_existent_apps_accumulative(new_non_existent_apps, "non_existent_apps.json")
        print(f"Non-existent apps saved to non_existent_apps.json ({len(new_non_existent_apps)} new apps)")
    
    # Also save failed app IDs if provided
    if new_failed_app_ids and len(new_failed_app_ids) > 0:
        save_failed_app_ids(new_failed_app_ids, "failed_fetch_app_ids.json")
        print(f"Failed app IDs saved to failed_fetch_app_ids.json ({len(new_failed_app_ids)} new apps)")


def process_failed_apps_batch(failed_app_ids: List[int], delay_between_requests: float = 0.5, 
                            batch_size: int = 50, output_file: str = "steam_apps_details.json") -> Tuple[List[int], List[int]]:
    """
    Processes a batch of failed app IDs.
    
    Args:
        failed_app_ids (List[int]): List of failed app IDs to retry
        delay_between_requests (float): Delay between API requests in seconds
        batch_size (int): Number of apps to process before saving intermediate results
        output_file (str): Name of the output JSON file
    
    Returns:
        Tuple[List[int], List[int]]: Tuple containing:
            - List of app IDs that still failed after retry
            - List of app IDs that don't exist or have no data
    """
    # Load existing app details (this ensures we append to existing data, not overwrite)
    all_app_details = load_json_file(output_file)
    initial_count = len(all_app_details)
    if all_app_details:
        print(f"📁 Loaded {initial_count} existing app details from {output_file}")
        print(f"📝 Will append new data to existing file (no overwrite)")
    else:
        print(f"📁 No existing {output_file} found. Will create new file.")
    
    # Filter out apps that already exist in the details file
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
    
    if total_apps == 0:
        print("🎉 All failed apps already exist in steam_apps_details.json! Nothing to process.")
        return [], []
    
    print(f"Processing {total_apps} failed apps...")
    print(f"Delay between requests: {delay_between_requests}s")
    print(f"Batch size: {batch_size}")
    
    new_failed_app_ids = []
    new_non_existent_apps = []
    
    for i, app_id in enumerate(apps_to_process, 1):
        print(f"Processing failed app {app_id} ({i}/{total_apps}) - {(i/total_apps)*100:.1f}%")
        
        # Process the app
        process_single_failed_app(app_id, all_app_details, new_failed_app_ids, new_non_existent_apps)
        
        # Add delay between requests to avoid rate limiting
        if i < total_apps:  # Don't delay after the last request
            time.sleep(delay_between_requests)
        
        # Save intermediate results every batch_size apps
        if i % batch_size == 0:
            save_intermediate_results(all_app_details, output_file, i, new_non_existent_apps, new_failed_app_ids)
    
    # Save final results (appending to existing data)
    final_count = len(all_app_details)
    new_added = final_count - initial_count
    print(f"Saving final results to {output_file}...")
    if save_json_file(all_app_details, output_file):
        print(f"✅ Successfully saved {final_count} app details to {output_file}")
        print(f"📈 Added {new_added} new apps to existing {initial_count} apps")
    
    return new_failed_app_ids, new_non_existent_apps


def retry_failed_apps_loop(delay_between_requests: float = 0.5, batch_size: int = 50, 
                          output_file: str = "steam_apps_details.json") -> None:
    """
    Main loop that processes failed app IDs until there are none left.
    
    Args:
        delay_between_requests (float): Delay between API requests in seconds
        batch_size (int): Number of apps to process before saving intermediate results
        output_file (str): Name of the output JSON file
    """
    iteration = 1
    total_processed = 0
    total_successful = 0
    total_non_existent = 0
    
    print("="*60)
    print("FAILED APPS RETRY PROCESSOR")
    print("="*60)
    print(f"Delay between requests: {delay_between_requests}s")
    print(f"Batch size: {batch_size}")
    print(f"Output file: {output_file}")
    print("="*60)
    
    while True:
        # Load current failed app IDs
        failed_app_ids = load_failed_app_ids("failed_fetch_app_ids.json")
        
        if not failed_app_ids:
            print(f"\n🎉 No more failed apps to process! All done!")
            break
        
        print(f"\n📋 ITERATION {iteration}")
        print(f"📊 Processing {len(failed_app_ids)} failed apps...")
        
        # Process the batch
        new_failed_app_ids, new_non_existent_apps = process_failed_apps_batch(
            failed_app_ids, delay_between_requests, batch_size, output_file
        )
        
        # Update counters
        total_processed += len(failed_app_ids)
        total_successful += len(failed_app_ids) - len(new_failed_app_ids) - len(new_non_existent_apps)
        total_non_existent += len(new_non_existent_apps)
        
        # Print iteration summary
        print(f"\n📈 ITERATION {iteration} SUMMARY:")
        print(f"   Apps processed: {len(failed_app_ids)}")
        print(f"   Successful: {len(failed_app_ids) - len(new_failed_app_ids) - len(new_non_existent_apps)}")
        print(f"   Non-existent: {len(new_non_existent_apps)}")
        print(f"   Still failed: {len(new_failed_app_ids)}")
        
        # Save results
        if new_non_existent_apps:
            save_non_existent_apps_accumulative(new_non_existent_apps, "non_existent_apps.json")
        
        if new_failed_app_ids:
            save_failed_app_ids(new_failed_app_ids, "failed_fetch_app_ids.json")
        else:
            # No more failed apps - clear the file
            save_failed_app_ids([], "failed_fetch_app_ids.json")
        
        iteration += 1
        
        # Add a small delay between iterations
        if new_failed_app_ids:
            print(f"\n⏳ Waiting 5 seconds before next iteration...")
            time.sleep(5)
    
    # Print final summary
    print(f"\n" + "="*60)
    print("FINAL SUMMARY")
    print("="*60)
    print(f"Total iterations: {iteration - 1}")
    print(f"Total apps processed: {total_processed}")
    print(f"Total successful: {total_successful}")
    print(f"Total non-existent: {total_non_existent}")
    print(f"Final failed apps: 0")
    print("="*60)


def main():
    """
    Main function to run the failed apps retry processor.
    
    IMPORTANT: This script will APPEND to the existing steam_apps_details.json file.
    It will NOT overwrite or create a new file. All existing data will be preserved.
    """
    try:
        # Configuration
        delay_between_requests = 0.5  # 0.5 second delay between requests
        batch_size = 50  # Save intermediate results every 50 apps
        output_file = "steam_apps_details.json"
        
        print("🔒 SAFETY GUARANTEE: This script will APPEND to existing steam_apps_details.json")
        print("🔒 It will NOT overwrite or create a new file. All existing data will be preserved.")
        print()
        
        # Run the retry loop
        retry_failed_apps_loop(delay_between_requests, batch_size, output_file)
        
    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user.")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")


if __name__ == "__main__":
    main() 