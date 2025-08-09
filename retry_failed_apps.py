import requests
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Tuple
from retry import retry
from steam_api_processor import SteamApiProcessor
from file_manager import FileManager
from config_loader import get_config


# ============================================================================
# CONFIGURATION - Loaded from config.yml with fallback defaults
# ============================================================================

# Default delay between API requests (in seconds)
DEFAULT_DELAY_BETWEEN_REQUESTS = get_config('retry_failed_apps.delay_between_requests', 0.5)

# Default batch size for saving intermediate results
DEFAULT_BATCH_SIZE = get_config('retry_failed_apps.batch_size', 50)

# Default output file name
DEFAULT_OUTPUT_FILE = get_config('retry_failed_apps.files.output_file', 'steam_apps_details.json')

# Default failed app IDs file name
DEFAULT_FAILED_FETCH_FILE = get_config('retry_failed_apps.files.failed_fetch_file', 'failed_fetch_app_ids.json')

# Default non-existent apps file name
DEFAULT_NON_EXISTENT_FILE = get_config('retry_failed_apps.files.non_existent_file', 'non_existent_apps.json')

# Wait time between retry iterations (in seconds)
ITERATION_WAIT_TIME = get_config('retry_failed_apps.iteration_wait_time', 5)

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

def load_failed_app_ids(filename: str = DEFAULT_FAILED_FETCH_FILE) -> List[int]:
    return file_manager.load_failed_app_ids(filename)

def save_failed_app_ids(failed_app_ids: List[int], filename: str = DEFAULT_FAILED_FETCH_FILE) -> None:
    return file_manager.save_failed_app_ids(failed_app_ids, filename)

def save_non_existent_apps(non_existent_apps: List[int], filename: str = DEFAULT_NON_EXISTENT_FILE) -> None:
    return file_manager.save_non_existent_apps(non_existent_apps, filename)

def save_non_existent_apps_accumulative(new_non_existent_apps: List[int], filename: str = DEFAULT_NON_EXISTENT_FILE) -> None:
    return file_manager.save_non_existent_apps_accumulative(new_non_existent_apps, filename)

def load_non_existent_apps(filename: str = DEFAULT_NON_EXISTENT_FILE) -> List[int]:
    return file_manager.load_non_existent_apps(filename)


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
    # Use the steam_api instance to process the failed app
    steam_api.process_single_app(app_id, all_app_details, new_failed_app_ids, new_non_existent_apps, is_retry=True)


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
    # Use the file_manager instance to save intermediate results
    file_manager.save_intermediate_results(all_app_details, output_file, current_count, new_non_existent_apps, new_failed_app_ids)


def process_failed_apps_batch(failed_app_ids: List[int], delay_between_requests: float = DEFAULT_DELAY_BETWEEN_REQUESTS, 
                            batch_size: int = DEFAULT_BATCH_SIZE, output_file: str = DEFAULT_OUTPUT_FILE) -> Tuple[List[int], List[int]]:
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
    # Use the steam_api instance to process the failed apps batch
    return steam_api.process_failed_apps_batch(
        failed_app_ids=failed_app_ids,
        delay_between_requests=delay_between_requests,
        batch_size=batch_size,
        output_file=output_file,
        file_manager=file_manager
    )


def _print_retry_loop_header(delay_between_requests: float, batch_size: int, output_file: str) -> None:
    """
    Prints the retry loop header information.
    
    Args:
        delay_between_requests (float): Delay between requests
        batch_size (int): Batch size
        output_file (str): Output file name
    """
    print("="*60)
    print("FAILED APPS RETRY PROCESSOR")
    print("="*60)
    print(f"Delay between requests: {delay_between_requests}s")
    print(f"Batch size: {batch_size}")
    print(f"Output file: {output_file}")
    print("="*60)

def _check_for_failed_apps() -> List[int]:
    """
    Checks for failed app IDs that need processing.
    
    Returns:
        List[int]: List of failed app IDs, empty if none found
    """
    failed_app_ids = load_failed_app_ids(DEFAULT_FAILED_FETCH_FILE)
    
    if not failed_app_ids:
        print(f"\n🎉 No more failed apps to process! All done!")
    
    return failed_app_ids

def _print_iteration_header(iteration: int, failed_app_count: int) -> None:
    """
    Prints iteration header information.
    
    Args:
        iteration (int): Current iteration number
        failed_app_count (int): Number of failed apps to process
    """
    print(f"\n📋 ITERATION {iteration}")
    print(f"📊 Processing {failed_app_count} failed apps...")

def _calculate_iteration_stats(failed_app_ids: List[int], new_failed_app_ids: List[int], 
                              new_non_existent_apps: List[int]) -> int:
    """
    Calculates statistics for the current iteration.
    
    Args:
        failed_app_ids (List[int]): Original failed app IDs
        new_failed_app_ids (List[int]): New failed app IDs
        new_non_existent_apps (List[int]): New non-existent app IDs
        
    Returns:
        int: Number of successful apps in this iteration
    """
    return len(failed_app_ids) - len(new_failed_app_ids) - len(new_non_existent_apps)

def _print_iteration_summary(iteration: int, failed_app_ids: List[int], 
                            new_failed_app_ids: List[int], new_non_existent_apps: List[int]) -> None:
    """
    Prints summary for the current iteration.
    
    Args:
        iteration (int): Current iteration number
        failed_app_ids (List[int]): Original failed app IDs
        new_failed_app_ids (List[int]): New failed app IDs
        new_non_existent_apps (List[int]): New non-existent app IDs
    """
    successful = _calculate_iteration_stats(failed_app_ids, new_failed_app_ids, new_non_existent_apps)
    
    print(f"\n📈 ITERATION {iteration} SUMMARY:")
    print(f"   Apps processed: {len(failed_app_ids)}")
    print(f"   Successful: {successful}")
    print(f"   Non-existent: {len(new_non_existent_apps)}")
    print(f"   Still failed: {len(new_failed_app_ids)}")

def _save_iteration_results(new_failed_app_ids: List[int], new_non_existent_apps: List[int]) -> None:
    """
    Saves the results from the current iteration.
    
    Args:
        new_failed_app_ids (List[int]): New failed app IDs
        new_non_existent_apps (List[int]): New non-existent app IDs
    """
    if new_non_existent_apps:
        save_non_existent_apps_accumulative(new_non_existent_apps, DEFAULT_NON_EXISTENT_FILE)
    
    if new_failed_app_ids:
        save_failed_app_ids(new_failed_app_ids, DEFAULT_FAILED_FETCH_FILE)
    else:
        # No more failed apps - clear the file
        save_failed_app_ids([], DEFAULT_FAILED_FETCH_FILE)

def _wait_between_iterations(new_failed_app_ids: List[int]) -> None:
    """
    Waits between iterations if there are still failed apps.
    
    Args:
        new_failed_app_ids (List[int]): New failed app IDs
    """
    if new_failed_app_ids:
        print(f"\n⏳ Waiting {ITERATION_WAIT_TIME} seconds before next iteration...")
        time.sleep(ITERATION_WAIT_TIME)

def _print_final_summary(iteration: int, total_processed: int, total_successful: int, total_non_existent: int) -> None:
    """
    Prints the final summary of the retry loop.
    
    Args:
        iteration (int): Final iteration number
        total_processed (int): Total apps processed
        total_successful (int): Total successful apps
        total_non_existent (int): Total non-existent apps
    """
    print(f"\n" + "="*60)
    print("FINAL SUMMARY")
    print("="*60)
    print(f"Total iterations: {iteration - 1}")
    print(f"Total apps processed: {total_processed}")
    print(f"Total successful: {total_successful}")
    print(f"Total non-existent: {total_non_existent}")
    print(f"Final failed apps: 0")
    print("="*60)

def retry_failed_apps_loop(delay_between_requests: float = DEFAULT_DELAY_BETWEEN_REQUESTS, batch_size: int = DEFAULT_BATCH_SIZE, 
                          output_file: str = DEFAULT_OUTPUT_FILE) -> None:
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
    
    # Print header
    _print_retry_loop_header(delay_between_requests, batch_size, output_file)
    
    while True:
        # Check for failed apps
        failed_app_ids = _check_for_failed_apps()
        if not failed_app_ids:
            break
        
        # Print iteration header
        _print_iteration_header(iteration, len(failed_app_ids))
        
        # Process the batch
        new_failed_app_ids, new_non_existent_apps = process_failed_apps_batch(
            failed_app_ids, delay_between_requests, batch_size, output_file
        )
        
        # Update counters
        total_processed += len(failed_app_ids)
        successful = _calculate_iteration_stats(failed_app_ids, new_failed_app_ids, new_non_existent_apps)
        total_successful += successful
        total_non_existent += len(new_non_existent_apps)
        
        # Print iteration summary
        _print_iteration_summary(iteration, failed_app_ids, new_failed_app_ids, new_non_existent_apps)
        
        # Save results
        _save_iteration_results(new_failed_app_ids, new_non_existent_apps)
        
        iteration += 1
        
        # Wait between iterations if needed
        _wait_between_iterations(new_failed_app_ids)
    
    # Print final summary
    _print_final_summary(iteration, total_processed, total_successful, total_non_existent)


def main():
    """
    Main function to run the failed apps retry processor.
    
    IMPORTANT: This script will APPEND to the existing steam_apps_details.json file.
    It will NOT overwrite or create a new file. All existing data will be preserved.
    """
    try:
        # Configuration
        delay_between_requests = DEFAULT_DELAY_BETWEEN_REQUESTS  # Default delay between requests
        batch_size = DEFAULT_BATCH_SIZE  # Default batch size for intermediate saves
        output_file = DEFAULT_OUTPUT_FILE  # Default output file
        
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