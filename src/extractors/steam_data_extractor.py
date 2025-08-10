#!/usr/bin/env python3
"""
Steam Data Extractor - Pure Data Extraction Module

This module handles pure data extraction from Steam API without processing or saving.
Designed for use in ETL pipelines where extraction, processing, and loading are separate steps.
"""

import time
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
from src.extractors.steam_api_client import SteamApiClient
from config.config_manager import get_config

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class SteamDataExtractor:
    """
    Pure data extraction class for Steam API data.
    
    This class focuses solely on extracting data from Steam API without
    any processing, transformation, or saving operations.
    """
    
    def __init__(self, timeout: int = None, delay: float = None):
        """
        Initialize the Steam data extractor.
        
        Args:
            timeout (int): API request timeout in seconds
            delay (float): Delay between API requests in seconds
        """
        self.timeout = timeout or get_config('steam_api_client.timeout', 30)
        self.delay = delay or get_config('steam_api_client.delay', 0.5)
        self.api_client = SteamApiClient(default_timeout=self.timeout)
    
    def extract_app_list(self) -> List[Dict[str, Any]]:
        """
        Extract the complete list of Steam apps.
        
        Returns:
            List[Dict[str, Any]]: List of apps with appid and name
        """
        print("Extracting Steam app list...")
        apps = self.api_client.get_app_list()
        
        if apps:
            print(f"Successfully extracted {len(apps)} apps from Steam API")
        else:
            print("Failed to extract app list from Steam API")
        
        return apps or []
    
    def extract_app_details(self, app_id: int) -> Tuple[Dict[str, Any], bool]:
        """
        Extract detailed information for a single Steam app.
        
        Args:
            app_id (int): Steam app ID
            
        Returns:
            Tuple[Dict[str, Any], bool]: App details and failure flag
                - App details (empty dict if failed/non-existent)
                - True if failed due to error, False if app doesn't exist
        """
        return self.api_client.get_app_details_with_failure_info(app_id)
    
    def extract_multiple_app_details(self, app_ids: List[int], 
                                   delay_between_requests: float = None,
                                   progress_callback: Optional[callable] = None) -> Tuple[Dict[str, Any], List[int], List[int]]:
        """
        Extract detailed information for multiple Steam apps.
        
        Args:
            app_ids (List[int]): List of Steam app IDs
            delay_between_requests (float): Delay between requests (uses instance default if None)
            progress_callback (callable): Optional callback for progress updates
            
        Returns:
            Tuple containing:
                - Dict[str, Any]: Successfully extracted app details (app_id -> details)
                - List[int]: App IDs that failed due to errors
                - List[int]: App IDs that don't exist or have no data
        """
        if delay_between_requests is None:
            delay_between_requests = self.delay
        
        print(f"Extracting details for {len(app_ids)} apps...")
        print(f"Using {delay_between_requests}s delay between requests")
        
        extracted_details = {}
        failed_app_ids = []
        non_existent_apps = []
        
        total_apps = len(app_ids)
        
        for i, app_id in enumerate(app_ids, 1):
            # Progress update
            progress_percent = (i / total_apps) * 100
            print(f"Extracting app {app_id} ({i}/{total_apps}) - {progress_percent:.1f}%")
            
            # Extract app details
            app_details, is_failure = self.extract_app_details(app_id)
            
            if app_details:
                extracted_details[str(app_id)] = app_details
            elif is_failure:
                failed_app_ids.append(app_id)
            else:
                non_existent_apps.append(app_id)
            
            # Progress callback
            if progress_callback:
                progress_callback(i, total_apps, app_id, bool(app_details))
            
            # Rate limiting delay
            if i < total_apps:  # Don't delay after the last request
                time.sleep(delay_between_requests)
        
        print(f"\nExtraction completed:")
        print(f"  Successfully extracted: {len(extracted_details)} apps")
        print(f"  Failed extractions: {len(failed_app_ids)} apps")
        print(f"  Non-existent apps: {len(non_existent_apps)} apps")
        
        return extracted_details, failed_app_ids, non_existent_apps
    
    def extract_app_ids_only(self) -> List[int]:
        """
        Extract only the app IDs as a list of integers.
        
        Returns:
            List[int]: List of Steam app IDs
        """
        apps = self.extract_app_list()
        return [app.get('appid') for app in apps if app.get('appid') is not None]
    
    def create_apps_dictionary(self) -> Dict[str, str]:
        """
        Extract and create a dictionary of app_id -> app_name.
        
        Returns:
            Dict[str, str]: Dictionary with app IDs as keys and names as values
        """
        print("Creating apps dictionary from extracted data...")
        apps = self.extract_app_list()
        
        if not apps:
            print("No apps extracted.")
            return {}
        
        apps_dict = {}
        for app in apps:
            app_id = app.get('appid')
            name = app.get('name', 'Unknown')
            if app_id is not None:
                apps_dict[str(app_id)] = name
        
        print(f"Created dictionary with {len(apps_dict)} apps")
        return apps_dict
    
    def extract_missing_app_details(self, existing_details: Dict[str, Any], 
                                  all_app_ids: List[int],
                                  non_existent_apps: List[int] = None) -> List[int]:
        """
        Determine which app IDs are missing from existing details.
        
        Args:
            existing_details (Dict[str, Any]): Already extracted app details
            all_app_ids (List[int]): Complete list of app IDs to check
            non_existent_apps (List[int]): List of known non-existent apps to skip
            
        Returns:
            List[int]: App IDs that need to be extracted
        """
        non_existent_set = set(non_existent_apps or [])
        missing_app_ids = []
        
        for app_id in all_app_ids:
            app_id_str = str(app_id)
            if app_id not in non_existent_set and app_id_str not in existing_details:
                missing_app_ids.append(app_id)
        
        print(f"Found {len(missing_app_ids)} apps that need extraction")
        return missing_app_ids


# Main wrapper function for Airflow
def run_steam_extraction() -> Dict[str, Any]:
    """
    Main extraction function for Airflow.
    Handles the complete extraction workflow.
    """
    extractor = SteamDataExtractor()
    
    # Extract apps dictionary
    apps_dict = extractor.create_apps_dictionary()
    if not apps_dict:
        return {"status": "error", "message": "Failed to extract apps dictionary"}
    
    # Get sample of app IDs to extract (first 100 for demo)
    all_app_ids = [int(app_id) for app_id in apps_dict.keys() if app_id != 'updated_at']
    sample_app_ids = all_app_ids[:100] if len(all_app_ids) > 100 else all_app_ids
    
    # Extract app details
    extracted_details, failed_ids, non_existent_ids = extractor.extract_multiple_app_details(
        sample_app_ids, delay_between_requests=0.1
    )
    
    return {
        "status": "success",
        "extracted_data": extracted_details,
        "total_extracted": len(extracted_details),
        "failed_count": len(failed_ids),
        "non_existent_count": len(non_existent_ids)
    }


# Convenience functions for backward compatibility
def extract_steam_app_list() -> List[Dict[str, Any]]:
    """Extract Steam app list using default settings."""
    extractor = SteamDataExtractor()
    return extractor.extract_app_list()


def extract_steam_apps_dictionary() -> Dict[str, str]:
    """Extract Steam apps as dictionary using default settings."""
    extractor = SteamDataExtractor()
    return extractor.create_apps_dictionary()


def extract_steam_app_details_batch(app_ids: List[int], 
                                  delay: float = 0.5) -> Tuple[Dict[str, Any], List[int], List[int]]:
    """Extract multiple app details using default settings."""
    extractor = SteamDataExtractor()
    return extractor.extract_multiple_app_details(app_ids, delay)
