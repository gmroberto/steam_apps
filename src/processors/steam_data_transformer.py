#!/usr/bin/env python3
"""
Steam Data Transformer - Data Processing and Transformation Module

This module handles processing and transformation of raw Steam API data.
Designed for use in ETL pipelines where extraction, processing, and loading are separate steps.
"""

from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Tuple
from config.config_manager import get_config

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class SteamDataTransformer:
    """
    Data transformation class for Steam API data.
    
    This class focuses on processing, cleaning, and transforming raw Steam data
    without handling extraction or saving operations.
    """
    
    def __init__(self):
        """Initialize the Steam data transformer."""
        self.processed_count = 0
        self.skipped_count = 0
        self.error_count = 0
    
    def clean_app_details(self, app_details: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and standardize individual app details.
        
        Args:
            app_details (Dict[str, Any]): Raw app details from Steam API
            
        Returns:
            Dict[str, Any]: Cleaned and standardized app details
        """
        if not app_details:
            return {}
        
        cleaned_details = {}
        
        try:
            # Basic app information
            cleaned_details['app_id'] = app_details.get('steam_appid')
            cleaned_details['name'] = app_details.get('name', '').strip()
            cleaned_details['type'] = app_details.get('type', 'unknown')
            cleaned_details['is_free'] = app_details.get('is_free', False)
            
            # Detailed description
            cleaned_details['short_description'] = app_details.get('short_description', '').strip()
            cleaned_details['detailed_description'] = app_details.get('detailed_description', '').strip()
            
            # Release information
            release_date = app_details.get('release_date', {})
            cleaned_details['release_date'] = {
                'coming_soon': release_date.get('coming_soon', False),
                'date': release_date.get('date', '').strip()
            }
            
            # Developer and publisher info
            cleaned_details['developers'] = app_details.get('developers', [])
            cleaned_details['publishers'] = app_details.get('publishers', [])
            
            # Categories and genres
            categories = app_details.get('categories', [])
            cleaned_details['categories'] = [cat.get('description', '') for cat in categories if isinstance(cat, dict)]
            
            genres = app_details.get('genres', [])
            cleaned_details['genres'] = [genre.get('description', '') for genre in genres if isinstance(genre, dict)]
            
            # Platform support
            platforms = app_details.get('platforms', {})
            cleaned_details['platforms'] = {
                'windows': platforms.get('windows', False),
                'mac': platforms.get('mac', False),
                'linux': platforms.get('linux', False)
            }
            
            # Price information
            price_overview = app_details.get('price_overview', {})
            if price_overview:
                cleaned_details['price'] = {
                    'currency': price_overview.get('currency', ''),
                    'initial': price_overview.get('initial', 0),
                    'final': price_overview.get('final', 0),
                    'discount_percent': price_overview.get('discount_percent', 0),
                    'initial_formatted': price_overview.get('initial_formatted', ''),
                    'final_formatted': price_overview.get('final_formatted', '')
                }
            else:
                cleaned_details['price'] = None
            
            # Metacritic score
            metacritic = app_details.get('metacritic', {})
            if metacritic:
                cleaned_details['metacritic_score'] = metacritic.get('score')
                cleaned_details['metacritic_url'] = metacritic.get('url', '')
            else:
                cleaned_details['metacritic_score'] = None
                cleaned_details['metacritic_url'] = ''
            
            # Age requirements
            cleaned_details['required_age'] = app_details.get('required_age', 0)
            
            # Content descriptors
            content_descriptors = app_details.get('content_descriptors', {})
            cleaned_details['content_descriptors'] = content_descriptors.get('notes', []) if content_descriptors else []
            
            # Screenshots and media
            screenshots = app_details.get('screenshots', [])
            cleaned_details['screenshot_count'] = len(screenshots)
            cleaned_details['has_screenshots'] = len(screenshots) > 0
            
            movies = app_details.get('movies', [])
            cleaned_details['movie_count'] = len(movies)
            cleaned_details['has_movies'] = len(movies) > 0
            
            # Achievements
            achievements = app_details.get('achievements', {})
            if achievements:
                cleaned_details['achievement_count'] = achievements.get('total', 0)
                cleaned_details['has_achievements'] = achievements.get('total', 0) > 0
            else:
                cleaned_details['achievement_count'] = 0
                cleaned_details['has_achievements'] = False
            
            # System requirements
            pc_requirements = app_details.get('pc_requirements', {})
            cleaned_details['has_system_requirements'] = bool(pc_requirements and pc_requirements.get('minimum'))
            
            # Language support
            supported_languages = app_details.get('supported_languages', '')
            cleaned_details['supported_languages'] = supported_languages.strip()
            
            # Add processing metadata
            cleaned_details['processed_at'] = datetime.now().isoformat()
            cleaned_details['data_version'] = '1.0'
            
            return cleaned_details
            
        except Exception as e:
            print(f"Error cleaning app details: {e}")
            return {'error': str(e), 'processed_at': datetime.now().isoformat()}
    
    def process_raw_app_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process raw Steam app data dictionary.
        
        Args:
            raw_data (Dict[str, Any]): Raw app data with app_id -> details mapping
            
        Returns:
            Dict[str, Any]: Processed app data
        """
        print(f"Processing {len(raw_data)} raw app records...")
        
        processed_data = {}
        self.processed_count = 0
        self.skipped_count = 0
        self.error_count = 0
        
        for app_id, app_details in raw_data.items():
            try:
                # Skip metadata entries
                if app_id in ['updated_at', 'processed_at', 'metadata']:
                    continue
                
                # Clean and process the app details
                cleaned_details = self.clean_app_details(app_details)
                
                if cleaned_details and 'error' not in cleaned_details:
                    processed_data[app_id] = cleaned_details
                    self.processed_count += 1
                else:
                    self.error_count += 1
                    print(f"Error processing app {app_id}")
                    
            except Exception as e:
                self.error_count += 1
                print(f"Exception processing app {app_id}: {e}")
        
        # Add processing metadata
        processed_data['processing_metadata'] = {
            'processed_at': datetime.now().isoformat(),
            'total_processed': self.processed_count,
            'total_errors': self.error_count,
            'total_skipped': self.skipped_count,
            'processing_version': '1.0'
        }
        
        print(f"Processing completed:")
        print(f"  Successfully processed: {self.processed_count} apps")
        print(f"  Errors: {self.error_count} apps")
        print(f"  Skipped: {self.skipped_count} apps")
        
        return processed_data
    
    def filter_by_criteria(self, processed_data: Dict[str, Any], 
                          criteria: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter processed data based on specified criteria.
        
        Args:
            processed_data (Dict[str, Any]): Processed app data
            criteria (Dict[str, Any]): Filtering criteria
            
        Returns:
            Dict[str, Any]: Filtered app data
        """
        filtered_data = {}
        filtered_count = 0
        
        # Extract filtering criteria
        min_metacritic_score = criteria.get('min_metacritic_score')
        required_platforms = criteria.get('platforms', [])
        required_genres = criteria.get('genres', [])
        max_price = criteria.get('max_price')
        min_achievement_count = criteria.get('min_achievement_count')
        app_types = criteria.get('app_types', [])
        
        print(f"Applying filters to {len(processed_data)} apps...")
        
        for app_id, app_details in processed_data.items():
            # Skip metadata
            if app_id in ['processing_metadata', 'updated_at']:
                filtered_data[app_id] = app_details
                continue
            
            try:
                # Apply filters
                if self._app_meets_criteria(app_details, criteria):
                    filtered_data[app_id] = app_details
                    filtered_count += 1
                    
            except Exception as e:
                print(f"Error filtering app {app_id}: {e}")
        
        print(f"Filtering completed: {filtered_count} apps match criteria")
        return filtered_data
    
    def _app_meets_criteria(self, app_details: Dict[str, Any], criteria: Dict[str, Any]) -> bool:
        """
        Check if an app meets the specified filtering criteria.
        
        Args:
            app_details (Dict[str, Any]): App details to check
            criteria (Dict[str, Any]): Filtering criteria
            
        Returns:
            bool: True if app meets all criteria
        """
        # Metacritic score filter
        min_metacritic = criteria.get('min_metacritic_score')
        if min_metacritic is not None:
            score = app_details.get('metacritic_score')
            if not score or score < min_metacritic:
                return False
        
        # Platform filter
        required_platforms = criteria.get('platforms', [])
        if required_platforms:
            app_platforms = app_details.get('platforms', {})
            for platform in required_platforms:
                if not app_platforms.get(platform, False):
                    return False
        
        # Genre filter
        required_genres = criteria.get('genres', [])
        if required_genres:
            app_genres = app_details.get('genres', [])
            for genre in required_genres:
                if genre not in app_genres:
                    return False
        
        # Price filter
        max_price = criteria.get('max_price')
        if max_price is not None:
            price_info = app_details.get('price')
            if price_info and price_info.get('final', 0) > max_price:
                return False
        
        # Achievement count filter
        min_achievements = criteria.get('min_achievement_count')
        if min_achievements is not None:
            achievement_count = app_details.get('achievement_count', 0)
            if achievement_count < min_achievements:
                return False
        
        # App type filter
        app_types = criteria.get('app_types', [])
        if app_types:
            app_type = app_details.get('type', 'unknown')
            if app_type not in app_types:
                return False
        
        return True
    
    def aggregate_statistics(self, processed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate aggregate statistics from processed data.
        
        Args:
            processed_data (Dict[str, Any]): Processed app data
            
        Returns:
            Dict[str, Any]: Statistics and aggregations
        """
        stats = {
            'total_apps': 0,
            'free_apps': 0,
            'paid_apps': 0,
            'app_types': {},
            'platforms': {'windows': 0, 'mac': 0, 'linux': 0},
            'genres': {},
            'developers': {},
            'publishers': {},
            'price_ranges': {
                'free': 0,
                'under_10': 0,
                'under_25': 0,
                'under_50': 0,
                'over_50': 0
            },
            'metacritic_scores': {
                'total_with_score': 0,
                'average_score': 0,
                'score_ranges': {
                    'excellent_90_plus': 0,
                    'good_75_89': 0,
                    'average_60_74': 0,
                    'poor_below_60': 0
                }
            }
        }
        
        total_metacritic_score = 0
        
        for app_id, app_details in processed_data.items():
            # Skip metadata
            if app_id in ['processing_metadata', 'updated_at']:
                continue
            
            try:
                stats['total_apps'] += 1
                
                # Free vs paid
                if app_details.get('is_free', False):
                    stats['free_apps'] += 1
                else:
                    stats['paid_apps'] += 1
                
                # App types
                app_type = app_details.get('type', 'unknown')
                stats['app_types'][app_type] = stats['app_types'].get(app_type, 0) + 1
                
                # Platforms
                platforms = app_details.get('platforms', {})
                for platform in ['windows', 'mac', 'linux']:
                    if platforms.get(platform, False):
                        stats['platforms'][platform] += 1
                
                # Genres
                genres = app_details.get('genres', [])
                for genre in genres:
                    stats['genres'][genre] = stats['genres'].get(genre, 0) + 1
                
                # Developers
                developers = app_details.get('developers', [])
                for dev in developers:
                    stats['developers'][dev] = stats['developers'].get(dev, 0) + 1
                
                # Publishers
                publishers = app_details.get('publishers', [])
                for pub in publishers:
                    stats['publishers'][pub] = stats['publishers'].get(pub, 0) + 1
                
                # Price ranges
                price_info = app_details.get('price')
                if app_details.get('is_free', False):
                    stats['price_ranges']['free'] += 1
                elif price_info:
                    final_price = price_info.get('final', 0) / 100  # Convert from cents
                    if final_price < 10:
                        stats['price_ranges']['under_10'] += 1
                    elif final_price < 25:
                        stats['price_ranges']['under_25'] += 1
                    elif final_price < 50:
                        stats['price_ranges']['under_50'] += 1
                    else:
                        stats['price_ranges']['over_50'] += 1
                
                # Metacritic scores
                metacritic_score = app_details.get('metacritic_score')
                if metacritic_score:
                    stats['metacritic_scores']['total_with_score'] += 1
                    total_metacritic_score += metacritic_score
                    
                    if metacritic_score >= 90:
                        stats['metacritic_scores']['score_ranges']['excellent_90_plus'] += 1
                    elif metacritic_score >= 75:
                        stats['metacritic_scores']['score_ranges']['good_75_89'] += 1
                    elif metacritic_score >= 60:
                        stats['metacritic_scores']['score_ranges']['average_60_74'] += 1
                    else:
                        stats['metacritic_scores']['score_ranges']['poor_below_60'] += 1
                        
            except Exception as e:
                print(f"Error generating stats for app {app_id}: {e}")
        
        # Calculate average Metacritic score
        if stats['metacritic_scores']['total_with_score'] > 0:
            stats['metacritic_scores']['average_score'] = total_metacritic_score / stats['metacritic_scores']['total_with_score']
        
        # Add generation metadata
        stats['generated_at'] = datetime.now().isoformat()
        
        return stats


# Main wrapper function for Airflow
def run_steam_processing() -> Dict[str, Any]:
    """
    Main processing function for Airflow.
    Reads raw data from JSON files, processes it, and saves back to files.
    """
    from src.utils.file_operations import FileManager
    
    file_manager = FileManager()
    transformer = SteamDataTransformer()
    
    # Load raw data from JSON file
    raw_data = file_manager.load_json_file("steam_apps_details.json")
    
    if not raw_data:
        return {"status": "error", "message": "No raw data found in steam_apps_details.json"}
    
    # Process the raw data
    processed_data = transformer.process_raw_app_data(raw_data)
    
    if not processed_data:
        return {"status": "error", "message": "Processing failed"}
    
    # Save processed data back to file
    file_manager.save_json_file(processed_data, "steam_apps_details.json")
    
    # Generate and save statistics
    statistics = transformer.aggregate_statistics(processed_data)
    file_manager.save_json_file(statistics, "steam_processing_statistics.json")
    
    return {
        "status": "success",
        "message": f"Processed {transformer.processed_count} apps",
        "total_processed": transformer.processed_count,
        "processing_errors": transformer.error_count
    }


# Convenience functions for backward compatibility
def process_steam_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process raw Steam data using default settings."""
    transformer = SteamDataTransformer()
    return transformer.process_raw_app_data(raw_data)


def filter_steam_data(processed_data: Dict[str, Any], criteria: Dict[str, Any]) -> Dict[str, Any]:
    """Filter processed Steam data using specified criteria."""
    transformer = SteamDataTransformer()
    return transformer.filter_by_criteria(processed_data, criteria)


def generate_steam_statistics(processed_data: Dict[str, Any]) -> Dict[str, Any]:
    """Generate statistics from processed Steam data."""
    transformer = SteamDataTransformer()
    return transformer.aggregate_statistics(processed_data)
