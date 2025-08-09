#!/usr/bin/env python3
"""
JSON Saver - Data Saving and File Management Module

This module handles saving processed data to JSON files with various formatting and organization options.
Designed for use in ETL pipelines where extraction, processing, and loading are separate steps.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from src.utils.config_manager import get_config

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class JsonSaver:
    """
    JSON file saving and management class.
    
    This class focuses on saving processed data to JSON files with various
    formatting, organization, and backup options.
    """
    
    def __init__(self, base_output_dir: str = None, encoding: str = None, 
                 indent: int = None, ensure_ascii: bool = None):
        """
        Initialize the JSON saver.
        
        Args:
            base_output_dir (str): Base directory for output files
            encoding (str): File encoding (default: utf-8)
            indent (int): JSON indentation (default: 2)
            ensure_ascii (bool): Whether to ensure ASCII encoding (default: False)
        """
        self.base_output_dir = base_output_dir or get_config('json_saver.output_dir', 'data/processed')
        self.encoding = encoding or get_config('json_saver.encoding', 'utf-8')
        self.indent = indent or get_config('json_saver.indent', 2)
        self.ensure_ascii = ensure_ascii if ensure_ascii is not None else get_config('json_saver.ensure_ascii', False)
        
        # Ensure output directory exists
        self._ensure_directory_exists(self.base_output_dir)
    
    def _ensure_directory_exists(self, directory: str) -> None:
        """
        Ensure that a directory exists, create it if it doesn't.
        
        Args:
            directory (str): Directory path to check/create
        """
        Path(directory).mkdir(parents=True, exist_ok=True)
    
    def _add_save_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add saving metadata to the data.
        
        Args:
            data (Dict[str, Any]): Data to add metadata to
            
        Returns:
            Dict[str, Any]: Data with added metadata
        """
        data_with_metadata = data.copy()
        data_with_metadata['save_metadata'] = {
            'saved_at': datetime.now().isoformat(),
            'saver_version': '1.0',
            'total_records': len([k for k in data.keys() if k not in ['processing_metadata', 'save_metadata', 'updated_at']])
        }
        return data_with_metadata
    
    def save_processed_data(self, processed_data: Dict[str, Any], 
                          filename: str = None,
                          add_timestamp: bool = True,
                          add_metadata: bool = True,
                          create_backup: bool = False) -> str:
        """
        Save processed data to a JSON file.
        
        Args:
            processed_data (Dict[str, Any]): Processed data to save
            filename (str): Output filename (auto-generated if None)
            add_timestamp (bool): Whether to add timestamp to filename
            add_metadata (bool): Whether to add saving metadata
            create_backup (bool): Whether to create backup of existing file
            
        Returns:
            str: Path to the saved file
        """
        # Generate filename if not provided
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'steam_processed_data_{timestamp}.json'
        elif add_timestamp and not self._has_timestamp(filename):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            name, ext = os.path.splitext(filename)
            filename = f'{name}_{timestamp}{ext}'
        
        output_path = os.path.join(self.base_output_dir, filename)
        
        # Create backup if requested and file exists
        if create_backup and os.path.exists(output_path):
            self._create_backup(output_path)
        
        # Add metadata if requested
        data_to_save = processed_data
        if add_metadata:
            data_to_save = self._add_save_metadata(processed_data)
        
        # Save the file
        try:
            with open(output_path, 'w', encoding=self.encoding) as f:
                json.dump(data_to_save, f, indent=self.indent, ensure_ascii=self.ensure_ascii)
            
            record_count = len([k for k in processed_data.keys() if k not in ['processing_metadata', 'save_metadata', 'updated_at']])
            print(f"Successfully saved {record_count} processed records to {output_path}")
            return output_path
            
        except Exception as e:
            print(f"Error saving processed data to {output_path}: {e}")
            raise
    
    def save_by_category(self, processed_data: Dict[str, Any], 
                        category_field: str = 'type',
                        output_subdir: str = 'by_category') -> Dict[str, str]:
        """
        Save processed data split by category into separate files.
        
        Args:
            processed_data (Dict[str, Any]): Processed data to save
            category_field (str): Field to use for categorization
            output_subdir (str): Subdirectory for category files
            
        Returns:
            Dict[str, str]: Mapping of category -> file path
        """
        category_dir = os.path.join(self.base_output_dir, output_subdir)
        self._ensure_directory_exists(category_dir)
        
        # Group data by category
        categories = {}
        for app_id, app_data in processed_data.items():
            # Skip metadata
            if app_id in ['processing_metadata', 'save_metadata', 'updated_at']:
                continue
            
            category = app_data.get(category_field, 'unknown')
            if category not in categories:
                categories[category] = {}
            categories[category][app_id] = app_data
        
        # Save each category to separate file
        saved_files = {}
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for category, category_data in categories.items():
            # Clean category name for filename
            clean_category = self._clean_filename(category)
            filename = f'steam_{clean_category}_{timestamp}.json'
            output_path = os.path.join(category_dir, filename)
            
            # Add metadata
            category_data_with_metadata = self._add_save_metadata(category_data)
            
            try:
                with open(output_path, 'w', encoding=self.encoding) as f:
                    json.dump(category_data_with_metadata, f, indent=self.indent, ensure_ascii=self.ensure_ascii)
                
                saved_files[category] = output_path
                print(f"Saved {len(category_data)} {category} apps to {output_path}")
                
            except Exception as e:
                print(f"Error saving category {category} to {output_path}: {e}")
        
        return saved_files
    
    def save_statistics(self, statistics: Dict[str, Any], 
                       filename: str = None) -> str:
        """
        Save statistics data to a JSON file.
        
        Args:
            statistics (Dict[str, Any]): Statistics data to save
            filename (str): Output filename
            
        Returns:
            str: Path to the saved statistics file
        """
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'steam_statistics_{timestamp}.json'
        
        output_path = os.path.join(self.base_output_dir, filename)
        
        # Add save metadata
        stats_with_metadata = statistics.copy()
        stats_with_metadata['statistics_saved_at'] = datetime.now().isoformat()
        
        try:
            with open(output_path, 'w', encoding=self.encoding) as f:
                json.dump(stats_with_metadata, f, indent=self.indent, ensure_ascii=self.ensure_ascii)
            
            print(f"Successfully saved statistics to {output_path}")
            return output_path
            
        except Exception as e:
            print(f"Error saving statistics to {output_path}: {e}")
            raise
    
    def save_filtered_data(self, filtered_data: Dict[str, Any], 
                          filter_criteria: Dict[str, Any],
                          filename: str = None) -> str:
        """
        Save filtered data with filter criteria information.
        
        Args:
            filtered_data (Dict[str, Any]): Filtered data to save
            filter_criteria (Dict[str, Any]): Criteria used for filtering
            filename (str): Output filename
            
        Returns:
            str: Path to the saved filtered data file
        """
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'steam_filtered_data_{timestamp}.json'
        
        output_path = os.path.join(self.base_output_dir, filename)
        
        # Add filter metadata
        data_with_metadata = filtered_data.copy()
        data_with_metadata['filter_metadata'] = {
            'filter_criteria': filter_criteria,
            'filtered_at': datetime.now().isoformat(),
            'total_after_filter': len([k for k in filtered_data.keys() if k not in ['processing_metadata', 'save_metadata', 'updated_at', 'filter_metadata']])
        }
        
        try:
            with open(output_path, 'w', encoding=self.encoding) as f:
                json.dump(data_with_metadata, f, indent=self.indent, ensure_ascii=self.ensure_ascii)
            
            record_count = data_with_metadata['filter_metadata']['total_after_filter']
            print(f"Successfully saved {record_count} filtered records to {output_path}")
            return output_path
            
        except Exception as e:
            print(f"Error saving filtered data to {output_path}: {e}")
            raise
    
    def save_multiple_formats(self, processed_data: Dict[str, Any],
                            base_filename: str = None,
                            formats: List[str] = None) -> Dict[str, str]:
        """
        Save data in multiple formats (compact, pretty, etc.).
        
        Args:
            processed_data (Dict[str, Any]): Data to save
            base_filename (str): Base filename (without extension)
            formats (List[str]): List of formats ('compact', 'pretty', 'minified')
            
        Returns:
            Dict[str, str]: Mapping of format -> file path
        """
        if formats is None:
            formats = ['pretty', 'compact']
        
        if base_filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_filename = f'steam_data_{timestamp}'
        
        saved_files = {}
        data_with_metadata = self._add_save_metadata(processed_data)
        
        for format_type in formats:
            filename = f'{base_filename}_{format_type}.json'
            output_path = os.path.join(self.base_output_dir, filename)
            
            try:
                if format_type == 'compact':
                    # Compact format - no indentation
                    with open(output_path, 'w', encoding=self.encoding) as f:
                        json.dump(data_with_metadata, f, separators=(',', ':'), ensure_ascii=self.ensure_ascii)
                elif format_type == 'pretty':
                    # Pretty format - nice indentation
                    with open(output_path, 'w', encoding=self.encoding) as f:
                        json.dump(data_with_metadata, f, indent=4, ensure_ascii=self.ensure_ascii)
                elif format_type == 'minified':
                    # Minified format - no spaces
                    with open(output_path, 'w', encoding=self.encoding) as f:
                        json.dump(data_with_metadata, f, separators=(',', ':'), ensure_ascii=self.ensure_ascii)
                
                saved_files[format_type] = output_path
                print(f"Saved {format_type} format to {output_path}")
                
            except Exception as e:
                print(f"Error saving {format_type} format to {output_path}: {e}")
        
        return saved_files
    
    def _has_timestamp(self, filename: str) -> bool:
        """Check if filename already has a timestamp."""
        import re
        timestamp_pattern = r'_\d{8}_\d{6}'
        return bool(re.search(timestamp_pattern, filename))
    
    def _clean_filename(self, name: str) -> str:
        """Clean a string to be safe for use as filename."""
        import re
        # Replace non-alphanumeric characters with underscores
        cleaned = re.sub(r'[^\w\-_]', '_', name)
        # Remove multiple consecutive underscores
        cleaned = re.sub(r'_+', '_', cleaned)
        # Remove leading/trailing underscores
        cleaned = cleaned.strip('_')
        return cleaned.lower()
    
    def _create_backup(self, file_path: str) -> str:
        """Create a backup of an existing file."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = f'{file_path}.backup_{timestamp}'
        
        try:
            import shutil
            shutil.copy2(file_path, backup_path)
            print(f"Created backup: {backup_path}")
            return backup_path
        except Exception as e:
            print(f"Error creating backup of {file_path}: {e}")
            return None
    
    def get_save_summary(self, processed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get a summary of what would be saved without actually saving.
        
        Args:
            processed_data (Dict[str, Any]): Data to analyze
            
        Returns:
            Dict[str, Any]: Summary information
        """
        summary = {
            'total_records': 0,
            'data_size_estimate': 0,
            'record_types': {},
            'has_metadata': False,
            'estimated_file_size_mb': 0
        }
        
        for key, value in processed_data.items():
            if key in ['processing_metadata', 'save_metadata', 'updated_at']:
                summary['has_metadata'] = True
                continue
            
            summary['total_records'] += 1
            
            # Estimate data size
            data_str = json.dumps(value, ensure_ascii=self.ensure_ascii)
            summary['data_size_estimate'] += len(data_str.encode(self.encoding))
            
            # Count record types
            record_type = value.get('type', 'unknown') if isinstance(value, dict) else 'other'
            summary['record_types'][record_type] = summary['record_types'].get(record_type, 0) + 1
        
        # Estimate file size in MB
        summary['estimated_file_size_mb'] = round(summary['data_size_estimate'] / (1024 * 1024), 2)
        
        return summary


# Main wrapper function for Airflow
def run_steam_saving() -> Dict[str, Any]:
    """
    Main saving function for Airflow.
    Reads processed data from JSON files and saves in various formats.
    """
    from src.utils.file_operations import FileManager
    
    file_manager = FileManager()
    saver = JsonSaver()
    
    try:
        # Load processed data from file
        processed_data = file_manager.load_json_file("steam_apps_details.json")
        
        if not processed_data:
            return {"status": "error", "message": "No processed data found in steam_apps_details.json"}
        
        # Save main processed data in different formats
        main_file = saver.save_processed_data(processed_data)
        
        # Save data by type
        category_files = saver.save_by_category(processed_data, 'type', 'by_type')
        
        saved_files = {
            'main': main_file,
            'by_type': category_files
        }
        
        # Load and save statistics if available
        try:
            statistics = file_manager.load_json_file("steam_processing_statistics.json")
            if statistics:
                stats_file = saver.save_statistics(statistics)
                saved_files['statistics'] = stats_file
        except:
            pass  # Statistics file might not exist yet
        
        return {
            "status": "success",
            "message": f"Saved {len(processed_data)} records in multiple formats",
            "saved_files": saved_files,
            "total_records_saved": len(processed_data)
        }
        
    except Exception as e:
        return {"status": "error", "message": f"Saving failed: {str(e)}"}


# Convenience functions for backward compatibility
def save_steam_data_json(processed_data: Dict[str, Any], filename: str = None) -> str:
    """Save processed Steam data using default settings."""
    saver = JsonSaver()
    return saver.save_processed_data(processed_data, filename)


def save_steam_statistics_json(statistics: Dict[str, Any], filename: str = None) -> str:
    """Save Steam statistics using default settings."""
    saver = JsonSaver()
    return saver.save_statistics(statistics, filename)


def save_steam_data_by_type(processed_data: Dict[str, Any]) -> Dict[str, str]:
    """Save Steam data split by app type using default settings."""
    saver = JsonSaver()
    return saver.save_by_category(processed_data, 'type', 'by_type')
