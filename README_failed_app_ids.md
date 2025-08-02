# Failed App IDs Tracking

This document describes the new functionality for tracking and exporting failed app IDs in the Steam app data fetcher.

## Overview

The application now tracks app IDs that fail after all retry attempts and exports them to separate JSON files. This helps identify problematic apps that may need manual investigation or retry.

## New Features

### 1. Failed App IDs Tracking

The system now distinguishes between:
- **Non-existent apps**: Apps that don't exist in Steam's database (not considered failures)
- **Failed apps**: Apps that failed due to network errors, rate limiting, or other issues after all retry attempts

### 2. Multiple Output Files

The system generates several JSON files for different types of failures:

- `failed_validation_app_ids.json`: Apps that failed during validation phase
- `failed_fetch_app_ids.json`: Apps that failed during detail fetching phase  
- `all_failed_app_ids.json`: Combined list of all failed apps (unique)

### 3. Enhanced Logging

The main script now provides detailed progress tracking and summary information:
- Validation progress with success/failure indicators
- Fetching progress with detailed status
- Final summary with counts and file locations

## File Structure

Each failed app IDs JSON file contains:

```json
{
  "failed_app_ids": [123456, 789012, 345678],
  "count": 3,
  "exported_at": "2024-01-15T10:30:45.123456",
  "description": "App IDs that failed after all retry attempts"
}
```

## Usage

### Running the Main Script

```bash
python main.py
```

The script will automatically:
1. Create the Steam apps dictionary
2. Validate app IDs and track validation failures
3. Fetch app details and track fetch failures
4. Export all failed app IDs to separate files
5. Display a comprehensive summary

### Testing the Functionality

```bash
python test_failed_app_ids.py
```

This test script validates the functionality with a small subset of apps.

## Output Files

After running the script, you'll find these files:

- `steam_apps_dict.json`: Dictionary of all Steam apps (app_id → name)
- `steam_apps_details.json`: Detailed information for successfully fetched apps
- `failed_validation_app_ids.json`: Apps that failed validation (if any)
- `failed_fetch_app_ids.json`: Apps that failed fetching (if any)
- `all_failed_app_ids.json`: All failed apps combined (if any)

## Error Handling

The system uses retry logic with exponential backoff:
- Validation: Up to 9 retries with 1-60 second delays
- Fetching: Up to 3 retries with 1-60 second delays

Only apps that fail after all retry attempts are considered "failed" and added to the failed app IDs lists.

## Configuration

You can adjust the retry behavior and timing in the main script:

```python
# In main.py
delay_between_requests = 0.1  # Delay between requests
batch_size = 100              # Save intermediate results every N apps
max_apps = None               # Limit processing (set to number to limit)
```

## Troubleshooting

If you encounter issues:

1. Check the console output for detailed error messages
2. Verify that the Steam API is accessible
3. Adjust the `delay_between_requests` if you're getting rate limited
4. Check the generated JSON files for specific failed app IDs

## Example Output

```
==================================================
COMPLETION SUMMARY
==================================================
Successfully fetched details for 12450 apps
Validation failures: 23 apps
Fetch failures: 15 apps
Total failures: 35 apps

Files created:
- steam_apps_details.json: App details
- failed_validation_app_ids.json: Apps that failed validation
- failed_fetch_app_ids.json: Apps that failed fetching
- all_failed_app_ids.json: All failed apps combined
``` 