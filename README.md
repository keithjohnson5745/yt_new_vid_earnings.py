# YouTube Analytics Report Generator

A Python script that generates monthly analytics reports for YouTube channels and exports the data to Google Sheets.

## Features

- Generates detailed reports for videos published in a specific month
- Tracks performance of both new videos and the back catalog
- Creates a trend analysis with month-over-month comparisons
- Exports data to Google Sheets with proper formatting and visualizations
- Handles API authentication and rate limiting

## Prerequisites

- Python 3.7 or higher
- A Google Cloud Platform project with the following APIs enabled:
  - YouTube Data API v3
  - YouTube Analytics API
  - Google Sheets API
- OAuth 2.0 credentials for the above APIs
- A Google Sheet to store the report data

## Installation

1. Clone this repository or download the script files.

2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up OAuth 2.0 credentials:
   - Go to the [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select an existing one
   - Enable the required APIs (YouTube Data API v3, YouTube Analytics API, Google Sheets API)
   - Create OAuth 2.0 credentials (Desktop app type)
   - Download the credentials JSON file and save it as `credentials.json` in the same directory as the script

## Usage

Run the script from the command line with the following arguments:

```bash
python youtube_analytics_report.py --channel_id CHANNEL_ID --month MM/YYYY --sheet_url SHEET_URL
```

### Required Arguments

- `--channel_id`: Your YouTube Channel ID (e.g., "UCxxxxxxxxxx")
- `--month`: The month and year for which to generate the report (e.g., "04/2025")
- `--sheet_url`: The URL of the Google Sheet where the report will be saved

### Optional Arguments

- `--config`: Path to a configuration file (default: "config.json")
- `--debug`: Enable debug logging

### Example

```bash
python youtube_analytics_report.py --channel_id UC-lHJZR3Gqxm24_Vd_AJ5Yw --month 05/2025 --sheet_url https://docs.google.com/spreadsheets/d/1abc123def456/edit
```

## Output

The script generates two tabs in the specified Google Sheet:

1. **Monthly Data Tab** (named after the month, e.g., "May 2025"):
   - Individual rows for each video published in the specified month
   - An aggregated "Back Catalog" row for all pre-existing videos
   - Metrics include views, watch time, subscriber gain, revenue, etc.

2. **Monthly Trends Tab** (named "Monthly Trends"):
   - Month-over-month comparisons for the last 12 months
   - Separate sections for new videos and back catalog performance
   - Percentage changes between consecutive months
   - Sparkline visualizations for trends

## Authentication Flow

On first run, the script will open a browser window asking you to authorize access to your YouTube data and Google Sheets. Once authorized, the credentials will be saved for future use.

## Scheduling the Script

To run the script automatically on the first day of each month, you can use:

### On Linux/macOS (using cron):

```bash
# Edit crontab
crontab -e

# Add this line (adjust the path as needed):
0 6 1 * * cd /path/to/script && python youtube_analytics_report.py --channel_id CHANNEL_ID --month $(date -d "$(date +%Y-%m-15) -1 month" +%m/%Y) --sheet_url SHEET_URL
```

### On Windows (using Task Scheduler):

1. Create a batch file (e.g., `run_youtube_report.bat`) with:
   ```batch
   @echo off
   cd C:\path\to\script
   python youtube_analytics_report.py --channel_id CHANNEL_ID --month %date:~4,2%/%date:~-4% --sheet_url SHEET_URL
   ```

2. Open Task Scheduler and create a new task that runs on the first day of each month.

## Troubleshooting

### Common Issues

1. **API Quota Exceeded**
   - The script includes rate limiting to avoid hitting API quotas
   - If you still see quota errors, try running the script at a different time of day

2. **Authentication Errors**
   - If you see authentication errors, try deleting the `token.json` file and running the script again
   - Make sure your `credentials.json` file is correctly set up

3. **Missing Data**
   - Ensure you have the correct permissions for the YouTube channel
   - For revenue data, you need to be the content owner of the channel

### Logging

The script logs information to both the console and a file (`youtube_analytics.log`). Enable debug logging with the `--debug` flag for more detailed information.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Google API Client for Python](https://github.com/googleapis/google-api-python-client)
- [YouTube Data API Documentation](https://developers.google.com/youtube/v3/docs)
- [YouTube Analytics API Documentation](https://developers.google.com/youtube/analytics/reference)