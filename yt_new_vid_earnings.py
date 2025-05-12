#!/usr/bin/env python3
"""
YouTube Analytics Report Generator

This script generates a monthly analytics report for YouTube channels and exports data to Google Sheets.
It provides detailed analytics for new videos published in the specified month,
as well as aggregated performance data for the back catalog of videos.

Usage:
    python youtube_analytics_report.py --channel_id CHANNEL_ID --month MM/YYYY --sheet_url SHEET_URL

Requirements:
    - Google API Client Library for Python
    - OAuth2 credentials for YouTube Analytics API
    - OAuth2 credentials for Google Sheets API
    - pandas
    - numpy
    - python-dotenv
"""

import os
import sys
import argparse
import logging
import re
from datetime import datetime, timedelta
import calendar
from typing import Dict, List, Tuple, Any, Optional
import json
import time
from pathlib import Path

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("youtube_analytics.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Define scopes for YouTube Data API, YouTube Analytics API, and Google Sheets API
SCOPES = [
    'https://www.googleapis.com/auth/youtube.readonly',
    'https://www.googleapis.com/auth/yt-analytics.readonly',
    'https://www.googleapis.com/auth/spreadsheets'
]

# Constants
TOKEN_FILE = 'token.json'
CREDENTIALS_FILE = 'credentials.json'
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
API_QUOTA_DELAY = 1  # seconds between API calls to avoid quota issues


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Generate YouTube Analytics Report')
    parser.add_argument('--channel_id', type=str, required=True, help='YouTube Channel ID')
    parser.add_argument('--month', type=str, required=True, 
                        help='Month in MM/YYYY format (e.g., 09/2025)')
    parser.add_argument('--sheet_url', type=str, required=True, 
                        help='Google Sheet URL associated with the Channel ID')
    parser.add_argument('--config', type=str, help='Path to configuration file', default='config.json')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Validate month format
    if not re.match(r'^\d{2}/\d{4}$', args.month):
        parser.error("Month must be in MM/YYYY format (e.g., 09/2025)")
    
    # Validate sheet URL format
    if not args.sheet_url.startswith('https://docs.google.com/spreadsheets/d/'):
        parser.error("Sheet URL must be a valid Google Sheets URL")
    
    return args


def get_credentials() -> Credentials:
    """Get and refresh OAuth2 credentials for Google APIs."""
    creds = None
    
    # Load token from file if it exists
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_info(
                json.loads(open(TOKEN_FILE).read()), SCOPES)
        except (ValueError, json.JSONDecodeError) as e:
            logger.error(f"Error loading token file: {e}")
            os.remove(TOKEN_FILE)
    
    # If credentials don't exist or are invalid, refresh or create new ones
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError as e:
                logger.error(f"Error refreshing credentials: {e}")
                os.remove(TOKEN_FILE)
                return get_credentials()
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                logger.error(f"Credentials file '{CREDENTIALS_FILE}' not found.")
                logger.info("Please download OAuth client ID credentials from Google Cloud Console "
                            "and save as 'credentials.json'")
                sys.exit(1)
                
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    
    return creds


def extract_sheet_id(sheet_url: str) -> str:
    """Extract the Google Sheet ID from the URL."""
    match = re.search(r'/d/([a-zA-Z0-9-_]+)', sheet_url)
    if not match:
        raise ValueError("Invalid Google Sheet URL")
    return match.group(1)


def get_date_range(month_str: str) -> Tuple[str, str, str]:
    """
    Parse the month string and return start date, end date, and month name.
    
    Args:
        month_str: Month in MM/YYYY format (e.g., 09/2025)
        
    Returns:
        Tuple containing (start_date, end_date, month_name)
    """
    try:
        month, year = map(int, month_str.split('/'))
        if month < 1 or month > 12 or year < 2005:  # YouTube was founded in 2005
            raise ValueError("Invalid month or year")
        
        last_day = calendar.monthrange(year, month)[1]
        start_date = f"{year}-{month:02d}-01"
        end_date = f"{year}-{month:02d}-{last_day}"
        month_name = calendar.month_name[month] + " " + str(year)
        
        return start_date, end_date, month_name
    except ValueError as e:
        logger.error(f"Error parsing month: {e}")
        raise


def format_duration(seconds: float) -> str:
    """Format seconds into minutes:seconds format."""
    minutes, seconds = divmod(int(seconds), 60)
    return f"{minutes}:{seconds:02d}"


def format_currency(amount: float) -> str:
    """Format amount as USD currency."""
    return f"${amount:.2f}"


class YouTubeAnalyticsReport:
    """Class to handle YouTube Analytics reporting."""
    
    def __init__(self, channel_id: str, start_date: str, end_date: str, sheet_id: str):
        """Initialize the report generator with the required parameters."""
        self.channel_id = channel_id
        self.start_date = start_date
        self.end_date = end_date
        self.sheet_id = sheet_id
        
        # Initialize API services
        self.credentials = get_credentials()
        self.youtube = build('youtube', 'v3', credentials=self.credentials)
        self.youtube_analytics = build('youtubeAnalytics', 'v2', credentials=self.credentials)
        self.sheets = build('sheets', 'v4', credentials=self.credentials)
        
        # Get channel content owner ID (needed for revenue data)
        self.content_owner_id = self._get_content_owner_id()
        
        logger.info(f"Initialized report for channel {channel_id} from {start_date} to {end_date}")
    
    def _get_content_owner_id(self) -> Optional[str]:
        """Get the content owner ID for the channel if available."""
        try:
            response = self.youtube.channels().list(
                part='contentOwnerDetails',
                id=self.channel_id
            ).execute()
            
            time.sleep(API_QUOTA_DELAY)  # Respect API quota
            
            if 'items' in response and response['items']:
                content_owner_details = response['items'][0].get('contentOwnerDetails', {})
                return content_owner_details.get('contentOwner')
            
            logger.warning("No content owner found for this channel. Revenue metrics may be unavailable.")
            return None
        except HttpError as e:
            logger.warning(f"Error getting content owner ID: {e}")
            return None
    
    def get_videos_published_in_month(self) -> pd.DataFrame:
        """Get all videos published in the specified month."""
        videos = []
        page_token = None
        
        while True:
            try:
                # Make the API request
                response = self.youtube.search().list(
                    part='id,snippet',
                    channelId=self.channel_id,
                    maxResults=50,
                    order='date',
                    type='video',
                    publishedAfter=f"{self.start_date}T00:00:00Z",
                    publishedBefore=f"{self.end_date}T23:59:59Z",
                    pageToken=page_token
                ).execute()
                
                time.sleep(API_QUOTA_DELAY)  # Respect API quota
                
                # Extract video IDs and details
                for item in response.get('items', []):
                    if item['id']['kind'] == 'youtube#video':
                        video_id = item['id']['videoId']
                        title = item['snippet']['title']
                        published_at = item['snippet']['publishedAt']
                        
                        videos.append({
                            'video_id': video_id,
                            'title': title,
                            'published_at': published_at
                        })
                
                # Check if there are more pages
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
            
            except HttpError as e:
                logger.error(f"Error retrieving videos: {e}")
                break
        
        logger.info(f"Found {len(videos)} videos published in the specified month")
        
        if not videos:
            return pd.DataFrame()
        
        # Create DataFrame and get additional video details
        videos_df = pd.DataFrame(videos)
        
        # Get additional details for all videos at once
        video_ids = videos_df['video_id'].tolist()
        details_df = self._get_video_details(video_ids)
        
        # Merge the DataFrames
        if not details_df.empty:
            videos_df = videos_df.merge(details_df, on='video_id', how='left')
        
        return videos_df
    
    def _get_video_details(self, video_ids: List[str]) -> pd.DataFrame:
        """Get additional details for a list of videos."""
        details = []
        
        # Process videos in chunks of 50 (API limit)
        for i in range(0, len(video_ids), 50):
            chunk = video_ids[i:i+50]
            
            try:
                response = self.youtube.videos().list(
                    part='contentDetails,statistics',
                    id=','.join(chunk)
                ).execute()
                
                time.sleep(API_QUOTA_DELAY)  # Respect API quota
                
                for item in response.get('items', []):
                    # Parse duration string (PT1H2M3S format)
                    duration_str = item['contentDetails']['duration']
                    duration_seconds = self._parse_duration(duration_str)
                    
                    details.append({
                        'video_id': item['id'],
                        'duration': duration_seconds,
                        'duration_formatted': format_duration(duration_seconds),
                        'view_count': int(item['statistics'].get('viewCount', 0)),
                        'like_count': int(item['statistics'].get('likeCount', 0)),
                        'comment_count': int(item['statistics'].get('commentCount', 0))
                    })
            
            except HttpError as e:
                logger.error(f"Error retrieving video details: {e}")
        
        return pd.DataFrame(details) if details else pd.DataFrame()
    
    def _parse_duration(self, duration_str: str) -> int:
        """Parse ISO 8601 duration format (e.g., PT1H2M3S) to seconds."""
        hours = re.search(r'(\d+)H', duration_str)
        minutes = re.search(r'(\d+)M', duration_str)
        seconds = re.search(r'(\d+)S', duration_str)
        
        total_seconds = 0
        if hours:
            total_seconds += int(hours.group(1)) * 3600
        if minutes:
            total_seconds += int(minutes.group(1)) * 60
        if seconds:
            total_seconds += int(seconds.group(1))
        
        return total_seconds
    
    def get_video_analytics(self, video_ids: List[str]) -> pd.DataFrame:
        """Get analytics metrics for the specified videos."""
        if not video_ids:
            return pd.DataFrame()
        
        metrics = [
            'views', 'estimatedMinutesWatched', 'subscribersGained', 
            'estimatedRevenue', 'averageViewDuration'
        ]
        
        try:
            # For revenue data, we need content owner ID
            if self.content_owner_id:
                response = self.youtube_analytics.reports().query(
                    dimensions='video',
                    ids=f'contentOwner={self.content_owner_id}',
                    startDate=self.start_date,
                    endDate=self.end_date,
                    metrics=','.join(metrics),
                    filters=f'video=={";".join(video_ids)}'
                ).execute()
            else:
                # Fall back to channel data without revenue
                metrics.remove('estimatedRevenue')
                response = self.youtube_analytics.reports().query(
                    dimensions='video',
                    ids=f'channel=={self.channel_id}',
                    startDate=self.start_date,
                    endDate=self.end_date,
                    metrics=','.join(metrics),
                    filters=f'video=={";".join(video_ids)}'
                ).execute()
            
            time.sleep(API_QUOTA_DELAY)  # Respect API quota
            
            # Parse response
            if 'rows' in response:
                columns = ['video_id'] + response['columnHeaders'][1:]
                columns = [col['name'] for col in columns]
                
                data = pd.DataFrame(response['rows'], columns=columns)
                
                # Convert to appropriate types
                if 'estimatedMinutesWatched' in data.columns:
                    data['watch_time_hours'] = data['estimatedMinutesWatched'] / 60
                if 'averageViewDuration' in data.columns:
                    data['avg_view_duration_formatted'] = data['averageViewDuration'].apply(
                        lambda x: format_duration(x))
                if 'estimatedRevenue' in data.columns:
                    data['revenue_formatted'] = data['estimatedRevenue'].apply(
                        lambda x: format_currency(float(x)))
                
                return data
            
            return pd.DataFrame()
        
        except HttpError as e:
            logger.error(f"Error retrieving video analytics: {e}")
            return pd.DataFrame()
    
    def get_back_catalog_analytics(self) -> Dict[str, Any]:
        """Get aggregated metrics for videos published before the specified month."""
        # Get all videos published before the start date
        back_catalog_end_date = (datetime.strptime(self.start_date, '%Y-%m-%d') - 
                                 timedelta(days=1)).strftime('%Y-%m-%d')
        
        metrics = [
            'views', 'estimatedMinutesWatched', 'subscribersGained',
            'averageViewDuration'
        ]
        
        # Add revenue metric if we have content owner ID
        if self.content_owner_id:
            metrics.append('estimatedRevenue')
        
        try:
            # Make API request
            if self.content_owner_id:
                response = self.youtube_analytics.reports().query(
                    dimensions='',  # No dimensions for aggregation
                    ids=f'contentOwner={self.content_owner_id}',
                    startDate=self.start_date,
                    endDate=self.end_date,
                    metrics=','.join(metrics),
                    filters=f'video!=;claimedStatus==claimed;uploadDate<{self.start_date}'
                ).execute()
            else:
                # Fall back to channel data without revenue
                if 'estimatedRevenue' in metrics:
                    metrics.remove('estimatedRevenue')
                
                response = self.youtube_analytics.reports().query(
                    dimensions='',  # No dimensions for aggregation
                    ids=f'channel=={self.channel_id}',
                    startDate=self.start_date,
                    endDate=self.end_date,
                    metrics=','.join(metrics),
                    filters=f'video!=;uploadDate<{self.start_date}'
                ).execute()
            
            time.sleep(API_QUOTA_DELAY)  # Respect API quota
            
            # Parse response
            if 'rows' in response and response['rows']:
                # Create dictionary mapping column names to values
                result = {}
                for i, header in enumerate(response['columnHeaders']):
                    column_name = header['name']
                    value = response['rows'][0][i]
                    result[column_name] = value
                
                # Format values
                if 'estimatedMinutesWatched' in result:
                    result['watch_time_hours'] = float(result['estimatedMinutesWatched']) / 60
                if 'averageViewDuration' in result:
                    result['avg_view_duration_formatted'] = format_duration(
                        float(result['averageViewDuration']))
                if 'estimatedRevenue' in result:
                    result['revenue_formatted'] = format_currency(float(result['estimatedRevenue']))
                
                logger.info("Successfully retrieved back catalog analytics")
                return result
            
            logger.warning("No back catalog data found")
            return {}
        
        except HttpError as e:
            logger.error(f"Error retrieving back catalog analytics: {e}")
            return {}
    
    def _get_historical_data(self, months: int = 12) -> pd.DataFrame:
        """Get historical monthly data for trend analysis."""
        # Calculate date range for the past N months
        end_date = datetime.strptime(self.end_date, '%Y-%m-%d')
        start_date = end_date.replace(day=1) - timedelta(days=1)
        start_date = start_date.replace(day=1) - timedelta(days=(months-1)*30)
        start_date_str = start_date.strftime('%Y-%m-%d')
        
        metrics = [
            'views', 'estimatedMinutesWatched', 'subscribersGained',
            'averageViewDuration'
        ]
        
        if self.content_owner_id:
            metrics.append('estimatedRevenue')
        
        try:
            # Get monthly aggregated data
            if self.content_owner_id:
                response = self.youtube_analytics.reports().query(
                    dimensions='month',
                    ids=f'contentOwner={self.content_owner_id}',
                    startDate=start_date_str,
                    endDate=self.end_date,
                    metrics=','.join(metrics)
                ).execute()
            else:
                if 'estimatedRevenue' in metrics:
                    metrics.remove('estimatedRevenue')
                
                response = self.youtube_analytics.reports().query(
                    dimensions='month',
                    ids=f'channel=={self.channel_id}',
                    startDate=start_date_str,
                    endDate=self.end_date,
                    metrics=','.join(metrics)
                ).execute()
            
            time.sleep(API_QUOTA_DELAY)  # Respect API quota
            
            # Process response
            if 'rows' in response:
                columns = [header['name'] for header in response['columnHeaders']]
                data = pd.DataFrame(response['rows'], columns=columns)
                
                # Format month column (YYYYMM to readable format)
                if 'month' in data.columns:
                    data['month_str'] = data['month'].apply(
                        lambda x: datetime.strptime(str(x), '%Y%m').strftime('%B %Y'))
                
                # Calculate percentage changes
                for metric in metrics:
                    if metric in data.columns:
                        data[f'{metric}_pct_change'] = data[metric].pct_change() * 100
                
                return data
            
            return pd.DataFrame()
        
        except HttpError as e:
            logger.error(f"Error retrieving historical data: {e}")
            return pd.DataFrame()
    
    def _get_historical_back_catalog(self, months: int = 12) -> pd.DataFrame:
        """Get historical monthly data for back catalog performance."""
        # Calculate date range
        end_date = datetime.strptime(self.end_date, '%Y-%m-%d')
        
        # Create a list of month ranges
        month_ranges = []
        current_month_end = end_date
        
        for i in range(months):
            current_month_start = current_month_end.replace(day=1)
            
            if i > 0:
                current_month_end = current_month_start - timedelta(days=1)
                current_month_start = current_month_end.replace(day=1)
            
            month_ranges.append({
                'start': current_month_start.strftime('%Y-%m-%d'),
                'end': current_month_end.strftime('%Y-%m-%d'),
                'label': current_month_start.strftime('%B %Y')
            })
            
            # Move to previous month
            current_month_end = current_month_start - timedelta(days=1)
        
        # Reverse so most recent month is last
        month_ranges.reverse()
        
        # Get data for each month
        catalog_data = []
        for month_range in month_ranges:
            result = self._get_back_catalog_for_month(
                month_range['start'], month_range['end'])
            
            if result:
                result['month'] = month_range['label']
                catalog_data.append(result)
        
        # Convert to DataFrame
        if catalog_data:
            df = pd.DataFrame(catalog_data)
            
            # Calculate percentage changes
            for col in df.columns:
                if col != 'month' and not col.endswith('_pct_change') and not col.endswith('_formatted'):
                    df[f'{col}_pct_change'] = df[col].pct_change() * 100
            
            return df
        
        return pd.DataFrame()
    
    def _get_back_catalog_for_month(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Get back catalog metrics for a specific month."""
        metrics = [
            'views', 'estimatedMinutesWatched', 'subscribersGained',
            'averageViewDuration'
        ]
        
        if self.content_owner_id:
            metrics.append('estimatedRevenue')
        
        try:
            # Make API request
            if self.content_owner_id:
                response = self.youtube_analytics.reports().query(
                    dimensions='',  # No dimensions for aggregation
                    ids=f'contentOwner={self.content_owner_id}',
                    startDate=start_date,
                    endDate=end_date,
                    metrics=','.join(metrics),
                    filters=f'video!=;claimedStatus==claimed;uploadDate<{start_date}'
                ).execute()
            else:
                if 'estimatedRevenue' in metrics:
                    metrics.remove('estimatedRevenue')
                
                response = self.youtube_analytics.reports().query(
                    dimensions='',  # No dimensions for aggregation
                    ids=f'channel=={self.channel_id}',
                    startDate=start_date,
                    endDate=end_date,
                    metrics=','.join(metrics),
                    filters=f'video!=;uploadDate<{start_date}'
                ).execute()
            
            time.sleep(API_QUOTA_DELAY)  # Respect API quota
            
            # Parse response
            if 'rows' in response and response['rows']:
                result = {}
                for i, header in enumerate(response['columnHeaders']):
                    column_name = header['name']
                    value = response['rows'][0][i]
                    result[column_name] = value
                
                return result
            
            return {}
        
        except HttpError as e:
            logger.error(f"Error retrieving back catalog data for period: {e}")
            return {}
    
    def _check_sheet_exists(self, sheet_name: str) -> bool:
        """Check if a sheet with the given name already exists."""
        try:
            response = self.sheets.spreadsheets().get(spreadsheetId=self.sheet_id).execute()
            
            for sheet in response.get('sheets', []):
                if sheet['properties']['title'] == sheet_name:
                    return True
            
            return False
        
        except HttpError as e:
            logger.error(f"Error checking sheet existence: {e}")
            return False
    
    def _create_or_update_sheet(self, sheet_name: str) -> None:
        """Create a new sheet or clear an existing one."""
        try:
            if self._check_sheet_exists(sheet_name):
                # Clear existing sheet
                range_name = f"{sheet_name}!A1:Z1000"
                self.sheets.spreadsheets().values().clear(
                    spreadsheetId=self.sheet_id,
                    range=range_name,
                    body={}
                ).execute()
                logger.info(f"Cleared existing sheet: {sheet_name}")
            else:
                # Create new sheet
                request = {
                    'requests': [{
                        'addSheet': {
                            'properties': {
                                'title': sheet_name
                            }
                        }
                    }]
                }
                
                self.sheets.spreadsheets().batchUpdate(
                    spreadsheetId=self.sheet_id,
                    body=request
                ).execute()
                
                logger.info(f"Created new sheet: {sheet_name}")
        
        except HttpError as e:
            logger.error(f"Error creating/updating sheet: {e}")
            raise
    
    def _write_to_sheet(self, sheet_name: str, data: List[List[Any]]) -> None:
        """Write data to the specified sheet."""
        try:
            range_name = f"{sheet_name}!A1"
            body = {
                'values': data
            }
            
            self.sheets.spreadsheets().values().update(
                spreadsheetId=self.sheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            
            logger.info(f"Data written to sheet: {sheet_name}")
        
        except HttpError as e:
            logger.error(f"Error writing to sheet: {e}")
            raise
    
    def _format_sheet(self, sheet_name: str, num_rows: int, num_cols: int) -> None:
        """Apply formatting to the sheet."""
        try:
            # Format headers (bold, freeze)
            requests = [
                # Freeze header row
                {
                    'updateSheetProperties': {
                        'properties': {
                            'title': sheet_name,
                            'gridProperties': {
                                'frozenRowCount': 1
                            }
                        },
                        'fields': 'gridProperties.frozenRowCount'
                    }
                },
                # Bold headers
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': self._get_sheet_id(sheet_name),
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': 0,
                            'endColumnIndex': num_cols
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'bold': True
                                },
                                'backgroundColor': {
                                    'red': 0.9,
                                    'green': 0.9,
                                    'blue': 0.9
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(textFormat,backgroundColor)'
                    }
                },
                # Auto-resize columns
                {
                    'autoResizeDimensions': {
                        'dimensions': {
                            'sheetId': self._get_sheet_id(sheet_name),
                            'dimension': 'COLUMNS',
                            'startIndex': 0,
                            'endIndex': num_cols
                        }
                    }
                }
            ]
            
            # Format alternating rows
            if num_rows > 1:
                requests.append({
                    'addConditionalFormatRule': {
                        'rule': {
                            'ranges': [{
                                'sheetId': self._get_sheet_id(sheet_name),
                                'startRowIndex': 1,
                                'endRowIndex': num_rows
                            }],
                            'booleanRule': {
                                'condition': {
                                    'type': 'CUSTOM_FORMULA',
                                    'values': [{
                                        'userEnteredValue': '=ISEVEN(ROW())'
                                    }]
                                },
                                'format': {
                                    'backgroundColor': {
                                        'red': 0.95,
                                        'green': 0.95,
                                        'blue': 0.95
                                    }
                                }
                            }
                        },
                        'index': 0
                    }
                })
            
            # Highlight "Back Catalog" row
            if num_rows > 2:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': self._get_sheet_id(sheet_name),
                            'startRowIndex': num_rows - 1,
                            'endRowIndex': num_rows,
                            'startColumnIndex': 0,
                            'endColumnIndex': num_cols
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': {
                                    'red': 0.9,
                                    'green': 0.95,
                                    'blue': 1.0
                                },
                                'textFormat': {
                                    'bold': True
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(textFormat,backgroundColor)'
                    }
                })
            
            # Apply all formatting
            body = {'requests': requests}
            self.sheets.spreadsheets().batchUpdate(
                spreadsheetId=self.sheet_id,
                body=body
            ).execute()
            
            logger.info(f"Formatting applied to sheet: {sheet_name}")
        
        except HttpError as e:
            logger.error(f"Error formatting sheet: {e}")
    
    def _get_sheet_id(self, sheet_name: str) -> int:
        """Get the sheet ID for a given sheet name."""
        try:
            response = self.sheets.spreadsheets().get(
                spreadsheetId=self.sheet_id
            ).execute()
            
            for sheet in response.get('sheets', []):
                if sheet['properties']['title'] == sheet_name:
                    return sheet['properties']['sheetId']
            
            raise ValueError(f"Sheet '{sheet_name}' not found")
        
        except HttpError as e:
            logger.error(f"Error getting sheet ID: {e}")
            raise
    
    def _add_sparklines(self, sheet_name: str, data_range: str, target_range: str) -> None:
        """Add sparklines to the sheet for visualizing trends."""
        try:
            # Get the sheet ID
            sheet_id = self._get_sheet_id(sheet_name)
            
            # Parse target range
            match = re.match(r'([A-Z]+)(\d+):([A-Z]+)(\d+)', target_range)
            if not match:
                raise ValueError(f"Invalid range format: {target_range}")
            
            start_col = self._column_letter_to_index(match.group(1))
            start_row = int(match.group(2)) - 1  # 0-indexed
            end_col = self._column_letter_to_index(match.group(3))
            end_row = int(match.group(4)) - 1
            
            # Create sparkline requests
            requests = []
            col_index = start_col
            
            while col_index <= end_col:
                row_index = start_row
                while row_index <= end_row:
                    requests.append({
                        'updateCells': {
                            'range': {
                                'sheetId': sheet_id,
                                'startRowIndex': row_index,
                                'endRowIndex': row_index + 1,
                                'startColumnIndex': col_index,
                                'endColumnIndex': col_index + 1
                            },
                            'rows': [{
                                'values': [{
                                    'userEnteredValue': {
                                        'formulaValue': f'=SPARKLINE({data_range})'
                                    }
                                }]
                            }],
                            'fields': 'userEnteredValue'
                        }
                    })
                    row_index += 1
                col_index += 1
            
            # Apply all sparkline requests
            body = {'requests': requests}
            self.sheets.spreadsheets().batchUpdate(
                spreadsheetId=self.sheet_id,
                body=body
            ).execute()
            
            logger.info(f"Sparklines added to sheet: {sheet_name}")
        
        except HttpError as e:
            logger.error(f"Error adding sparklines: {e}")
    
    def _column_letter_to_index(self, column_letter: str) -> int:
        """Convert a column letter (A, B, ..., Z, AA, AB, ...) to a 0-indexed column index."""
        result = 0
        for char in column_letter:
            result = result * 26 + (ord(char.upper()) - ord('A') + 1)
        return result - 1  # Convert to 0-indexed
    
    def generate_monthly_report(self, month_name: str) -> None:
        """Generate the monthly report and write it to Google Sheets."""
        # Step 1: Get videos published in the month
        videos_df = self.get_videos_published_in_month()
        
        # Step 2: Get analytics for each video
        if not videos_df.empty:
            video_ids = videos_df['video_id'].tolist()
            analytics_df = self.get_video_analytics(video_ids)
            
            # Merge with video details
            if not analytics_df.empty:
                videos_df = videos_df.merge(analytics_df, on='video_id', how='left')
        
        # Step 3: Get back catalog data
        back_catalog = self.get_back_catalog_analytics()
        
        # Step 4: Create or update the sheet
        self._create_or_update_sheet(month_name)
        
        # Step 5: Prepare data for the sheet
        # Define headers
        headers = [
            'Content ID', 'Video Title', 'Publish Date', 'Duration', 
            'Views', 'Watch Time (hours)', 'Subscribers Gained', 
            'Estimated Revenue (USD)', 'Avg View Duration'
        ]
        
        # Prepare data rows
        rows = [headers]
        
        # Add video data
        if not videos_df.empty:
            for _, video in videos_df.iterrows():
                row = [
                    video.get('video_id', ''),
                    video.get('title', ''),
                    video.get('published_at', ''),
                    video.get('duration_formatted', ''),
                    video.get('views', 0),
                    round(video.get('watch_time_hours', 0), 2),
                    video.get('subscribersGained', 0),
                    video.get('revenue_formatted', '$0.00'),
                    video.get('avg_view_duration_formatted', '0:00')
                ]
                rows.append(row)
        
        # Add back catalog row
        if back_catalog:
            back_catalog_row = [
                'BACK_CATALOG',
                'Back Catalog (all previous videos)',
                f'Prior to {self.start_date}',
                'N/A',
                back_catalog.get('views', 0),
                round(back_catalog.get('watch_time_hours', 0), 2),
                back_catalog.get('subscribersGained', 0),
                back_catalog.get('revenue_formatted', '$0.00'),
                back_catalog.get('avg_view_duration_formatted', '0:00')
            ]
            rows.append(back_catalog_row)
        
        # Add timestamp row
        timestamp_row = [
            f'Report generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        ]
        rows.append([''])  # Empty row
        rows.append(timestamp_row)
        
        # Step 6: Write data to the sheet
        self._write_to_sheet(month_name, rows)
        
        # Step 7: Format the sheet
        self._format_sheet(month_name, len(rows), len(headers))
        
        logger.info(f"Monthly report for {month_name} generated successfully")
    
    def generate_trend_analysis(self) -> None:
        """Generate the trend analysis tab."""
        # Step 1: Get historical data
        monthly_data = self._get_historical_data()
        back_catalog_data = self._get_historical_back_catalog()
        
        # Step 2: Create or update the trends sheet
        sheet_name = "Monthly Trends"
        self._create_or_update_sheet(sheet_name)
        
        # Step 3: Prepare data for the sheet
        rows = []
        
        # Add title and timestamp
        rows.append(['YouTube Channel Monthly Trend Analysis'])
        rows.append([f'Channel ID: {self.channel_id}'])
        rows.append([f'Report generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'])
        rows.append([''])  # Empty row
        
        # Section 1: New Videos Performance
        rows.append(['New Videos Performance (Month by Month)'])
        
        if not monthly_data.empty:
            # Headers
            headers = ['Month', 'Views', 'Change %', 'Watch Time (h)', 'Change %', 
                       'Subscribers', 'Change %']
            
            if 'estimatedRevenue' in monthly_data.columns:
                headers.extend(['Revenue (USD)', 'Change %'])
            
            headers.append('Trend')  # For sparklines
            rows.append(headers)
            
            # Data rows
            for _, month in monthly_data.iterrows():
                row = [
                    month.get('month_str', ''),
                    month.get('views', 0),
                    f"{month.get('views_pct_change', 0):.1f}%",
                    round(float(month.get('estimatedMinutesWatched', 0)) / 60, 2),
                    f"{month.get('estimatedMinutesWatched_pct_change', 0):.1f}%",
                    month.get('subscribersGained', 0),
                    f"{month.get('subscribersGained_pct_change', 0):.1f}%"
                ]
                
                if 'estimatedRevenue' in monthly_data.columns:
                    row.extend([
                        f"${float(month.get('estimatedRevenue', 0)):.2f}",
                        f"{month.get('estimatedRevenue_pct_change', 0):.1f}%"
                    ])
                
                row.append('')  # Placeholder for sparkline
                rows.append(row)
        else:
            rows.append(['No historical data available'])
        
        rows.append([''])  # Empty row
        
        # Section 2: Back Catalog Performance
        rows.append(['Back Catalog Performance (Month by Month)'])
        
        if not back_catalog_data.empty:
            # Headers
            headers = ['Month', 'Views', 'Change %', 'Watch Time (h)', 'Change %', 
                       'Subscribers', 'Change %']
            
            if 'estimatedRevenue' in back_catalog_data.columns:
                headers.extend(['Revenue (USD)', 'Change %'])
            
            headers.append('Trend')  # For sparklines
            rows.append(headers)
            
            # Data rows
            for _, month in back_catalog_data.iterrows():
                row = [
                    month.get('month', ''),
                    month.get('views', 0),
                    f"{month.get('views_pct_change', 0):.1f}%",
                    round(float(month.get('estimatedMinutesWatched', 0)) / 60, 2),
                    f"{month.get('estimatedMinutesWatched_pct_change', 0):.1f}%",
                    month.get('subscribersGained', 0),
                    f"{month.get('subscribersGained_pct_change', 0):.1f}%"
                ]
                
                if 'estimatedRevenue' in back_catalog_data.columns:
                    row.extend([
                        f"${float(month.get('estimatedRevenue', 0)):.2f}",
                        f"{month.get('estimatedRevenue_pct_change', 0):.1f}%"
                    ])
                
                row.append('')  # Placeholder for sparkline
                rows.append(row)
        else:
            rows.append(['No back catalog data available'])
        
        # Step 4: Write data to the sheet
        self._write_to_sheet(sheet_name, rows)
        
        # Step 5: Format the sheet
        self._format_sheet(sheet_name, len(rows), len(rows[5]) if len(rows) > 5 else 9)
        
        # Step 6: Add sparklines for trends (if data available)
        if not monthly_data.empty and len(rows) > 7:
            new_videos_start_row = 6
            new_videos_end_row = new_videos_start_row + len(monthly_data) - 1
            
            # Find the sparkline column index
            sparkline_col = len(rows[5]) - 1
            sparkline_col_letter = chr(ord('A') + sparkline_col)
            
            # Find the views column index
            views_col_letter = 'B'
            
            # Create sparkline for the 'Views' column
            data_range = f"{sheet_name}!{views_col_letter}{new_videos_start_row}:{views_col_letter}{new_videos_end_row}"
            target_range = f"{sparkline_col_letter}{new_videos_start_row}:{sparkline_col_letter}{new_videos_end_row}"
            
            self._add_sparklines(sheet_name, data_range, target_range)
        
        logger.info("Trend analysis report generated successfully")


def main():
    """Main function to run the script."""
    # Parse command line arguments
    args = parse_arguments()
    
    # Set log level
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # Load environment variables from .env file if it exists
    load_dotenv()
    
    try:
        # Parse the month string
        start_date, end_date, month_name = get_date_range(args.month)
        
        # Extract sheet ID from URL
        sheet_id = extract_sheet_id(args.sheet_url)
        
        # Initialize the report generator
        report = YouTubeAnalyticsReport(
            channel_id=args.channel_id,
            start_date=start_date,
            end_date=end_date,
            sheet_id=sheet_id
        )
        
        # Generate monthly report
        logger.info(f"Generating report for {month_name}...")
        report.generate_monthly_report(month_name)
        
        # Generate trend analysis
        logger.info("Generating trend analysis...")
        report.generate_trend_analysis()
        
        logger.info("Report generation completed successfully!")
        print(f"YouTube Analytics Report for {month_name} generated successfully.")
        print(f"Report is available at: {args.sheet_url}")
    
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()