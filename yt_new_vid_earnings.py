#!/usr/bin/env python3
"""
YouTube Analytics Report Generator

This script generates a monthly analytics report for YouTube channels and exports data to Google Sheets.
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
API_QUOTA_DELAY = 1  # seconds between API calls to avoid quota issues


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Generate YouTube Analytics Report')
    parser.add_argument('--channel_id', type=str, required=True, help='YouTube Channel ID')
    parser.add_argument('--month', type=str, required=True, 
                        help='Month in MM/YYYY format (e.g., 09/2025)')
    parser.add_argument('--sheet_url', type=str, required=True, 
                        help='Google Sheet URL associated with the Channel ID')
    parser.add_argument('--credentials', type=str, 
                        default='/Users/keithjohnson/Desktop/yt_new_vid_earnings.py/client_secret_yt_cms.json',
                        help='Path to OAuth credentials file')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Validate month format
    if not re.match(r'^\d{2}/\d{4}$', args.month):
        parser.error("Month must be in MM/YYYY format (e.g., 09/2025)")
    
    # Validate sheet URL format
    if not args.sheet_url.startswith('https://docs.google.com/spreadsheets/d/'):
        parser.error("Sheet URL must be a valid Google Sheets URL")
    
    # Validate credentials file exists
    if not os.path.exists(args.credentials):
        parser.error(f"Credentials file not found: {args.credentials}")
    
    return args


def get_credentials(credentials_file):
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
                return get_credentials(credentials_file)
        else:
            if not os.path.exists(credentials_file):
                logger.error(f"Credentials file '{credentials_file}' not found.")
                logger.info("Please download OAuth client ID credentials from Google Cloud Console")
                sys.exit(1)
                
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    
    return creds


def extract_sheet_id(sheet_url):
    """Extract the Google Sheet ID from the URL."""
    match = re.search(r'/d/([a-zA-Z0-9-_]+)', sheet_url)
    if not match:
        raise ValueError("Invalid Google Sheet URL")
    return match.group(1)


def get_date_range(month_str):
    """
    Parse the month string and return start date, end date, and month name.
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


class YouTubeAnalyticsReport:
    """Class to handle YouTube Analytics reporting."""
    
    def __init__(self, channel_id, start_date, end_date, sheet_id, credentials_file):
        """Initialize the report generator with the required parameters."""
        self.channel_id = channel_id
        self.start_date = start_date
        self.end_date = end_date
        self.sheet_id = sheet_id
        
        # Initialize API services
        self.credentials = get_credentials(credentials_file)
        self.youtube = build('youtube', 'v3', credentials=self.credentials)
        self.youtube_analytics = build('youtubeAnalytics', 'v2', credentials=self.credentials)
        self.sheets = build('sheets', 'v4', credentials=self.credentials)
        
        # Get channel content owner ID (needed for revenue data)
        self.content_owner_id = self._get_content_owner_id()
        
        logger.info(f"Initialized report for channel {channel_id} from {start_date} to {end_date}")
    
    def _get_content_owner_id(self):
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
    
    def generate_monthly_report(self, month_name):
        """Generate the monthly report and write it to Google Sheets."""
        try:
            # Check if sheet exists, otherwise create it
            sheet_exists = self._check_sheet_exists(month_name)
            if not sheet_exists:
                self._create_sheet(month_name)
            
            # Add basic info to the sheet
            self._write_basic_info(month_name)
            
            logger.info(f"Monthly report for {month_name} generated successfully")
            return True
        except Exception as e:
            logger.error(f"Error generating monthly report: {e}")
            return False
    
    def _check_sheet_exists(self, sheet_name):
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
    
    def _create_sheet(self, sheet_name):
        """Create a new sheet."""
        try:
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
            return True
        except HttpError as e:
            logger.error(f"Error creating sheet: {e}")
            return False
    
    def _write_basic_info(self, sheet_name):
        """Write basic information to the sheet."""
        try:
            values = [
                ["YouTube Analytics Report"],
                [f"Channel ID: {self.channel_id}"],
                [f"Reporting Period: {self.start_date} to {self.end_date}"],
                [f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"],
                [""],  # Empty row
                ["Report is being populated. This is a test connection."]
            ]
            
            body = {
                'values': values
            }
            
            self.sheets.spreadsheets().values().update(
                spreadsheetId=self.sheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            
            logger.info(f"Basic info written to sheet: {sheet_name}")
            return True
        except HttpError as e:
            logger.error(f"Error writing to sheet: {e}")
            return False


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
            sheet_id=sheet_id,
            credentials_file=args.credentials
        )
        
        # Generate monthly report (simplified version for testing)
        logger.info(f"Generating report for {month_name}...")
        success = report.generate_monthly_report(month_name)
        
        if success:
            logger.info("Report generation completed successfully!")
            print(f"YouTube Analytics Report for {month_name} generated successfully.")
            print(f"Report is available at: {args.sheet_url}")
        else:
            logger.error("Failed to generate report.")
            print("Failed to generate report. Check the logs for details.")
    
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()