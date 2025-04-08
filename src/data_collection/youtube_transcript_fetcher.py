"""
Script to fetch transcripts from Joe Rogan Experience YouTube videos and store them in PostgreSQL.
"""

import os
import time
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from youtube_transcript_api import YouTubeTranscriptApi
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
import sys
import re

# Add the parent directory to the path so we can import the database module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_manager import DatabaseManager
from data_collection.data_quality import DataQualityChecker
from data_collection.collection_monitor import CollectionMonitor

# Set up logging directory
logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(logs_dir, 'transcript_collection.log')),
        logging.StreamHandler()
    ]
)

load_dotenv()

class JRETranscriptFetcher:
    def __init__(self, test_mode: bool = False):
        self.api_key = os.getenv('YOUTUBE_API_KEY')
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)
        self.channel_id = "UCnxGkOGNMqQEUMvroOWps6Q"  # JRE Clips channel ID
        self.db = DatabaseManager()
        self.quality_checker = DataQualityChecker(self.db)
        self.monitor = CollectionMonitor(self.db)
        self.test_mode = test_mode
        
        # Enhanced political keywords organized by category
        self.political_keywords = {
            'core_politics': [
                "politics", "political", "election", "democracy", "government",
                "policy", "legislation", "congress", "senate", "house"
            ],
            'parties_ideologies': [
                "democrat", "republican", "liberal", "conservative",
                "libertarian", "progressive", "left wing", "right wing",
                "socialist", "capitalist"
            ],
            'political_figures': [
                "trump", "biden", "obama", "clinton", "sanders", "warren",
                "pence", "harris", "pelosi", "mcconnell"
            ],
            'policy_issues': [
                "immigration", "healthcare", "climate change", "foreign policy",
                "censorship", "free speech", "gun control", "abortion",
                "taxation", "welfare"
            ],
            'cultural_issues': [
                "woke", "cancel culture", "identity politics", "social justice",
                "critical race theory", "gender", "equality", "diversity"
            ]
        }
        
        # Test mode keywords (subset for testing)
        self.test_keywords = [
            "politics", "election", "government",  # core_politics
            "trump", "biden",  # political_figures
            "immigration", "healthcare"  # policy_issues
        ]
        
    def get_transcript_with_backoff(self, video_id: str) -> Optional[List[Dict]]:
        """
        Attempt to get transcript with exponential backoff for rate limiting.
        """
        max_attempts = 3
        attempt = 0
        while attempt < max_attempts:
            try:
                return YouTubeTranscriptApi.get_transcript(video_id)
            except Exception as e:
                if "too many requests" in str(e).lower():
                    wait_time = (2 ** attempt) * 1  # 1, 2, 4 seconds
                    time.sleep(wait_time)
                    attempt += 1
                else:
                    logging.error(f"Transcript error for {video_id}: {str(e)}")
                    return None
        return None

    def extract_episode_number(self, title: str) -> Optional[int]:
        """Extract episode number from video title."""
        patterns = [
            r'#(\d+)',  # #1234
            r'Episode (\d+)',  # Episode 1234
            r'JRE (\d+)',  # JRE 1234
        ]
        
        for pattern in patterns:
            match = re.search(pattern, title)
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    continue
        return None

    def extract_guest_name(self, title: str) -> Optional[str]:
        """Extract guest name from video title."""
        # Common patterns for guest names in JRE titles
        patterns = [
            r'with (.+?)(?: #|$)',  # "with Guest Name #1234"
            r'Joe Rogan Experience #\d+ - (.+?)(?: #|$)',  # "JRE #1234 - Guest Name"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, title)
            if match:
                return match.group(1).strip()
        return None

    def calculate_political_score(self, title: str, description: str) -> Tuple[float, List[str]]:
        """
        Calculate a political relevance score and identify matching categories.
        Returns a tuple of (score, matching_categories).
        """
        score = 0.0
        matching_categories = []
        text = f"{title} {description}".lower()
        
        for category, keywords in self.political_keywords.items():
            matches = sum(1 for keyword in keywords if keyword.lower() in text)
            if matches > 0:
                score += matches * 0.2  # Each match adds 0.2 to the score
                matching_categories.append(category)
        
        return min(score, 1.0), matching_categories

    def search_political_videos(self, max_results: int = 50) -> List[Dict]:
        """
        Enhanced search for JRE videos with political content using categorized keywords.
        Limited to videos from 2017-2025.
        """
        videos = []
        seen_video_ids = set()
        
        # Use test keywords in test mode, all keywords otherwise
        keywords = self.test_keywords if self.test_mode else [
            kw for category in self.political_keywords.values() for kw in category
        ]
        
        # Adjust max_results and rate limiting based on test mode
        max_results = 5 if self.test_mode else max_results
        search_delay = 2 if self.test_mode else 1
        error_delay = 10 if self.test_mode else 5
        
        for keyword in keywords:
            try:
                logging.info(f"Searching for videos with keyword: {keyword}")
                request = self.youtube.search().list(
                    part="snippet",
                    channelId=self.channel_id,
                    q=keyword,
                    type="video",
                    maxResults=max_results,
                    publishedAfter="2017-01-01T00:00:00Z",
                    publishedBefore="2025-01-01T00:00:00Z",
                    order="relevance"
                )
                response = request.execute()
                
                for item in response['items']:
                    # Check if this is a video result
                    if item['id']['kind'] != 'youtube#video':
                        logging.debug(f"Skipping non-video result: {item['id']['kind']}")
                        continue
                        
                    # Get video ID from the response
                    video_id = item['id']['videoId']
                    if not video_id:
                        logging.warning(f"No videoId found in response item: {item}")
                        continue
                    
                    # Skip if we've already processed this video
                    if video_id in seen_video_ids:
                        continue
                    seen_video_ids.add(video_id)
                    
                    # Get additional video details
                    try:
                        video_response = self.youtube.videos().list(
                            part="statistics,contentDetails,snippet",
                            id=video_id
                        ).execute()
                        
                        if not video_response['items']:
                            continue
                            
                        video_data = video_response['items'][0]
                        stats = video_data['statistics']
                        snippet = video_data['snippet']
                        
                        # Calculate political score and matching categories
                        political_score, matching_categories = self.calculate_political_score(
                            snippet['title'],
                            snippet['description']
                        )
                        
                        # Only include videos with significant political content
                        if political_score < 0.3:  # Threshold for political relevance
                            continue
                        
                        # Extract episode number and guest name
                        episode_number = self.extract_episode_number(snippet['title'])
                        guest_name = self.extract_guest_name(snippet['title'])
                        
                        # Create or get guest first
                        guest = None
                        if guest_name:
                            guest = self.db.get_or_create_guest(guest_name)
                        
                        video_info = {
                            'video_id': video_id,
                            'title': snippet['title'],
                            'published_at': snippet['publishedAt'],
                            'description': snippet['description'],
                            'view_count': int(stats.get('viewCount', 0)),
                            'like_count': int(stats.get('likeCount', 0)),
                            'comment_count': int(stats.get('commentCount', 0)),
                            'duration': video_data['contentDetails']['duration'],
                            'political_score': political_score,
                            'political_categories': matching_categories,
                            'episode_number': episode_number,
                            'guest_id': guest.id if guest else None,
                            'thumbnail_url': snippet['thumbnails']['high']['url'],
                            'tags': snippet.get('tags', []),
                            'category_id': snippet.get('categoryId'),
                            'channel_title': snippet['channelTitle']
                        }
                        
                        videos.append(video_info)
                        logging.info(f"Found politically relevant video: {video_info['title']} (Score: {political_score})")
                        
                    except Exception as e:
                        logging.error(f"Error getting additional details for video {video_id}: {e}")
                
                # Rate limiting based on test mode
                time.sleep(search_delay)
                        
            except HttpError as e:
                logging.error(f"Error searching for videos with keyword '{keyword}': {e}")
                time.sleep(error_delay)
                
        return videos

    def process_videos(self, max_videos: int = 100):
        """
        Main function to process videos and store in database.
        """
        # Initialize database if needed
        self.db.init_db()
        
        # Search for political videos
        videos = self.search_political_videos(max_videos)
        if not videos:
            logging.warning("No videos found to process")
            return
            
        logging.info(f"\nFound {len(videos)} potentially relevant videos")
        
        # Process each video
        successful = 0
        for i, video in enumerate(videos, 1):
            logging.info(f"\nProcessing video {i}/{len(videos)}")
            try:
                if self.process_video(video):
                    successful += 1
                else:
                    logging.error(f"Failed to process video {video.get('video_id', 'unknown')}: {video.get('title', 'unknown title')}")
            except Exception as e:
                logging.error(f"Error processing video {video.get('video_id', 'unknown')}: {str(e)}")
                logging.error(f"Video data: {video}")
                self.monitor.record_error("processing_error", video.get('video_id', 'unknown'))
            time.sleep(1)  # Rate limiting
        
        # Generate final report
        summary = self.monitor.get_collection_summary()
        logging.info("\nCollection Summary:")
        logging.info(f"Total videos processed: {summary['total_videos']}")
        logging.info(f"Successfully processed: {summary['processed_videos']}")
        logging.info(f"Success rate: {summary['success_rate']}")
        logging.info(f"Total segments collected: {summary['total_segments']}")
        
        # Check for duplicates
        duplicates = self.quality_checker.check_duplicate_videos()
        if duplicates:
            logging.warning(f"\nFound {len(duplicates)} potential duplicate video groups")
            for group in duplicates:
                logging.warning(f"Duplicate group: {[v.title for v in group['videos']]}")

    def process_video(self, video_data: Dict) -> bool:
        """
        Process a single video: fetch transcript and store in database.
        Returns True if successful, False otherwise.
        """
        video_id = video_data.get('video_id')
        if not video_id:
            logging.error("No video_id provided in video_data")
            return False
            
        start_time = time.time()
        
        # Check if video already exists in database
        existing_video = self.db.get_video(video_id)
        if existing_video:
            if existing_video.is_processed:
                logging.info(f"Video {video_id} already exists and is processed, skipping...")
                return True
            else:
                logging.info(f"Video {video_id} exists but not processed, reprocessing...")
                # Delete existing video to start fresh
                self.db.delete_video(video_id)
        
        # Get transcript
        transcript = self.get_transcript_with_backoff(video_id)
        if not transcript:
            logging.error(f"No transcript available for video {video_id}")
            self.monitor.record_error("no_transcript", video_id)
            return False
        
        # Store video in database
        try:
            video = self.db.add_video(video_data)
            if not video:
                logging.error(f"Failed to add video {video_id} to database")
                self.monitor.record_error("database_error", video_id)
                return False
        except Exception as e:
            logging.error(f"Database error for video {video_id}: {str(e)}")
            logging.error(f"Video data: {video_data}")
            self.monitor.record_error("database_error", video_id)
            return False
        
        # Store transcript segments
        try:
            self.db.add_transcript_segments(video_id, transcript)
        except Exception as e:
            logging.error(f"Error storing transcript segments for video {video_id}: {str(e)}")
            self.monitor.record_error("transcript_error", video_id)
            return False
        
        # Run quality checks
        try:
            is_complete, transcript_issues = self.quality_checker.check_transcript_completeness(video_id)
            is_valid, metadata_issues = self.quality_checker.validate_video_metadata(video_id)
            
            if not is_complete or not is_valid:
                logging.warning(f"Quality issues found for video {video_id}:")
                if transcript_issues:
                    logging.warning(f"Transcript issues: {transcript_issues}")
                if metadata_issues:
                    logging.warning(f"Metadata issues: {metadata_issues}")
        except Exception as e:
            logging.error(f"Error running quality checks for video {video_id}: {str(e)}")
            self.monitor.record_error("quality_check_error", video_id)
        
        # Mark as processed and update statistics
        try:
            if self.db.mark_video_processed(video_id):
                # Verify the video was actually marked as processed
                processed_video = self.db.get_video(video_id)
                if processed_video and processed_video.is_processed:
                    processing_time = time.time() - start_time
                    self.monitor.update_video_processed(
                        video_id,
                        success=True,
                        segment_count=len(transcript),
                        processing_time=processing_time
                    )
                    logging.info(f"Successfully processed video {video_id}: {video_data.get('title', 'unknown title')}")
                    return True
                else:
                    logging.error(f"Video {video_id} was not properly marked as processed")
                    self.monitor.record_error("status_verification_error", video_id)
                    return False
            else:
                logging.error(f"Failed to mark video {video_id} as processed")
                return False
        except Exception as e:
            logging.error(f"Error updating video status for {video_id}: {str(e)}")
            self.monitor.record_error("status_update_error", video_id)
            return False

if __name__ == "__main__":
    fetcher = JRETranscriptFetcher()
    fetcher.process_videos()