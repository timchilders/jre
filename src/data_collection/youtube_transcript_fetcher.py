"""
Script to fetch transcripts from Joe Rogan Experience YouTube videos and store them in PostgreSQL.
"""

import os
import time
from typing import List, Dict, Optional
from datetime import datetime
from youtube_transcript_api import YouTubeTranscriptApi
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
import sys

# Add the parent directory to the path so we can import the database module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db_manager import DatabaseManager

load_dotenv()

class JRETranscriptFetcher:
    def __init__(self):
        self.api_key = os.getenv('YOUTUBE_API_KEY')
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)
        self.channel_id = "UCnxGkOGNMqQEUMvroOWps6Q"  # JRE Clips channel ID
        self.db = DatabaseManager()
        
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
                    print(f"Transcript error for {video_id}: {str(e)}")
                    return None
        return None

    def search_political_videos(self, max_results: int = 50) -> List[Dict]:
        """
        Search for JRE videos with political content using relevant keywords.
        Limited to videos from 2017-2025.
        """
        # Political topics and figures
        political_keywords = [
            # Core political terms
            "politics", "political", "election", "democracy",
            # Political parties and ideologies
            "democrat", "republican", "liberal", "conservative",
            "libertarian", "progressive", "left wing", "right wing",
            # Government and policy
            "policy", "government", "congress", "senate",
            # Political figures
            "trump", "biden", "obama", "clinton",
            # Current political issues
            "immigration", "healthcare", "climate change", "foreign policy",
            "censorship", "free speech"
        ]
        
        videos = []
        for keyword in political_keywords:
            try:
                print(f"Searching for videos with keyword: {keyword}")
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
                    video_data = {
                        'video_id': item['id']['videoId'],
                        'title': item['snippet']['title'],
                        'published_at': item['snippet']['publishedAt'],
                        'description': item['snippet']['description'],
                        'matching_keyword': keyword,
                        'channel_title': item['snippet']['channelTitle']
                    }
                    
                    # Get additional video details
                    try:
                        video_response = self.youtube.videos().list(
                            part="statistics,contentDetails",
                            id=item['id']['videoId']
                        ).execute()
                        
                        if video_response['items']:
                            stats = video_response['items'][0]['statistics']
                            video_data.update({
                                'view_count': int(stats.get('viewCount', 0)),
                                'like_count': int(stats.get('likeCount', 0)),
                                'comment_count': int(stats.get('commentCount', 0)),
                                'duration': video_response['items'][0]['contentDetails']['duration']
                            })
                    except Exception as e:
                        print(f"Error getting additional details for video {item['id']['videoId']}: {e}")
                    
                    if not any(v['video_id'] == video_data['video_id'] for v in videos):
                        videos.append(video_data)
                        print(f"Found video: {video_data['title']}")
                
                time.sleep(1)  # Rate limiting between keyword searches
                        
            except HttpError as e:
                print(f"Error searching for videos with keyword '{keyword}': {e}")
                time.sleep(5)  # Longer wait on error
                
        return videos

    def process_video(self, video_data: Dict) -> bool:
        """
        Process a single video: fetch transcript and store in database.
        Returns True if successful, False otherwise.
        """
        video_id = video_data['video_id']
        
        # Check if video already exists in database
        if self.db.get_video(video_id):
            print(f"Video {video_id} already exists in database, skipping...")
            return False
        
        try:
            # Get transcript
            transcript = self.get_transcript_with_backoff(video_id)
            if not transcript:
                print(f"No transcript available for video {video_id}")
                return False
            
            # Store video in database
            video = self.db.add_video(video_data)
            if not video:
                print(f"Failed to add video {video_id} to database")
                return False
            
            # Store transcript segments
            self.db.add_transcript_segments(video_id, transcript)
            print(f"Successfully processed video {video_id}: {video_data['title']}")
            return True
            
        except Exception as e:
            print(f"Error processing video {video_id}: {e}")
            return False

    def process_videos(self, max_videos: int = 100):
        """
        Main function to process videos and store in database.
        """
        # Initialize database if needed
        self.db.init_db()
        
        # Search for political videos
        videos = self.search_political_videos(max_videos)
        print(f"\nFound {len(videos)} potentially relevant videos")
        
        # Process each video
        successful = 0
        for i, video in enumerate(videos, 1):
            print(f"\nProcessing video {i}/{len(videos)}")
            if self.process_video(video):
                successful += 1
            time.sleep(1)  # Rate limiting
        
        print(f"\nSuccessfully processed {successful} out of {len(videos)} videos")

if __name__ == "__main__":
    fetcher = JRETranscriptFetcher()
    fetcher.process_videos()