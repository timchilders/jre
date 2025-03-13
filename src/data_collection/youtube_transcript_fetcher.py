"""
Script to fetch transcripts from Joe Rogan Experience YouTube videos.
"""

import os
from typing import List, Dict
import json
from datetime import datetime
import time
from tqdm import tqdm
from youtube_transcript_api import YouTubeTranscriptApi
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

class JRETranscriptFetcher:
    def __init__(self):
        self.api_key = os.getenv('YOUTUBE_API_KEY')
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)
        # JRE Clips channel
        self.channel_id = "UCnxGkOGNMqQEUMvroOWps6Q"  # JRE Clips channel ID
        self.output_dir = "../data/raw_transcripts"
        
    def get_transcript_with_backoff(self, video_id):
        """
        Attempt to get transcript with exponential backoff for rate limiting
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
                    raise e
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
            "socialism", "capitalism", "marxism",
            # Government and policy
            "policy", "government", "congress", "senate",
            "legislation", "law", "regulation",
            # Political figures
            "trump", "biden", "obama", "clinton", "sanders",
            "desantis", "aoc", "pelosi", "mcconnell",
            # Current political issues
            "immigration", "border", "gun control", "healthcare",
            "climate change", "foreign policy", "war", "military",
            "censorship", "free speech", "cancel culture",
            # Political events
            "january 6", "protest", "riot", "supreme court",
            "presidential", "campaign", "debate"
        ]
        
        videos = []
        for keyword in political_keywords:
            try:
                # Search in chronological order from 2017
                # Get more detailed video information
                request = self.youtube.search().list(
                    part="snippet",
                    channelId=self.channel_id,
                    q=keyword,
                    type="video",
                    maxResults=max_results,
                    publishedAfter="2017-01-01T00:00:00Z",
                    publishedBefore="2025-01-01T00:00:00Z",
                    order="relevance"  # Changed to relevance to get most relevant political content
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
                                'view_count': stats.get('viewCount'),
                                'like_count': stats.get('likeCount'),
                                'comment_count': stats.get('commentCount'),
                                'duration': video_response['items'][0]['contentDetails']['duration']
                            })
                    except Exception as e:
                        print(f"Error getting additional details for video {item['id']['videoId']}: {e}")
                    if video_data not in videos:
                        videos.append(video_data)
                        
            except HttpError as e:
                print(f"Error searching for videos with keyword '{keyword}': {e}")
                
        return videos
    
    def fetch_transcript(self, video_id: str) -> List[Dict]:
        """
        Fetch transcript for a specific video with improved error handling.
        """
        try:
            transcript = self.get_transcript_with_backoff(video_id)
            return transcript
        except Exception as e:
            error_message = str(e)
            if "Subtitles are disabled" in error_message:
                print(f"Video {video_id} has subtitles disabled")
            elif "age-restricted" in error_message:
                print(f"Video {video_id} is age-restricted and requires authentication")
            else:
                print(f"Error fetching transcript for video {video_id}: {e}")
            return None
    
    def save_transcript(self, video_id: str, video_data: Dict, transcript: List[Dict]):
        """
        Save transcript and metadata to JSON file.
        """
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
        output_data = {
            'video_id': video_id,
            'metadata': video_data,
            'transcript': transcript,
            'fetch_date': datetime.now().isoformat()
        }
        
        filename = f"{self.output_dir}/{video_id}.json"
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2)
            
    def process_videos(self, max_videos: int = 100):
        """
        Main function to process videos and save transcripts.
        Includes rate limiting and progress tracking.
        """
        videos = self.search_political_videos(max_videos)
        print(f"Found {len(videos)} potentially relevant videos")
        
        # Create progress bar
        pbar = tqdm(videos, desc="Processing videos")
        
        for video in pbar:
            video_id = video['video_id']
            pbar.set_description(f"Processing video {video_id}")
            
            # Check if we already have this transcript
            if os.path.exists(f"{self.output_dir}/{video_id}.json"):
                pbar.set_description(f"Skipping existing video {video_id}")
                continue
            
            transcript = self.fetch_transcript(video_id)
            
            if transcript:
                self.save_transcript(video_id, video, transcript)
                pbar.set_description(f"Saved transcript for {video_id}")
            
            # Rate limiting - wait 1 second between requests
            time.sleep(1)

if __name__ == "__main__":
    fetcher = JRETranscriptFetcher()
    fetcher.process_videos()