"""
Script to collect and manage transcript data from JRE Clips.
"""

import os
from youtube_transcript_fetcher import JRETranscriptFetcher
from datetime import datetime
import json
import pandas as pd

def main():
    # Initialize the transcript fetcher
    fetcher = JRETranscriptFetcher()
    
    # Create output directories if they don't exist
    os.makedirs("../data/raw_transcripts", exist_ok=True)
    os.makedirs("../data/processed_transcripts", exist_ok=True)
    
    # Fetch transcripts (limit to 100 videos initially as a test)
    print("Starting transcript collection...")
    fetcher.process_videos(max_videos=100)
    
    # Create a summary of collected data
    create_data_summary()

def create_data_summary():
    """Create a summary of collected transcripts."""
    transcript_dir = "../data/raw_transcripts"
    summary_data = []
    
    for filename in os.listdir(transcript_dir):
        if filename.endswith(".json"):
            with open(os.path.join(transcript_dir, filename), 'r') as f:
                data = json.load(f)
                
                # Calculate total duration and word count
                transcript = data['transcript']
                total_duration = sum(segment['duration'] for segment in transcript)
                total_words = sum(len(segment['text'].split()) for segment in transcript)
                
                summary_data.append({
                    'video_id': data['video_id'],
                    'title': data['metadata']['title'],
                    'published_at': data['metadata']['published_at'],
                    'duration_seconds': total_duration,
                    'word_count': total_words,
                    'fetch_date': data['fetch_date']
                })
    
    if summary_data:
        df = pd.DataFrame(summary_data)
        df['published_at'] = pd.to_datetime(df['published_at'])
        df['fetch_date'] = pd.to_datetime(df['fetch_date'])
        
        # Save summary to CSV
        df.to_csv("../data/transcript_summary.csv", index=False)
        
        print("\nData Collection Summary:")
        print(f"Total videos processed: {len(df)}")
        print(f"Date range: {df['published_at'].min()} to {df['published_at'].max()}")
        print(f"Average duration: {df['duration_seconds'].mean():.2f} seconds")
        print(f"Average word count: {df['word_count'].mean():.0f} words")
    else:
        print("No transcripts found in the data directory.")

if __name__ == "__main__":
    main()