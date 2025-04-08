"""
Database manager for handling PostgreSQL operations.
"""

import os
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from datetime import datetime, UTC
from typing import List, Dict, Optional, Union
import logging
import time

from .models import Base, Video, TranscriptSegment, PoliticalSegment, Guest

class DatabaseManager:
    def __init__(self):
        # Get database URL from environment variable or use default
        db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/jre')
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        
    def init_db(self):
        """Initialize the database schema."""
        Base.metadata.create_all(self.engine)
        
    def get_or_create_guest(self, name: str, description: Optional[str] = None) -> Guest:
        """Get an existing guest or create a new one."""
        session = self.Session()
        try:
            guest = session.query(Guest).filter(Guest.name == name).first()
            if not guest:
                guest = Guest(name=name, description=description)
                session.add(guest)
                session.commit()
            return guest
        except Exception as e:
            print(f"Error getting/creating guest: {e}")
            session.rollback()
            return None
        finally:
            session.close()
            
    def add_video(self, video_data: Dict) -> Optional[Video]:
        """Add a new video to the database."""
        session = self.Session()
        try:
            # Convert published_at to datetime if it's a string
            if isinstance(video_data['published_at'], str):
                try:
                    # YouTube API returns dates in ISO 8601 format with 'Z' timezone
                    video_data['published_at'] = datetime.fromisoformat(
                        video_data['published_at'].replace('Z', '+00:00')
                    )
                except ValueError as e:
                    logging.error(f"Error parsing date {video_data['published_at']}: {e}")
                    return None
            
            # Ensure published_at is timezone-aware
            if video_data['published_at'].tzinfo is None:
                video_data['published_at'] = video_data['published_at'].replace(tzinfo=UTC)
            
            # Handle guest information
            if 'guest_id' in video_data and video_data['guest_id']:
                guest = session.query(Guest).get(video_data['guest_id'])
                if not guest:
                    logging.error(f"Guest with ID {video_data['guest_id']} not found")
                    return None
                del video_data['guest_id']
                video_data['guest'] = guest
            
            # Create video object
            video = Video(**video_data)
            session.add(video)
            session.commit()
            return video
        except IntegrityError:
            session.rollback()
            return None
        except Exception as e:
            logging.error(f"Error adding video: {e}")
            session.rollback()
            return None
        finally:
            session.close()
            
    def add_transcript_segments(self, video_id: str, segments: List[Dict]):
        """Add transcript segments for a video."""
        session = self.Session()
        try:
            for segment_data in segments:
                segment = TranscriptSegment(
                    video_id=video_id,
                    text=segment_data['text'],
                    start_time=segment_data['start'],
                    duration=segment_data['duration']
                )
                session.add(segment)
            session.commit()
        except Exception as e:
            print(f"Error adding transcript segments: {e}")
            session.rollback()
        finally:
            session.close()
            
    def add_political_segment(self, segment_data: Dict):
        """Add a political segment to the database."""
        session = self.Session()
        try:
            segment = PoliticalSegment(**segment_data)
            session.add(segment)
            session.commit()
            return segment
        except Exception as e:
            print(f"Error adding political segment: {e}")
            session.rollback()
            return None
        finally:
            session.close()
            
    def get_video(self, video_id: str) -> Optional[Video]:
        """Get a video by its ID."""
        session = self.Session()
        try:
            return session.query(Video).filter(Video.video_id == video_id).first()
        finally:
            session.close()
            
    def get_guest_by_name(self, guest_name: str) -> Optional[Guest]:
        """Get a guest by their name."""
        session = self.Session()
        try:
            return session.query(Guest).filter(Guest.name == guest_name).first()
        finally:
            session.close()
            
    def get_videos_by_guest(self, guest_name: str) -> List[Video]:
        """Get all videos featuring a specific guest."""
        session = self.Session()
        try:
            # First check if the guest exists
            guest = self.get_guest_by_name(guest_name)
            if not guest:
                return []
                
            # Then get all videos for this guest
            return session.query(Video)\
                .filter(Video.guest_id == guest.id)\
                .order_by(Video.published_at)\
                .all()
        finally:
            session.close()
            
    def get_political_videos(self, min_score: float = 0.3) -> List[Video]:
        """Get videos with political content above a certain score."""
        session = self.Session()
        try:
            return session.query(Video)\
                .filter(Video.political_score >= min_score)\
                .order_by(Video.political_score.desc())\
                .all()
        finally:
            session.close()
            
    def get_transcript_segments(self, video_id: str) -> List[TranscriptSegment]:
        """Get all transcript segments for a video."""
        session = self.Session()
        try:
            return session.query(TranscriptSegment)\
                .filter(TranscriptSegment.video_id == video_id)\
                .order_by(TranscriptSegment.start_time)\
                .all()
        finally:
            session.close()
            
    def get_political_segments(self, video_id: str = None) -> List[PoliticalSegment]:
        """Get political segments, optionally filtered by video ID."""
        session = self.Session()
        try:
            query = session.query(PoliticalSegment)
            if video_id:
                query = query.filter(PoliticalSegment.video_id == video_id)
            return query.order_by(PoliticalSegment.start_time).all()
        finally:
            session.close()
            
    def mark_video_processed(self, video_id: str) -> bool:
        """Mark a video as processed."""
        max_retries = 3
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            session = self.Session()
            try:
                # Get the video with a lock to prevent concurrent updates
                video = session.query(Video).filter(Video.video_id == video_id).with_for_update().first()
                if not video:
                    logging.error(f"Video {video_id} not found when trying to mark as processed")
                    return False
                
                # Update the video status
                video.is_processed = True
                video.updated_at = datetime.now(UTC)
                
                # Flush changes to verify they work
                session.flush()
                
                # Verify the update
                updated_video = session.query(Video).filter(Video.video_id == video_id).first()
                if not updated_video or not updated_video.is_processed:
                    logging.error(f"Failed to verify video {video_id} was marked as processed")
                    session.rollback()
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    return False
                
                # Commit the changes
                session.commit()
                
                # Final verification
                final_video = self.get_video(video_id)
                if not final_video or not final_video.is_processed:
                    logging.error(f"Final verification failed for video {video_id}")
                    return False
                    
                return True
                
            except Exception as e:
                logging.error(f"Error marking video {video_id} as processed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                session.rollback()
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return False
            finally:
                session.close()
                
        return False
            
    def delete_video(self, video_id: str) -> bool:
        """Delete a video and all its associated segments."""
        session = self.Session()
        try:
            video = session.query(Video).filter(Video.video_id == video_id).first()
            if video:
                session.delete(video)
                session.commit()
                return True
            return False
        except Exception as e:
            print(f"Error deleting video: {e}")
            session.rollback()
            return False
        finally:
            session.close()