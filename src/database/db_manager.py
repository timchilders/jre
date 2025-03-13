"""
Database manager for handling PostgreSQL operations.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from typing import List, Dict, Optional

from .models import Base, Video, TranscriptSegment, PoliticalSegment

class DatabaseManager:
    def __init__(self):
        # Get database URL from environment variable or use default
        db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/jre_db')
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        
    def init_db(self):
        """Initialize the database schema."""
        Base.metadata.create_all(self.engine)
        
    def add_video(self, video_data: Dict) -> Optional[Video]:
        """Add a new video to the database."""
        session = self.Session()
        try:
            # Convert published_at to datetime if it's a string
            if isinstance(video_data['published_at'], str):
                video_data['published_at'] = datetime.fromisoformat(
                    video_data['published_at'].replace('Z', '+00:00')
                )
            
            video = Video(**video_data)
            session.add(video)
            session.commit()
            return video
        except IntegrityError:
            session.rollback()
            return None
        except Exception as e:
            print(f"Error adding video: {e}")
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