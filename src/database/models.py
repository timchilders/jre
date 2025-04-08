"""
SQLAlchemy models for the JRE transcript database.
"""

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, JSON, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime, UTC

Base = declarative_base()

class Guest(Base):
    __tablename__ = 'guests'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(Text)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    updated_at = Column(DateTime, default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))
    
    # Relationships
    videos = relationship("Video", back_populates="guest")

class Video(Base):
    __tablename__ = 'videos'
    
    id = Column(Integer, primary_key=True)
    video_id = Column(String, unique=True, nullable=False)
    title = Column(String, nullable=False)
    published_at = Column(DateTime, nullable=False)
    description = Column(Text)
    channel_title = Column(String)
    view_count = Column(Integer)
    like_count = Column(Integer)
    comment_count = Column(Integer)
    duration = Column(String)
    episode_number = Column(Integer)
    political_score = Column(Float)
    political_categories = Column(JSON)  # Store categories as JSON array
    thumbnail_url = Column(String)
    tags = Column(JSON)  # Store tags as JSON array
    category_id = Column(String)
    is_processed = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.now(UTC))
    updated_at = Column(DateTime, default=datetime.now(UTC))
    
    # Foreign Keys
    guest_id = Column(Integer, ForeignKey('guests.id'))
    
    # Relationships
    guest = relationship("Guest", back_populates="videos")
    transcript_segments = relationship("TranscriptSegment", back_populates="video", cascade="all, delete-orphan")
    political_segments = relationship("PoliticalSegment", back_populates="video", cascade="all, delete-orphan")

class TranscriptSegment(Base):
    __tablename__ = 'transcript_segments'
    
    id = Column(Integer, primary_key=True)
    video_id = Column(String, ForeignKey('videos.video_id'), nullable=False)
    text = Column(Text, nullable=False)
    start_time = Column(Float, nullable=False)
    duration = Column(Float, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    
    # Relationships
    video = relationship("Video", back_populates="transcript_segments")

class PoliticalSegment(Base):
    __tablename__ = 'political_segments'
    
    id = Column(Integer, primary_key=True)
    video_id = Column(String, ForeignKey('videos.video_id'), nullable=False)
    segment_text = Column(Text, nullable=False)
    start_time = Column(Float, nullable=False)
    end_time = Column(Float, nullable=False)
    keywords = Column(JSON)  # Store keywords as JSON array
    political_categories = Column(JSON)  # Store categories as JSON array
    sentiment_score = Column(Float)  # Add sentiment analysis score
    created_at = Column(DateTime, default=lambda: datetime.now(UTC))
    
    # Relationships
    video = relationship("Video", back_populates="political_segments")