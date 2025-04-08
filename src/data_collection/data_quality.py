"""
Module for data quality checks and validation of collected transcripts.
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import re
from collections import Counter
from database.models import Video  # Add this import

class DataQualityChecker:
    def __init__(self, db_manager):
        self.db = db_manager
        self.logger = logging.getLogger(__name__)
        
    def check_transcript_completeness(self, video_id: str) -> Tuple[bool, Dict]:
        """
        Check if a transcript is complete and valid.
        Returns (is_complete, issues) where issues is a dict of problems found.
        """
        issues = {}
        
        # Get video and transcript segments
        video = self.db.get_video(video_id)
        if not video:
            return False, {"error": "Video not found"}
            
        segments = self.db.get_transcript_segments(video_id)
        if not segments:
            return False, {"error": "No transcript segments found"}
            
        # Check for minimum segment count
        if len(segments) < 10:  # Arbitrary minimum
            issues["segment_count"] = f"Low segment count: {len(segments)}"
            
        # Check for time gaps
        time_gaps = []
        for i in range(len(segments) - 1):
            current_end = segments[i].start_time + segments[i].duration
            next_start = segments[i + 1].start_time
            if next_start - current_end > 5.0:  # 5 second gap threshold
                time_gaps.append((current_end, next_start))
        if time_gaps:
            issues["time_gaps"] = time_gaps
            
        # Check for empty or very short segments
        short_segments = []
        for segment in segments:
            if len(segment.text.strip()) < 5:  # 5 character minimum
                short_segments.append(segment.id)
        if short_segments:
            issues["short_segments"] = short_segments
            
        # Check for duplicate segments
        text_counts = Counter(segment.text.strip() for segment in segments)
        duplicates = [text for text, count in text_counts.items() if count > 1]
        if duplicates:
            issues["duplicates"] = duplicates
            
        return len(issues) == 0, issues
        
    def validate_video_metadata(self, video_id: str) -> Tuple[bool, Dict]:
        """
        Validate video metadata for completeness and correctness.
        Returns (is_valid, issues) where issues is a dict of problems found.
        """
        issues = {}
        video = self.db.get_video(video_id)
        if not video:
            return False, {"error": "Video not found"}
            
        # Check required fields
        required_fields = ['title', 'published_at', 'video_id', 'channel_title']
        for field in required_fields:
            if not getattr(video, field):
                issues[f"missing_{field}"] = f"Required field {field} is missing"
                
        # Validate dates
        if video.published_at:
            if video.published_at > datetime.utcnow():
                issues["future_date"] = "Publication date is in the future"
            if video.published_at < datetime(2017, 1, 1):
                issues["date_range"] = "Publication date is before 2017"
                
        # Validate episode number format
        if video.episode_number:
            if not isinstance(video.episode_number, int) or video.episode_number < 1:
                issues["episode_number"] = "Invalid episode number format"
                
        # Validate political score
        if video.political_score is not None:
            if not 0 <= video.political_score <= 1:
                issues["political_score"] = "Political score out of range"
                
        return len(issues) == 0, issues
        
    def check_duplicate_videos(self) -> List[Dict]:
        """
        Check for potential duplicate videos in the database.
        Returns a list of potential duplicate groups.
        """
        duplicates = []
        session = self.db.Session()
        try:
            # Get all videos ordered by title
            videos = session.query(Video).order_by(Video.title).all()
            
            # Group by similar titles
            current_group = []
            for i in range(len(videos) - 1):
                current = videos[i]
                next_video = videos[i + 1]
                
                # Simple similarity check (can be enhanced with fuzzy matching)
                if (current.title.lower() == next_video.title.lower() or
                    abs(current.episode_number - next_video.episode_number) <= 1):
                    if not current_group:
                        current_group.append(current)
                    current_group.append(next_video)
                else:
                    if len(current_group) > 1:
                        duplicates.append({
                            'videos': current_group,
                            'reason': 'similar_titles'
                        })
                    current_group = []
                    
            # Check last group
            if len(current_group) > 1:
                duplicates.append({
                    'videos': current_group,
                    'reason': 'similar_titles'
                })
                
        finally:
            session.close()
            
        return duplicates
        
    def generate_quality_report(self, video_id: str = None) -> Dict:
        """
        Generate a comprehensive quality report for a video or all videos.
        """
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'video_specific': {},
            'overall_metrics': {}
        }
        
        if video_id:
            # Generate report for specific video
            is_complete, transcript_issues = self.check_transcript_completeness(video_id)
            is_valid, metadata_issues = self.validate_video_metadata(video_id)
            
            report['video_specific'] = {
                'video_id': video_id,
                'transcript_complete': is_complete,
                'transcript_issues': transcript_issues,
                'metadata_valid': is_valid,
                'metadata_issues': metadata_issues
            }
        else:
            # Generate overall report
            session = self.db.Session()
            try:
                total_videos = session.query(Video).count()
                processed_videos = session.query(Video).filter(
                    Video.is_processed == True
                ).count()
                
                report['overall_metrics'] = {
                    'total_videos': total_videos,
                    'processed_videos': processed_videos,
                    'processing_rate': f"{(processed_videos/total_videos*100):.2f}%" if total_videos > 0 else "N/A",
                    'duplicate_videos': len(self.check_duplicate_videos())
                }
            finally:
                session.close()
                
        return report 