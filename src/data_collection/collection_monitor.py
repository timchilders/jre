"""
Module for monitoring collection progress and generating statistics.
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import json
from collections import defaultdict
import os

class CollectionMonitor:
    def __init__(self, db_manager):
        self.db = db_manager
        self.logger = logging.getLogger(__name__)
        
        # Set up logs directory
        self.logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
        os.makedirs(self.logs_dir, exist_ok=True)
        
        self.stats_file = os.path.join(self.logs_dir, "collection_stats.json")
        self._load_stats()
        
    def _load_stats(self):
        """Load statistics from file or initialize new stats."""
        if os.path.exists(self.stats_file):
            try:
                with open(self.stats_file, 'r') as f:
                    self.stats = json.load(f)
            except Exception as e:
                self.logger.error(f"Error loading stats file: {e}")
                self.stats = self._init_stats()
        else:
            self.stats = self._init_stats()
            
    def _init_stats(self) -> Dict:
        """Initialize statistics structure."""
        return {
            'start_time': datetime.utcnow().isoformat(),
            'last_update': datetime.utcnow().isoformat(),
            'total_videos': 0,
            'processed_videos': 0,
            'failed_videos': 0,
            'total_segments': 0,
            'total_political_segments': 0,
            'videos_by_year': defaultdict(int),
            'videos_by_guest': defaultdict(int),
            'political_categories': defaultdict(int),
            'processing_times': [],
            'error_counts': defaultdict(int),
            'daily_stats': defaultdict(lambda: {
                'videos_processed': 0,
                'segments_collected': 0,
                'errors': 0
            })
        }
        
    def _save_stats(self):
        """Save current statistics to file."""
        try:
            with open(self.stats_file, 'w') as f:
                json.dump(self.stats, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving stats file: {e}")
            
    def update_video_processed(self, video_id: str, success: bool, 
                             segment_count: int = 0, processing_time: float = 0):
        """Update statistics after processing a video."""
        video = self.db.get_video(video_id)
        if not video:
            return
            
        # Update basic counts
        self.stats['total_videos'] += 1
        if success:
            self.stats['processed_videos'] += 1
            self.stats['total_segments'] += segment_count
        else:
            self.stats['failed_videos'] += 1
            
        # Update by year
        year = video.published_at.year
        self.stats['videos_by_year'][str(year)] += 1
        
        # Update by guest
        if video.guest:
            self.stats['videos_by_guest'][video.guest.name] += 1
            
        # Update political categories
        if video.political_categories:
            for category in video.political_categories:
                self.stats['political_categories'][category] += 1
                
        # Update processing times
        if processing_time > 0:
            self.stats['processing_times'].append(processing_time)
            
        # Update daily stats
        today = datetime.utcnow().date().isoformat()
        self.stats['daily_stats'][today]['videos_processed'] += 1
        self.stats['daily_stats'][today]['segments_collected'] += segment_count
        
        self.stats['last_update'] = datetime.utcnow().isoformat()
        self._save_stats()
        
    def record_error(self, error_type: str, video_id: Optional[str] = None):
        """Record an error in the statistics."""
        self.stats['error_counts'][error_type] += 1
        if video_id:
            self.stats['failed_videos'] += 1
        today = datetime.utcnow().date().isoformat()
        self.stats['daily_stats'][today]['errors'] += 1
        self._save_stats()
        
    def get_collection_summary(self) -> Dict:
        """Generate a summary of collection progress."""
        summary = {
            'collection_started': self.stats['start_time'],
            'last_update': self.stats['last_update'],
            'total_videos': self.stats['total_videos'],
            'processed_videos': self.stats['processed_videos'],
            'success_rate': f"{(self.stats['processed_videos']/self.stats['total_videos']*100):.2f}%" 
                if self.stats['total_videos'] > 0 else "N/A",
            'total_segments': self.stats['total_segments'],
            'average_segments_per_video': self.stats['total_segments']/self.stats['processed_videos'] 
                if self.stats['processed_videos'] > 0 else 0,
            'top_guests': sorted(
                self.stats['videos_by_guest'].items(),
                key=lambda x: x[1],
                reverse=True
            )[:10],
            'top_political_categories': sorted(
                self.stats['political_categories'].items(),
                key=lambda x: x[1],
                reverse=True
            )[:5],
            'error_summary': dict(self.stats['error_counts']),
            'processing_time_stats': {
                'average': sum(self.stats['processing_times'])/len(self.stats['processing_times']) 
                    if self.stats['processing_times'] else 0,
                'min': min(self.stats['processing_times']) if self.stats['processing_times'] else 0,
                'max': max(self.stats['processing_times']) if self.stats['processing_times'] else 0
            }
        }
        return summary
        
    def get_daily_report(self, days: int = 7) -> Dict:
        """Generate a report of daily collection statistics."""
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=days)
        
        daily_data = []
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.isoformat()
            daily_data.append({
                'date': date_str,
                **self.stats['daily_stats'][date_str]
            })
            current_date += timedelta(days=1)
            
        return {
            'period': {
                'start': start_date.isoformat(),
                'end': end_date.isoformat()
            },
            'daily_data': daily_data
        }
        
    def get_guest_statistics(self) -> Dict:
        """Generate statistics about guests and their appearances."""
        guest_stats = {}
        for guest_name, count in self.stats['videos_by_guest'].items():
            guest = self.db.get_guest_by_name(guest_name)
            if not guest:
                continue
                
            videos = self.db.get_videos_by_guest(guest_name)
            if not videos:
                guest_stats[guest_name] = {
                    'appearance_count': count,
                    'first_appearance': None,
                    'last_appearance': None,
                    'average_political_score': 0
                }
                continue
                
            # Get all valid political scores
            political_scores = [v.political_score for v in videos if v.political_score is not None]
            avg_political_score = sum(political_scores) / len(political_scores) if political_scores else 0
            
            # Get all valid dates
            valid_dates = [v.published_at for v in videos if v.published_at is not None]
            if valid_dates:
                first_appearance = min(valid_dates)
                last_appearance = max(valid_dates)
            else:
                first_appearance = None
                last_appearance = None
                
            guest_stats[guest_name] = {
                'appearance_count': count,
                'first_appearance': first_appearance.isoformat() if first_appearance else None,
                'last_appearance': last_appearance.isoformat() if last_appearance else None,
                'average_political_score': avg_political_score
            }
            
        return guest_stats 