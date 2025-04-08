"""
Test script for JRE transcript collection and dashboard.
"""

import os
import sys
import logging
from datetime import datetime, timedelta, UTC
import json

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_collection.youtube_transcript_fetcher import JRETranscriptFetcher
from database.db_manager import DatabaseManager
from database.models import Video, Base

# Set up logging first, before any other imports
logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(logs_dir, exist_ok=True)
log_file = os.path.join(logs_dir, 'test_collection.log')

if os.path.exists(log_file):
    os.remove(log_file)  # Clear the log file if it exists

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def setup_test_environment():
    """Set up test environment and logging."""
    # Create test stats file
    test_stats = {
        'start_time': datetime.now(UTC).isoformat(),
        'last_update': datetime.now(UTC).isoformat(),
        'total_videos': 0,
        'processed_videos': 0,
        'failed_videos': 0,
        'total_segments': 0,
        'total_political_segments': 0,
        'videos_by_year': {},
        'videos_by_guest': {},
        'political_categories': {},
        'processing_times': [],
        'error_counts': {},
        'daily_stats': {}
    }
    
    stats_file = os.path.join(logs_dir, 'test_collection_stats.json')
    with open(stats_file, 'w') as f:
        json.dump(test_stats, f, indent=2)

def test_database_connection():
    """Test database connection and initialization."""
    logging.info("Testing database connection...")
    try:
        db = DatabaseManager()
        # Drop all tables and recreate them
        Base.metadata.drop_all(db.engine)
        db.init_db()
        logging.info("Database connection successful")
        return True
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return False

def test_collection(max_videos: int = 5):
    """Test video collection with a small number of videos."""
    logging.info(f"Testing collection with {max_videos} videos...")
    try:
        fetcher = JRETranscriptFetcher(test_mode=True)
        fetcher.process_videos(max_videos)
        
        # Get and display collection summary
        summary = fetcher.monitor.get_collection_summary()
        logging.info("\nTest Collection Summary:")
        logging.info(f"Total videos processed: {summary['total_videos']}")
        logging.info(f"Successfully processed: {summary['processed_videos']}")
        logging.info(f"Success rate: {summary['success_rate']}")
        logging.info(f"Total segments collected: {summary['total_segments']}")
        logging.info(f"Error summary: {summary['error_summary']}")
        
        # Check for duplicates
        duplicates = fetcher.quality_checker.check_duplicate_videos()
        if duplicates:
            logging.warning(f"\nFound {len(duplicates)} potential duplicate video groups")
            for group in duplicates:
                logging.warning(f"Duplicate group: {[v.title for v in group['videos']]}")
        
        logging.info("Collection test completed")
        return True
    except Exception as e:
        logging.error(f"Collection test failed: {e}")
        return False

def main():
    """Run all tests."""
    logging.info("Starting JRE collection system tests...")
    
    # Setup
    setup_test_environment()
    
    # Run tests
    tests = [
        ("Database Connection", test_database_connection),
        ("Collection Test", lambda: test_collection(5))
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            logging.error(f"Error in {test_name}: {e}")
            results[test_name] = False
    
    # Report results
    logging.info("\nTest Results:")
    for test_name, success in results.items():
        status = "PASSED" if success else "FAILED"
        logging.info(f"{test_name}: {status}")
    
    if all(results.values()):
        logging.info("\nAll tests passed successfully!")
    else:
        logging.error("\nSome tests failed. Check the logs for details.")

if __name__ == "__main__":
    main() 