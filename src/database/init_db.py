"""
Initialize the PostgreSQL database.
"""

import os
from db_manager import DatabaseManager

def main():
    # Create database manager
    db_manager = DatabaseManager()
    
    # Initialize database schema
    print("Initializing database schema...")
    db_manager.init_db()
    print("Database schema created successfully!")

if __name__ == "__main__":
    main()