"""
RDS Configuration
Replace these values with your actual RDS connection details
"""

# RDS Connection Settings
DB_HOST = 'my-tutorial-db.c8lou8gws4xf.us-east-1.rds.amazonaws.com'
DB_PORT = 3306
DB_USER = 'admin'
DB_PASSWORD = 'MyPassword123!'  # ← Change this!
DB_NAME = 'tutorial_db'

# Connection Pool Settings
POOL_SIZE = 5
MAX_OVERFLOW = 10