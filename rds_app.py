"""
RDS Best Practices Demo
Demonstrates proper patterns for interacting with Amazon RDS (MySQL)
"""

import pymysql
from pymysql.cursors import DictCursor
from contextlib import contextmanager
import json
from datetime import datetime
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME


class RDSApp:
    """Handles RDS operations with best practices"""
    
    def __init__(self):
        """Initialize RDS connection parameters"""
        self.connection_params = {
            'host': DB_HOST,
            'port': DB_PORT,
            'user': DB_USER,
            'password': DB_PASSWORD,
            'database': DB_NAME,
            'cursorclass': DictCursor,  # Return results as dictionaries
            'autocommit': False  # Explicit transaction control
        }
    
    @contextmanager
    def get_connection(self):
        """
        BEST PRACTICE: Use context manager for connections
        - Automatically closes connection
        - Handles exceptions properly
        - Ensures no connection leaks
        """
        connection = pymysql.connect(**self.connection_params)
        try:
            yield connection
        except Exception as e:
            connection.rollback()
            raise
        finally:
            connection.close()
    
    def setup_tables(self):
        """
        Create database schema
        - Defines relationships with foreign keys
        - Sets up indexes for query performance
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Drop existing tables (for clean demo)
            cursor.execute("DROP TABLE IF EXISTS preferences")
            cursor.execute("DROP TABLE IF EXISTS activity")
            cursor.execute("DROP TABLE IF EXISTS users")
            
            # Create users table
            cursor.execute("""
                CREATE TABLE users (
                    user_id VARCHAR(50) PRIMARY KEY,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_email (email)
                )
            """)
            
            # Create activity table
            cursor.execute("""
                CREATE TABLE activity (
                    activity_id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id VARCHAR(50) NOT NULL,
                    login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    login_count INT DEFAULT 1,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                    INDEX idx_user_time (user_id, login_time)
                )
            """)
            
            # Create preferences table
            cursor.execute("""
                CREATE TABLE preferences (
                    user_id VARCHAR(50) PRIMARY KEY,
                    theme VARCHAR(20) DEFAULT 'light',
                    notifications BOOLEAN DEFAULT TRUE,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
            """)
            
            conn.commit()
            print("✓ Created database schema (users, activity, preferences)")
    
    def load_seed_data(self, filename='seed_data.json'):
        """
        Load initial data from JSON file
        - Useful for resetting database to known state
        - Uses transactions for data integrity
        """
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Insert users
                for user in data['users']:
                    cursor.execute("""
                        INSERT INTO users (user_id, email, first_name, last_name)
                        VALUES (%(user_id)s, %(email)s, %(first_name)s, %(last_name)s)
                    """, user)
                
                # Insert activity records
                for activity in data['activity']:
                    cursor.execute("""
                        INSERT INTO activity (user_id, login_time, login_count)
                        VALUES (%(user_id)s, %(login_time)s, %(login_count)s)
                    """, activity)
                
                # Insert preferences
                for pref in data['preferences']:
                    cursor.execute("""
                        INSERT INTO preferences (user_id, theme, notifications)
                        VALUES (%(user_id)s, %(theme)s, %(notifications)s)
                    """, pref)
                
                conn.commit()
                total = len(data['users']) + len(data['activity']) + len(data['preferences'])
                print(f"✓ Loaded {total} records from {filename}")
        
        except FileNotFoundError:
            print(f"✗ File {filename} not found")
            raise
        except Exception as e:
            print(f"✗ Error loading seed data: {str(e)}")
            raise
    
    def create_user(self, user_id, email, first_name, last_name):
        """
        BEST PRACTICE: Use parameterized queries to prevent SQL injection
        - Never concatenate user input into SQL strings
        - Let the driver handle escaping
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                # BEST PRACTICE: Parameterized query (prevents SQL injection)
                cursor.execute("""
                    INSERT INTO users (user_id, email, first_name, last_name)
                    VALUES (%s, %s, %s, %s)
                """, (user_id, email, first_name, last_name))
                
                conn.commit()
                print(f"✓ Created user {user_id}")
                return cursor.lastrowid
            
            except pymysql.IntegrityError as e:
                if 'Duplicate entry' in str(e):
                    print(f"✗ User {user_id} or email {email} already exists")
                else:
                    print(f"✗ Integrity error: {str(e)}")
                raise
    
    def get_user(self, user_id):
        """
        BEST PRACTICE: SELECT only needed columns
        - Reduces network transfer
        - Improves query performance
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT user_id, email, first_name, last_name, created_at
                FROM users
                WHERE user_id = %s
            """, (user_id,))
            
            user = cursor.fetchone()
            
            if user:
                print(f"✓ Retrieved user {user_id}")
                return user
            else:
                print(f"✗ User {user_id} not found")
                return None
    
    def get_user_with_preferences(self, user_id):
        """
        BEST PRACTICE: Use JOINs to combine related data
        - Single query instead of multiple round trips
        - Much more efficient than separate queries
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    u.user_id,
                    u.email,
                    u.first_name,
                    u.last_name,
                    p.theme,
                    p.notifications
                FROM users u
                LEFT JOIN preferences p ON u.user_id = p.user_id
                WHERE u.user_id = %s
            """, (user_id,))
            
            result = cursor.fetchone()
            
            if result:
                print(f"✓ Retrieved user {user_id} with preferences")
                return result
            else:
                print(f"✗ User {user_id} not found")
                return None
    
    def get_user_activity(self, user_id, start_time=None, end_time=None):
        """
        BEST PRACTICE: Use WHERE clauses with indexes
        - Filter on indexed columns (user_id, login_time)
        - Dynamic query building based on parameters
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Build query dynamically based on parameters
            query = """
                SELECT activity_id, user_id, login_time, login_count
                FROM activity
                WHERE user_id = %s
            """
            params = [user_id]
            
            if start_time:
                query += " AND login_time >= %s"
                params.append(start_time)
            
            if end_time:
                query += " AND login_time <= %s"
                params.append(end_time)
            
            query += " ORDER BY login_time ASC"
            
            cursor.execute(query, params)
            activities = cursor.fetchall()
            
            print(f"✓ Found {len(activities)} activity records for {user_id}")
            return activities
    
    def update_login_count(self, user_id, activity_id, increment=1):
        """
        BEST PRACTICE: Atomic updates with expressions
        - Use SQL expressions for atomic operations
        - Avoids race conditions
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE activity
                SET login_count = login_count + %s
                WHERE user_id = %s AND activity_id = %s
            """, (increment, user_id, activity_id))
            
            conn.commit()
            
            if cursor.rowcount > 0:
                print(f"✓ Updated login count for activity {activity_id}")
                return cursor.rowcount
            else:
                print(f"✗ Activity {activity_id} not found")
                return 0
    
    def update_preferences(self, user_id, theme=None, notifications=None):
        """
        BEST PRACTICE: Update only specified fields
        - Dynamic UPDATE statement
        - Only changes what's provided
        """
        if theme is None and notifications is None:
            print("✗ No updates specified")
            return None
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Build UPDATE statement dynamically
            updates = []
            params = []
            
            if theme is not None:
                updates.append("theme = %s")
                params.append(theme)
            
            if notifications is not None:
                updates.append("notifications = %s")
                params.append(notifications)
            
            params.append(user_id)
            
            query = f"""
                UPDATE preferences
                SET {', '.join(updates)}
                WHERE user_id = %s
            """
            
            cursor.execute(query, params)
            conn.commit()
            
            if cursor.rowcount > 0:
                print(f"✓ Updated preferences for {user_id}")
                return self.get_user_with_preferences(user_id)
            else:
                print(f"✗ No preferences found for {user_id}")
                return None
    
    def search_users_by_email(self, email_pattern):
        """
        BEST PRACTICE: Use LIKE for pattern matching
        - Indexed column (email) for performance
        - Useful for search functionality
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT user_id, email, first_name, last_name
                FROM users
                WHERE email LIKE %s
            """, (f"%{email_pattern}%",))
            
            users = cursor.fetchall()
            print(f"✓ Found {len(users)} users matching '{email_pattern}'")
            return users
    
    def get_activity_summary(self):
        """
        BEST PRACTICE: Use aggregate functions with GROUP BY
        - Compute statistics in database (faster)
        - JOINs with aggregation
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    u.user_id,
                    u.first_name,
                    u.last_name,
                    COUNT(a.activity_id) as total_activities,
                    SUM(a.login_count) as total_logins,
                    MAX(a.login_time) as last_login
                FROM users u
                LEFT JOIN activity a ON u.user_id = a.user_id
                GROUP BY u.user_id, u.first_name, u.last_name
                ORDER BY total_logins DESC
            """)
            
            summary = cursor.fetchall()
            print(f"✓ Generated activity summary for {len(summary)} users")
            return summary
    
    def delete_user(self, user_id):
        """
        BEST PRACTICE: Foreign key constraints handle cascading deletes
        - ON DELETE CASCADE automatically removes related records
        - Maintains referential integrity
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                DELETE FROM users
                WHERE user_id = %s
            """, (user_id,))
            
            conn.commit()
            
            if cursor.rowcount > 0:
                print(f"✓ Deleted user {user_id} (and related records)")
                return True
            else:
                print(f"✗ User {user_id} not found")
                return False
    
    def create_user_with_preferences(self, user_id, email, first_name, last_name, 
                                     theme='light', notifications=True):
        """
        BEST PRACTICE: Use transactions for multi-table operations
        - All succeed or all fail (atomicity)
        - Maintains data consistency
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                # Insert user
                cursor.execute("""
                    INSERT INTO users (user_id, email, first_name, last_name)
                    VALUES (%s, %s, %s, %s)
                """, (user_id, email, first_name, last_name))
                
                # Insert preferences
                cursor.execute("""
                    INSERT INTO preferences (user_id, theme, notifications)
                    VALUES (%s, %s, %s)
                """, (user_id, theme, notifications))
                
                # BEST PRACTICE: Explicit commit for transaction
                conn.commit()
                print(f"✓ Created user {user_id} with preferences (transaction)")
                return True
            
            except Exception as e:
                # BEST PRACTICE: Rollback on error
                conn.rollback()
                print(f"✗ Transaction failed: {str(e)}")
                raise
    
    def batch_get_users(self, user_ids):
        """
        BEST PRACTICE: Use IN clause for batch retrieval
        - Single query instead of multiple queries
        - More efficient than looping
        """
        if not user_ids:
            return []
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create placeholders for IN clause
            placeholders = ','.join(['%s'] * len(user_ids))
            
            cursor.execute(f"""
                SELECT user_id, email, first_name, last_name
                FROM users
                WHERE user_id IN ({placeholders})
            """, user_ids)
            
            users = cursor.fetchall()
            print(f"✓ Retrieved {len(users)} users in batch")
            return users


def demo():
    """Demonstrate RDS best practices"""
    app = RDSApp()
    
    print("\n=== 0. SETUP DATABASE ===")
    app.setup_tables()
    
    print("\n=== 1. LOAD SEED DATA ===")
    app.load_seed_data()
    
    print("\n=== 2. CREATE USER (with parameterized query) ===")
    try:
        app.create_user('user999', 'user999@example.com', 'Bob', 'Williams')
    except pymysql.IntegrityError:
        pass
    
    print("\n=== 3. GET USER (simple SELECT) ===")
    user = app.get_user('user123')
    if user:
        print(f"   User: {user['first_name']} {user['last_name']} ({user['email']})")
    
    print("\n=== 4. GET USER WITH JOIN ===")
    user_prefs = app.get_user_with_preferences('user123')
    if user_prefs:
        print(f"   Theme: {user_prefs.get('theme')}, Notifications: {user_prefs.get('notifications')}")
    
    print("\n=== 5. QUERY ACTIVITY (with date range) ===")
    activities = app.get_user_activity('user123', start_time='2023-10-31')
    for activity in activities:
        print(f"   - {activity['login_time']}: {activity['login_count']} logins")
    
    print("\n=== 6. UPDATE LOGIN COUNT (atomic) ===")
    if activities:
        app.update_login_count('user123', activities[0]['activity_id'], increment=2)
    
    print("\n=== 7. UPDATE PREFERENCES (dynamic) ===")
    updated = app.update_preferences('user456', theme='purple', notifications=True)
    if updated:
        print(f"   New theme: {updated.get('theme')}")
    
    print("\n=== 8. SEARCH BY EMAIL PATTERN ===")
    users = app.search_users_by_email('example.com')
    print(f"   Found {len(users)} users with 'example.com'")
    
    print("\n=== 9. ACTIVITY SUMMARY (aggregation) ===")
    summary = app.get_activity_summary()
    for row in summary:
        print(f"   - {row['first_name']}: {row['total_logins']} total logins")
    
    print("\n=== 10. TRANSACTION (multi-table insert) ===")
    try:
        app.create_user_with_preferences(
            'user888', 'user888@example.com', 'Charlie', 'Brown',
            theme='dark', notifications=False
        )
    except Exception:
        pass
    
    print("\n=== 11. BATCH GET ===")
    batch_users = app.batch_get_users(['user123', 'user456', 'user789'])
    print(f"   Retrieved {len(batch_users)} users")
    
    print("\n=== 12. DELETE USER (with cascade) ===")
    app.delete_user('user999')


if __name__ == '__main__':
    demo()
