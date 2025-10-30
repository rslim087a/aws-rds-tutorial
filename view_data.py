from rds_app import RDSApp

app = RDSApp()

with app.get_connection() as conn:
    cursor = conn.cursor()
    
    print("\n=== USERS ===")
    cursor.execute("SELECT * FROM users")
    for row in cursor.fetchall():
        print(row)
    
    print("\n=== ACTIVITY ===")
    cursor.execute("SELECT * FROM activity")
    for row in cursor.fetchall():
        print(row)
    
    print("\n=== PREFERENCES ===")
    cursor.execute("SELECT * FROM preferences")
    for row in cursor.fetchall():
        print(row)