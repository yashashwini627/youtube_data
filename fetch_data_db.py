
import mysql.connector

# MySQL Configuration
DB_HOST = 'localhost'
DB_USER = 'root'  # Change this to your MySQL username
DB_PASSWORD = 'Yvss@123'  # Change this to your MySQL password
DB_NAME = 'youtube_videos'  # Database name

# Fetch and print table names
def fetch_tables():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()

        # Fetch all tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        print(f"Tables in database `{DB_NAME}`:")
        for table in tables:
            print(f"Table'{table[0]}':")  # Print table name

        conn.close()
    except mysql.connector.Error as err:
        print(f"Error: {err}")

# Fetch and print data from a specific table
def fetch_table_data(table_name):
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()

        # Fetch all data from the table
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()

        print(f"\nData from `{table_name}`:")
        for row in rows:
            print(row)

        conn.close()
    except mysql.connector.Error as err:
        print(f"Error: {err}")

# === Main Program ===
if __name__ == "__main__":
    # Fetch all tables in the database
    fetch_tables()

    # Fetch data from a specific table (for example: 'final_videos')
    fetch_table_data('final_videos')  # Change to any table name you want to fetch
