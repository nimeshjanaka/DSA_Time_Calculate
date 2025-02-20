import mysql.connector
import time

# Database connection
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="956800544xV@",
    database="testing"
)
# queries execute
cursor = db.cursor() 

# delete existing table for clear and reset database
cursor.execute("DROP TABLE IF EXISTS numbers_no_index")
cursor.execute("DROP TABLE IF EXISTS numbers_with_index")

# Create Tables
cursor.execute("""
    CREATE TABLE numbers_no_index (
        id INT PRIMARY KEY AUTO_INCREMENT,
        value INT
    )
""")

cursor.execute("""
    CREATE TABLE numbers_with_index (
        id INT PRIMARY KEY AUTO_INCREMENT,
        value INT,
        INDEX (value)  
    )
""")

db.commit()

# Record insert
def insert_numbers(table_name):
    start_time = time.time()
    for i in range(1, 10001):
        cursor.execute(f"INSERT INTO {table_name} (value) VALUES ({i})")
    db.commit()

#  Measure Retrieval Speed
def fetch_numbers(table_name):
    start_time = time.time()
    cursor.execute(f"SELECT * FROM {table_name}")
    data = cursor.fetchall()
    end_time = time.time()
    return end_time - start_time

time_fetch_no_index = fetch_numbers("numbers_no_index")
time_fetch_with_index = fetch_numbers("numbers_with_index")

print(f"Retrieval Time Without Index: {time_fetch_no_index:.4f} seconds")
print(f"Retrieval Time With Index: {time_fetch_with_index:.4f} seconds")

# Close the connection
cursor.close()
db.close()