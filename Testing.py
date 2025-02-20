import mysql.connector
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
from datetime import datetime

def connect_database(host, user, password, database):
    """
    Establish a connection to the MySQL database.
    """
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

# Function to create a test table and insert data
def create_test_table(conn, table_name="performance_test"):
    """
    Create a test table and populate it with 1 million random records.
    """
    try:
        cursor = conn.cursor()

        # Drop table if it exists
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Create table
        create_table_query = f"""
        CREATE TABLE {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT,
            username VARCHAR(50),
            email VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_query)

        # Generate 1 million records
        print("Generating 1 million records...")
        for i in tqdm(range(1000000)):
            insert_query = f"""
            INSERT INTO {table_name} (user_id, username, email, created_at)
            VALUES (
                {np.random.randint(1, 1000000)},
                'user_{i}',
                'user_{i}@example.com',
                '{datetime.now()}'
            )
            """
            cursor.execute(insert_query)

        conn.commit()
        print("Test table created successfully")
    except Exception as e:
        print(f"Error creating test table: {e}")
        conn.rollback()

# Function to run the performance test
def run_performance_test(conn, table_name="performance_test", num_iterations=100):
    """
    Run performance tests with and without an index on the user_id column.
    """
    cursor = conn.cursor()
    results = {"no_index": [], "with_index": []}

    # Test without index
    print("\nTesting queries without index...")
    for i in tqdm(range(num_iterations)):
        random_user_id = np.random.randint(1, 1000000)

        start_time = time.time()
        cursor.execute(f"SELECT * FROM {table_name} WHERE user_id = {random_user_id}")
        cursor.fetchall()
        end_time = time.time()

        results["no_index"].append(end_time - start_time)

    # Create index
    print("\nCreating index on user_id...")
    cursor.execute(f"CREATE INDEX idx_user_id ON {table_name}(user_id)")
    conn.commit()
    
    # Test with index
    print("\nTesting queries with index...")
    for i in tqdm(range(num_iterations)):
        random_user_id = np.random.randint(1, 1000000)

        start_time = time.time()
        cursor.execute(f"SELECT * FROM {table_name} WHERE user_id = {random_user_id}")
        cursor.fetchall()
        end_time = time.time()

        results["with_index"].append(end_time - start_time)

    return results

# Function to visualize performance results
def visualize_results(results):
    """
    Create visualizations of the performance results.
    """
    # Convert results to DataFrame
    df = pd.DataFrame({
        "No Index": results["no_index"],
        "With Index": results["with_index"]
    })

    # Add test run number
    df["Test Run"] = range(1, len(df) + 1)

    # Calculate statistics
    stats = df.describe()
    print(stats)

    # Create Box Plot
    plt.figure(figsize=(10, 6))
    df[["No Index", "With Index"]].boxplot()
    plt.title("Query Performance: With vs Without Index")
    plt.ylabel("Time (seconds)")
    plt.grid(True)
    plt.show()

    # Create Line Plot with Trend Lines
    plt.figure(figsize=(12, 6))
    plt.plot(df["Test Run"], df["No Index"], label="No Index", linestyle='-', marker='o', alpha=0.7)
    plt.plot(df["Test Run"], df["With Index"], label="With Index", linestyle='-', marker='o', alpha=0.7)

    # Adding trend lines (linear regression)
    z_no_index = np.polyfit(df["Test Run"], df["No Index"], 1)
    p_no_index = np.poly1d(z_no_index)
    plt.plot(df["Test Run"], p_no_index(df["Test Run"]), linestyle='--', color='blue', alpha=0.5)

    z_with_index = np.polyfit(df["Test Run"], df["With Index"], 1)
    p_with_index = np.poly1d(z_with_index)
    plt.plot(df["Test Run"], p_with_index(df["Test Run"]), linestyle='--', color='orange', alpha=0.5)

    plt.xlabel("Test Run")
    plt.ylabel("Query Time (seconds)")
    plt.title("Query Execution Time Over Test Runs")
    plt.legend()
    plt.grid(True)
    plt.show()

    # Create Histogram
    plt.figure(figsize=(10, 6))
    plt.hist(df["No Index"], bins=20, alpha=0.7, label="No Index")
    plt.hist(df["With Index"], bins=20, alpha=0.7, label="With Index")
    plt.xlabel("Query Time (seconds)")
    plt.ylabel("Frequency")
    plt.title("Distribution of Query Execution Times")
    plt.legend()
    plt.grid(True)
    plt.show()

    # Performance Metrics
    mean_no_index = df["No Index"].mean()
    mean_with_index = df["With Index"].mean()
    performance_gain = ((mean_no_index - mean_with_index) / mean_no_index) * 100

    # Line Plot with Performance Summary
    plt.figure(figsize=(12, 6))
    plt.plot(df["Test Run"], df["No Index"], label="No Index", linestyle='-', marker='o', alpha=0.7)
    plt.plot(df["Test Run"], df["With Index"], label="With Index", linestyle='-', marker='o', alpha=0.7)

    # Add horizontal line for averages
    plt.axhline(mean_no_index, color='blue', linestyle='--', label=f"Avg No Index: {mean_no_index:.3f}s")
    plt.axhline(mean_with_index, color='orange', linestyle='--', label=f"Avg With Index: {mean_with_index:.3f}s")

    # Add text annotations
    plt.text(len(df) * 0.8, mean_no_index + 0.005, f"Avg No Index: {mean_no_index:.3f}s", color='blue')
    plt.text(len(df) * 0.8, mean_with_index - 0.005, f"Avg With Index: {mean_with_index:.3f}s", color='orange')
    plt.text(len(df) * 0.2, max(df["No Index"]) * 0.8, f"Performance Gain: {performance_gain:.2f}%", fontsize=12, color='red')

    plt.xlabel("Test Run")
    plt.ylabel("Query Time (seconds)")
    plt.title("Query Execution Time Over Test Runs with Performance Summary")
    plt.legend()
    plt.grid(True)
    plt.show()

def main():
    # Database connection parameters
    host = "localhost"
    user = "root"
    password = "956800544xV@"
    database = "test_DB"

    # Connect to the database
    conn = connect_database(host, user, password, database)

    if conn:
        # Create test table and populate it with data
        create_test_table(conn)

        # Run performance test
        results = run_performance_test(conn)

        # Visualize results
        visualize_results(results)

        # Close connection
        conn.close()
    else:
        print("Failed to connect to the database.")

if __name__ == "__main__":
    main()
