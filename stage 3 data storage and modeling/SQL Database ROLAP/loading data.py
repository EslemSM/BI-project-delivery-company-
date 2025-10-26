import pandas as pd
import mysql.connector

# Database connection setup
def create_connection(database=None):
    if database:
        return mysql.connector.connect(
            host="localhost",
            user="root",  # Replace with your username
            password="islemsmiila*332-W",  # Replace with your password
            database=database
        )
    else:
        return mysql.connector.connect(
            host="localhost",
            user="root",  # Replace with your username
            password="islemsmiila*332-W"  # Replace with your password
        )

# Create database and tables
def setup_database():
    conn = create_connection()  # Connect without specifying a database
    cursor = conn.cursor()

    # Create database if it doesn't exist
    cursor.execute("CREATE DATABASE IF NOT EXISTS deliveryexpress")
    conn.commit()  # Commit the creation of the database

    # Now connect to the specific database
    conn = create_connection("deliveryexpress")
    cursor = conn.cursor()

    # Create tables
    tables = {
        "Customers": """
            CREATE TABLE IF NOT EXISTS Customers (
                customer_id INT PRIMARY KEY,
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                email VARCHAR(255),
                phone_number VARCHAR(20),
                address VARCHAR(255),
                postal_code VARCHAR(20),
                state VARCHAR(50)
            )
        """,
        "Payments": """
            CREATE TABLE IF NOT EXISTS Payments (
                payment_id INT PRIMARY KEY,
                payment_amount DECIMAL(10, 2),
                payment_method VARCHAR(50)
            )
        """,
        "Drivers": """
            CREATE TABLE IF NOT EXISTS Drivers (
                driver_id INT PRIMARY KEY,
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                email VARCHAR(255),
                phone_number VARCHAR(20)
            )
        """,
        "Shipments": """
            CREATE TABLE IF NOT EXISTS Shipments (
                shipment_id INT PRIMARY KEY,
                item_category VARCHAR(50),
                item_name VARCHAR(255),
                item_price DECIMAL(10, 2)
            )
        """,
        "Deliveries": """
            CREATE TABLE IF NOT EXISTS Deliveries (
                delivery_id INT PRIMARY KEY,
                delivery_address VARCHAR(255),
                pickup_address VARCHAR(255),
                delivery_cost DECIMAL(10, 2)
            )
        """,
        "fact_table": """
            CREATE TABLE IF NOT EXISTS fact_table (
                fact_id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id INT DEFAULT 0,
                payment_id INT DEFAULT 0,
                driver_id INT DEFAULT 0,
                shipment_id INT DEFAULT 0,
                delivery_id INT DEFAULT 0,
                Sales DECIMAL(10, 2) DEFAULT 0,
                FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
                FOREIGN KEY (payment_id) REFERENCES Payments(payment_id),
                FOREIGN KEY (driver_id) REFERENCES Drivers(driver_id),
                FOREIGN KEY (shipment_id) REFERENCES Shipments(shipment_id),
                FOREIGN KEY (delivery_id) REFERENCES Deliveries(delivery_id)
            )
        """
    }

    for table_name, table_query in tables.items():
        cursor.execute(table_query)

    conn.commit()
    conn.close()

# Insert data from Excel to MySQL
def insert_data_from_excel(file_path):
    conn = create_connection("deliveryexpress")  # Connect to the 'deliveryexpress' database
    cursor = conn.cursor()

    # Read each sheet and insert data
    sheets = ["Customers", "Payments", "Drivers", "Shipments", "Deliveries"]
    for sheet in sheets:
        df = pd.read_excel(file_path, sheet_name=sheet)

        # Filter columns that match table structure
        cursor.execute(f"DESCRIBE {sheet}")
        table_columns = [row[0] for row in cursor.fetchall()]
        df = df[[col for col in df.columns if col in table_columns]]

        # Insert data into the table
        for _, row in df.iterrows():
            placeholders = ", ".join(["%s"] * len(row))
            columns = ", ".join(row.index)
            sql = f"INSERT INTO {sheet} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(row))

    conn.commit()
    conn.close()

# Populate fact table
def populate_fact_table():
    conn = create_connection("deliveryexpress")  # Connect to the 'deliveryexpress' database
    cursor = conn.cursor()

    # Get the maximum number of rows across dimensions
    cursor.execute("SELECT COUNT(*) FROM Customers")
    customer_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM Payments")
    payment_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM Drivers")
    driver_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM Shipments")
    shipment_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM Deliveries")
    delivery_count = cursor.fetchone()[0]

    max_rows = max(customer_count, payment_count, driver_count, shipment_count, delivery_count)

    # Insert rows into fact table
    for i in range(max_rows):
        cursor.execute("""
            INSERT INTO fact_table (customer_id, payment_id, driver_id, shipment_id, delivery_id, Sales)
            VALUES (
                IFNULL((SELECT customer_id FROM Customers LIMIT %s, 1), 0),
                IFNULL((SELECT payment_id FROM Payments LIMIT %s, 1), 0),
                IFNULL((SELECT driver_id FROM Drivers LIMIT %s, 1), 0),
                IFNULL((SELECT shipment_id FROM Shipments LIMIT %s, 1), 0),
                IFNULL((SELECT delivery_id FROM Deliveries LIMIT %s, 1), 0),
                IFNULL((SELECT delivery_cost FROM Deliveries LIMIT %s, 1), 0)
            )
        """, (i, i, i, i, i, i))

    conn.commit()
    conn.close()

# Main function
def main():
    file_path = "C:\\Users\\dell\\Desktop\\data cleaning\\etl data\\cleaned_data.xlsx"  

    setup_database()
    insert_data_from_excel(file_path)
    populate_fact_table()

if __name__ == "__main__":
    main()
