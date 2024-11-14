import mysql.connector
from pymongo import MongoClient

# Configuration for MySQL and MongoDB
MYSQL_CONFIG = {
    'host': 'your_mysql_host',
    'user': 'your_mysql_user',
    'password': 'your_mysql_password',
    'database': 'your_mysql_database'
}

MONGODB_URI = 'your_mongodb_uri'
MONGODB_DATABASE = 'your_mongodb_database'

# Connect to MySQL
def connect_mysql():
    return mysql.connector.connect(**MYSQL_CONFIG)

# Connect to MongoDB
def connect_mongodb():
    client = MongoClient(MONGODB_URI)
    return client[MONGODB_DATABASE]

# Template-based SQL query generation
def generate_sql_query(template, table, A, B, condition=None):
    if condition:
        return template.format(table=table, A=A, B=B, condition=condition)
    return template.format(table=table, A=A, B=B)

# Template-based MongoDB query generation
def generate_mongodb_query(A, B):
    return [{'$group': {'_id': f"${B}", 'total': {'$sum': f"${A}"}}}]

# SQL Query Execution
def execute_sql_query(query):
    conn = connect_mysql()
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return results

# MongoDB Query Execution
def execute_mongodb_query(collection, pipeline):
    db = connect_mongodb()
    results = list(db[collection].aggregate(pipeline))
    return results

# Sample SQL Query Templates
def get_sample_sql_queries():
    templates = [
        "SELECT SUM({A}) FROM {table} GROUP BY {B};",
        "SELECT {B}, COUNT({A}) FROM {table} WHERE {condition} GROUP BY {B};",
        "SELECT {A}, {B} FROM {table} ORDER BY {A} DESC LIMIT 10;"
    ]
    # Example field names, adjust as necessary for your dataset
    table = "sales"
    A = "sales_amount"
    B = "product_category"
    condition = "sales_amount > 100"
    return [generate_sql_query(template, table, A, B, condition) for template in templates]

# Sample MongoDB Query Templates
def get_sample_mongodb_queries():
    # Example field names, adjust as necessary for your dataset
    A = "sales_amount"
    B = "product_category"
    return [
        generate_mongodb_query(A, B),
        generate_mongodb_query("quantity", "region")
    ]

# Generate descriptive natural language for queries
def describe_query(template, A, B):
    if "GROUP BY" in template:
        return f"Total {A} by {B}"
    elif "ORDER BY" in template:
        return f"Top {A} and {B} ordered by {A}"
    return "Unknown description"

# User interaction loop
def main():
    while True:
        print("\nWelcome to ChatDB!")
        print("1. Connect to SQL Database (MySQL)")
        print("2. Connect to NoSQL Database (MongoDB)")
        print("3. Get sample SQL queries")
        print("4. Get sample MongoDB queries")
        print("5. Exit")
        choice = input("Select an option: ")

        if choice == '1':
            try:
                conn = connect_mysql()
                if conn.is_connected():
                    print("Connected to MySQL!")
                    conn.close()
            except Exception as e:
                print(f"Error connecting to MySQL: {e}")

        elif choice == '2':
            try:
                db = connect_mongodb()
                print(f"Connected to MongoDB database: {MONGODB_DATABASE}")
            except Exception as e:
                print(f"Error connecting to MongoDB: {e}")

        elif choice == '3':
            print("\nSample SQL Queries:")
            sample_queries = get_sample_sql_queries()
            for query in sample_queries:
                description = describe_query(query, "sales_amount", "product_category")
                print(f"Description: {description}")
                print(f"Query: {query}")
                if input("Execute this query? (y/n): ").lower() == 'y':
                    results = execute_sql_query(query)
                    print("Results:", results)

        elif choice == '4':
            print("\nSample MongoDB Queries:")
            sample_queries = get_sample_mongodb_queries()
            for pipeline in sample_queries:
                print(f"Query Pipeline: {pipeline}")
                if input("Execute this query? (y/n): ").lower() == 'y':
                    results = execute_mongodb_query("sales", pipeline)
                    print("Results:", results)

        elif choice == '5':
            print("Exiting ChatDB.")
            break

        else:
            print("Invalid option. Please try again.")

# Run ChatDB
if __name__ == "__main__":
    main()

