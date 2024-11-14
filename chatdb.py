import os
import mysql.connector
import pandas as pd
from pymongo import MongoClient
import random
from sqlalchemy import create_engine

# Clear screen function
def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

# Configuration for MySQL and MongoDB
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password',
    'database': 'chatdbsql'
}
MONGODB_URI = 'mongodb://localhost:27017'
MONGODB_DATABASE = 'chatdbmongo'

# Initialize SQLAlchemy engine for MySQL
def get_sqlalchemy_engine():
    return create_engine(
        f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}/{MYSQL_CONFIG['database']}"
    )

# Connect to MySQL
def connect_mysql():
    return mysql.connector.connect(**MYSQL_CONFIG)

# Connect to MongoDB
def connect_mongo():
    client = MongoClient(MONGODB_URI)
    return client[MONGODB_DATABASE]

# Upload CSV to both MySQL and MongoDB
def upload_csv(file_path, table_name, database_type):
    df = pd.read_csv(file_path)
    if database_type == 'sql':
        engine = get_sqlalchemy_engine()
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Uploaded data from {file_path} into MySQL table '{table_name}'.")
        get_columns_and_sample_data_sql(table_name)
    elif database_type == 'mongo':
        db = connect_mongo()
        db[table_name].insert_many(df.to_dict("records"))
        print(f"Uploaded data from {file_path} into MongoDB collection '{table_name}'.")
        get_columns_and_sample_data_mongo(table_name)

# Fetch column information and sample data from MySQL table
def get_columns_and_sample_data_sql(table_name):
    conn = connect_mysql()
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE `{table_name}`")
    columns = cursor.fetchall()
    cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 5")
    sample_data = cursor.fetchall()
    conn.close()
    return columns, sample_data

# Fetch sample data from MongoDB collection
def get_columns_and_sample_data_mongo(collection_name):
    db = connect_mongo()
    collection = db[collection_name]
    sample_data = list(collection.find().limit(5))
    
    if sample_data:
        columns = [(key, type(value).__name__) for key, value in sample_data[0].items()]
    else:
        columns = []
        
    return columns, sample_data

# Generate diverse SQL Query Patterns, avoiding SELECT *
def generate_sql_queries(table_name, num_queries=5):
    columns, sample_data = get_columns_and_sample_data_sql(table_name)
    column_names = [f"`{col[0]}`" for col in columns]
    quantitative_columns = [f"`{col[0]}`" for col in columns if col[1] in ('int', 'bigint', 'float', 'double')]
    categorical_columns = [f"`{col[0]}`" for col in columns if col[1] in ('varchar', 'text')]

    # Use a sample value for 'Platform' filter if sample data is available
    sample_platform = sample_data[0][column_names.index("`Platform`")].strip("'") if sample_data else 'SamplePlatform'

    query_patterns = [
        ("Group by <A>", "Group by a column", f"SELECT <A>, COUNT(*) FROM `{table_name}` GROUP BY <A>;"),
        ("Sum of <A> by <B>", "Calculate the sum of <A> grouped by <B>", f"SELECT <B>, SUM(<A>) FROM `{table_name}` GROUP BY <B>;"),
        ("Average <A> by <B>", "Calculate the average <A> for each <B>", f"SELECT <B>, AVG(<A>) FROM `{table_name}` GROUP BY <B>;"),
        ("Max <A> for each <B>", "Find max <A> value grouped by <B>", f"SELECT <B>, MAX(<A>) FROM `{table_name}` GROUP BY <B>;"),
        ("Order by <A>", "Select records ordered by <A>", f"SELECT {', '.join(column_names)} FROM `{table_name}` ORDER BY <A> DESC;"),
        ("Filter by `Platform`", "Select records where `Platform` matches a sample value", f"SELECT {', '.join(column_names)} FROM `{table_name}` WHERE `Platform` = '{sample_platform}';"),
        ("Group by and Having <A>", "Group by column <A> with a HAVING clause", f"SELECT <A>, COUNT(*) FROM `{table_name}` GROUP BY <A> HAVING COUNT(*) > 1;"),
    ]

    all_queries = []
    for title, description, query_template in query_patterns:
        for A in quantitative_columns:
            for B in categorical_columns:
                query_title = title.replace("<A>", A).replace("<B>", B)
                description_filled = description.replace("<A>", A).replace("<B>", B)
                
                # Format query to avoid "SELECT *" and use specific column names
                formatted_columns = ", ".join(column_names)
                query_filled = query_template.replace("<A>", A).replace("<B>", B).format(table=table_name, columns=formatted_columns)
                
                all_queries.append((query_title, description_filled, query_filled))

    return random.sample(all_queries, min(num_queries, len(all_queries)))

# Generate MongoDB Query Patterns
def generate_mongo_queries(collection_name, num_queries=5):
    columns, _ = get_columns_and_sample_data_mongo(collection_name)
    quantitative_columns = [col[0] for col in columns if col[1] in ('int', 'float')]
    categorical_columns = [col[0] for col in columns if col[1] == 'str']

    query_patterns = [
        ("Group by <B>", "Calculate the count of documents by <B>", [{"$group": {"_id": "$<B>", "count": {"$sum": 1}}}]),
        ("Sum <A> by <B>", "Calculate the sum of <A> grouped by <B>", [{"$group": {"_id": "$<B>", "sum_<A>": {"$sum": "$<A>"}}}]),
        ("Average <A> by <B>", "Calculate the average <A> for each <B>", [{"$group": {"_id": "$<B>", "avg_<A>": {"$avg": "$<A>"}}}]),
        ("Match <B>", "Filter documents where <B> equals 'value'", [{"$match": {"<B>": "value"}}]),
    ]

    all_queries = []
    for title, description, query_template in query_patterns:
        for A in quantitative_columns:
            for B in categorical_columns:
                query_title = title.replace("<A>", A).replace("<B>", B)
                description_filled = description.replace("<A>", A).replace("<B>", B)
                query_filled = [{k.replace("<A>", A).replace("<B>", B): v for k, v in step.items()} for step in query_template]
                all_queries.append((query_title, description_filled, query_filled))

    return random.sample(all_queries, min(num_queries, len(all_queries)))

# Display and Execute SQL or MongoDB queries
def display_and_execute_queries(queries, db_type, selected_table):
    print(f"\nSample {db_type.upper()} Queries:")
    for i, (title, description, query) in enumerate(queries, 1):
        print(f"\n{i}. {title}")
        print(f"Description: {description}")
        
        if db_type == "sql":
            print(f"Query: {query}")
            conn = connect_mysql()
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                result = cursor.fetchall()
                print("Result:")
                if len(result) > 9:
                    for row in result[:8]:
                        print(row)
                    print("...")
                    print(result[-1])
                else:
                    for row in result:
                        print(row)
            except mysql.connector.errors.ProgrammingError as e:
                print("Error executing query:", e)
            finally:
                conn.close()
        elif db_type == "mongo":
            print("MongoDB Query Pipeline:")
            for step in query:
                print(step)
            print("Sample MongoDB Result (no actual execution in this display function):")
            db = connect_mongo()
            collection = db[selected_table]
            result = list(collection.aggregate(query))
            for row in result:
                print(row)

# Commands Function
def commands():
    print("\nAvailable Commands:")
    print("1. explore databases - Explore and switch between MySQL and MongoDB databases.")
    print("2. upload dataset - Upload a CSV file as a new dataset.")
    print("3. example sql queries - Generate example SQL queries for the selected dataset.")
    print("4. example mongo queries - Generate example MongoDB queries for the selected dataset.")
    print("5. exit - Exit the program.")

# Main ChatDB CLI
def main():
    clear_screen()
    print("Welcome to ChatDB!\n")
    selected_table = None
    db_type = None  # Keep track of the selected database type

    # Show available databases on startup
    print("Available databases:\n")
    print("SQL Database:", MYSQL_CONFIG['database'])
    mysql_tables = list_mysql_tables()
    print("Tables:", ', '.join(mysql_tables) if mysql_tables else "No tables found")
    
    print("\nMongoDB Database:", MONGODB_DATABASE)
    mongo_collections = connect_mongo().list_collection_names()
    print("Collections:", ', '.join(mongo_collections) if mongo_collections else "No collections found")
    
    print("\nAlternatively, type 'upload dataset' to upload a new one.\n")

    # Choose a dataset or upload a new one
    while selected_table is None:
        choice = input("ChatDB: ").strip().lower()
        
        if choice == "upload dataset":
            db_choice = input("Which database to upload to (sql/mongo): ").strip().lower()
            file_path = input("Enter the path to the CSV file: ").strip()
            table_or_collection_name = input("Enter the name for the new dataset: ").strip()
            if db_choice == "sql":
                upload_csv(file_path, table_or_collection_name, 'sql')
                db_type = 'sql'
            elif db_choice == "mongo":
                upload_csv(file_path, table_or_collection_name, 'mongo')
                db_type = 'mongo'
            else:
                print("Invalid database choice. Choose either 'sql' or 'mongo'.")
                continue
            selected_table = table_or_collection_name
            print(f"Using dataset: {selected_table}")

        elif choice in mysql_tables:
            selected_table = choice
            db_type = 'sql'
            print(f"Using dataset: {selected_table}")
            columns, sample_data = get_columns_and_sample_data_sql(selected_table)
            print("\nSample data:")
            print(', '.join([col[0] for col in columns]))  # Print column names in one line
            for row in sample_data:
                print(row)

        elif choice in mongo_collections:
            selected_table = choice
            db_type = 'mongo'
            print(f"Using dataset: {selected_table}")
            columns, sample_data = get_columns_and_sample_data_mongo(selected_table)
            print("\nSample data:")
            print(', '.join([col[0] for col in columns]))  # Print column names in one line
            for row in sample_data:
                print(row)

        else:
            print("Invalid choice. Please select an existing dataset or upload a new one.")

    # Main loop for selected dataset
    while True:
        commands()
        prompt = f"ChatDB[{db_type.capitalize()}->{selected_table}]: "
        command = input(prompt).strip().lower()
        
        if command == "explore databases":
            clear_screen()
            print("Available databases:\n")
            print("SQL Database:", MYSQL_CONFIG['database'])
            mysql_tables = list_mysql_tables()
            print("Tables:", ', '.join(mysql_tables) if mysql_tables else "No tables found")
            
            print("\nMongoDB Database:", MONGODB_DATABASE)
            mongo_collections = connect_mongo().list_collection_names()
            print("Collections:", ', '.join(mongo_collections) if mongo_collections else "No collections found")
            
            selected_table = input("\nEnter the name of the table or collection to use: ").strip()
            if selected_table in mysql_tables:
                db_type = 'sql'
                print(f"Using dataset: {selected_table}")
                columns, sample_data = get_columns_and_sample_data_sql(selected_table)
                print("\nSample data:")
                print(', '.join([col[0] for col in columns]))  # Print column names in one line
                for row in sample_data:
                    print(row)
            elif selected_table in mongo_collections:
                db_type = 'mongo'
                print(f"Using dataset: {selected_table}")
                columns, sample_data = get_columns_and_sample_data_mongo(selected_table)
                print("\nSample data:")
                print(', '.join([col[0] for col in columns]))  # Print column names in one line
                for row in sample_data:
                    print(row)
            else:
                print("Invalid choice. Please select a valid table or collection.")
                selected_table = None

        elif command == "upload dataset":
            db_choice = input("Which database to upload to (sql/mongo): ").strip().lower()
            file_path = input("Enter the path to the CSV file: ").strip()
            table_or_collection_name = input("Enter the name for the new dataset: ").strip()
            if db_choice == "sql":
                upload_csv(file_path, table_or_collection_name, 'sql')
                db_type = 'sql'
            elif db_choice == "mongo":
                upload_csv(file_path, table_or_collection_name, 'mongo')
                db_type = 'mongo'
            else:
                print("Invalid database choice. Choose either 'sql' or 'mongo'.")
                continue
            selected_table = table_or_collection_name
            print(f"Using dataset: {selected_table}")

        elif command == "example sql queries" and db_type == "sql" and selected_table:
            display_and_execute_queries(generate_sql_queries(selected_table), "sql", selected_table)

        elif command == "example mongo queries" and db_type == "mongo" and selected_table:
            display_and_execute_queries(generate_mongo_queries(selected_table), "mongo", selected_table)

        elif command == "exit":
            break

        else:
            print("Unknown command. Please try again.")

# Helper functions
def list_mysql_tables():
    conn = connect_mysql()
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES;")
    tables = [table[0] for table in cursor.fetchall()]
    conn.close()
    return tables

if __name__ == "__main__":
    main()
