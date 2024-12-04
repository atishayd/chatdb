import random
import mysql.connector
import pandas as pd
from pymongo import MongoClient
from typing import Dict

# pre configured mysql connection
# NOTE: if you want to use your own database, change these or the .env file
# on the github repo
# parameters on line 922s

MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password',
    'database': 'chatdbsql'
}

# connecting to default mongodb database and port
MONGODB_DATABASE = 'chatdbmongo'

# initializing the ChatDB class itself
# we want the current db type and dataset to always be shown during prompting
# to ensure we can easily track exactly what database/dataset we are currently using
class ChatDB:
    def __init__(self):
        # helps generate the ChatDB[db_type->dataset]: prompt prefix
        self.current_db_type = None
        self.current_dataset = None
        self.commands = {
            'commands': 'Show this menu',
            'switch database': 'Switch Database',
            'upload dataset': 'Upload Dataset',
            'explore database': 'Show Sample Data',
            'generate queries': 'Generate Sample Queries',
            'exit': 'Exit Program'
        }

    # getter methods

    # list the current commands with their given descriptions
    def get_commands(self):
        print("\nAvailable Commands:")
        for cmd, desc in self.commands.items():
            print(f"{cmd} - {desc}")

    # prefix creator
    def get_prompt(self) -> str:
        if not self.current_db_type or not self.current_dataset:
            return "ChatDB:"
        return f"ChatDB[{self.current_db_type.upper()}->{self.current_dataset}]"
    
    # source: https://dev.mysql.com/doc/connector-python/en/connector-python-example-connecting.html
    def connect_mysql(self):
        return mysql.connector.connect(**MYSQL_CONFIG)

        
    # source: https://www.w3schools.com/python/python_mongodb_create_collection.asp
    def connect_mongo(self):
        client = MongoClient("mongodb://localhost:27017")
        return client[MONGODB_DATABASE]


    def get_databases(self):
        # databases is a dictionary that stores the database information
        databases = {'sql': [], 'mongo': []}

        # MYSQL connection section using mysql_connector
        cnx = self.connect_mysql()
        # Check if the connection is valid
        if cnx is not None:
            cursor = cnx.cursor()
            # Ensure cursor object is valid
            if cursor is not None:
                # SQL show tables equivalent 
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                if tables:
                # extract table names using a loop and append to the 'sql' list
                    for table in tables:
                        databases['sql'].append(table[0])
            cnx.close()
    
        # Get MongoDB collections
        db = self.connect_mongo()
        if db is not None:  # Check if the MongoDB connection is valid
            collections = db.list_collection_names()
            if collections:  # Check if any collections are returned
                databases['mongo'] = collections
                
        return databases

    def display_available_databases(self):
        databases = self.get_databases()
        
        print("\nAvailable Databases")
        print(f"\nMySQL Database: {MYSQL_CONFIG['database']}")
        print("Available datasets:")
        if databases['sql']:
            for table in databases['sql']:
                print(f"  - {table}")
        else:
            print("No datasets available")

        print(f"\nMongoDB Database: {MONGODB_DATABASE}")
        print("Available collections:")
        if databases['mongo']:
            for collection in databases['mongo']:
                print(f"  - {collection}")
        else:
            print("No collections available")

        print("\nPlease enter a name of a table/collection, or alternatively upload a new dataset using the command 'upload dataset'.")

    def find_dataset_type(self, dataset_name):
        """Find if dataset exists and its type"""
        databases = self.get_databases()
        
        if dataset_name in databases['sql']:
            return True, 'sql'
        elif dataset_name in databases['mongo']:
            return True, 'mongo'
        
        return False, None
    
    # setter functions

    def set_current_dataset(self, dataset_name):
        """Set the current dataset and its type"""
        exists, db_type = self.find_dataset_type(dataset_name)
        if exists:
            self.current_db_type = db_type
            self.current_dataset = dataset_name
            print("\nUse 'commands' to see the list of available commands")
            return True
        return False

    def process_command(self, command):
        # convert the command to lowercase and remove any leading or trailing whitespace
        command = command.lower().strip()
        
        # dictionary mapping sql related command phrases to their respective identifiers
        # the key is a user-friendly description, and the value is a tuple with 
        # the query type and the specific SQL operation
        sql_query_commands = {
            'example sql queries': ('sql', None),
            'example query with group by': ('sql', 'group_by'),
            'example query with having': ('sql', 'having'),
            'example query with where': ('sql', 'where'),
            'example query with order by': ('sql', 'order_by'),
            'example query with aggregation': ('sql', 'aggregation'),
            'example sum query': ('sql', 'sum'),
            'example count query': ('sql', 'count'),
            'example avg query': ('sql', 'avg')
        }
        
        # dictionary mapping mongodb related command phrases to their respective identifiers
        # the key is a user-friendly description, and the value is a tuple with 
        # the query type and the specific mongodb operation
        mongo_query_commands = {
            'example find functions': ('mongo', 'find'),
            'example aggregate function': ('mongo', 'aggregate'),
            'example find function with query criteria': ('mongo', 'find_criteria'),
            'example find function with projection': ('mongo', 'projection'),
            'example aggregate with group': ('mongo', 'group'),
            'example aggregate with match': ('mongo', 'match')
        }
        
        # if given command exists in the sql commands dictionary
        # return the corresponding tuple for SQL operation
        if command in sql_query_commands:
            return sql_query_commands[command]
            
        # if given command exists in the mongodb commands dictionary
        # return the corresponding tuple for mongodb operation
        if command in mongo_query_commands:
            return mongo_query_commands[command]
            
        # if command doesn't match any sql or mongo commands, return default values
        return None, None


    def upload_csv(self, file_path, dataset_name, database_type):
        try:
            # Read the CSV file at the given file path into a Pandas DataFrame
            df = pd.read_csv(file_path)

            # Check the target database type and call the respective upload function
            if database_type == 'sql':
                # Upload the DataFrame to an SQL database
                self.upload_to_sql(df, dataset_name)
            elif database_type == 'mongo':
                # Upload the DataFrame to a MongoDB collection
                self.upload_to_mongo(df, dataset_name)

            # Print a confirmation message with details about the upload
            print(f"\nSuccessfully uploaded {file_path} to {database_type} database as {dataset_name}")

            # Update the reference to the current dataset being worked on
            self.current_dataset = dataset_name

            # Display a sample of the uploaded data for verification
            self.show_sample_data(dataset_name, database_type)
        except Exception as e:
            # Catch and log any errors that occur during the upload process
            print(f"Error uploading data: {e}")

    # implementation inspired from:
    # https://medium.com/@affanhamid007/how-to-convert-csv-to-sql-database-using-python-and-sqlite3-b693d687c04a

    def upload_to_sql(self, df, table_name):
        cnx = self.connect_mysql()
        cursor = cnx.cursor()

        # Generate a SQL CREATE TABLE statement based on the DataFrame's structure
        create_table_stmt = self.generate_create_table_stmt(df, table_name)

        # Drop the table if it already exists to avoid conflicts with new data
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Execute the CREATE TABLE statement to create the new table
        cursor.execute(create_table_stmt)

        # Iterate over each row in the DataFrame to insert data into the table
        for _, row in df.iterrows():
            # Create placeholders for parameterized queries based on the number of columns
            placeholders = ','.join(['%s'] * len(row))

            # Execute an INSERT statement with values from the current row
            cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", tuple(row))

        cnx.commit()
        cnx.close()

    def upload_to_mongo(self, df, collection_name):
        db = self.connect_mongo()
        # Drop the collection if it exists to avoid duplicate data
        db[collection_name].drop()
        # Convert the DataFrame to a list of dictionaries and insert them into the collection
        db[collection_name].insert_many(df.to_dict("records"))

    def generate_create_table_stmt(self, df, table_name):
        dtype_map = {
            'int64': 'INT',
            'float64': 'FLOAT',
            'object': 'VARCHAR(255)',
            'datetime64[ns]': 'DATETIME',
            'bool': 'BOOLEAN'
        }
        columns = []
        for col, dtype in df.dtypes.items():
            sql_type = dtype_map.get(str(dtype), 'VARCHAR(255)')
            columns.append(f"`{col}` {sql_type}")
        return f"CREATE TABLE {table_name} ({', '.join(columns)})"

    def show_sample_data(self, dataset_name, db_type):
        if db_type == 'sql':
            # Fetch column metadata and sample data from the SQL database
            columns, data = self.sample_sql_data(dataset_name)
            
            # Extract headers from columns
            headers = []
            for col in columns:
                headers.append(col[0])
            
            print("\nColumns:")
            # Print column names and their data types
            for col in columns:
                print(f"{col[0]} ({col[1]})")
            
            print("\nSample Data (5 rows):")
            # Print header row
            print(" | ".join(headers))
            print("-" * (len(" | ".join(headers))))
            
            # Print data rows
            for row in data:
                formatted_row = []
                for val in row:
                    formatted_row.append(str(val))
                print(" | ".join(formatted_row))
        
        else:
            # Fetch column metadata and sample data from the MongoDB database
            columns, data = self.sample_mongo_data(dataset_name)
            
            # Extract headers from columns
            headers = []
            for col in columns:
                headers.append(col[0])
            
            print("\nColumns:")
            # Print column names and their data types
            for col in columns:
                print(f"{col[0]} ({col[1]})")
            
            print("\nSample Data (5 rows):")
            # Print header row
            print(" | ".join(headers))
            print("-" * (len(" | ".join(headers))))
            
            # Print data rows
            for row in data:
                formatted_row = []
                for val in row:
                    formatted_row.append(str(val))
                print(" | ".join(formatted_row))
        
        # Prompt the user for further actions
        print("\nType 'commands' to get a list of available commands")

    def sample_sql_data(self, table_name):
        cnx = self.connect_mysql()
        cursor = cnx.cursor()
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
        sample_data = cursor.fetchall()
        cnx.close()
        return columns, sample_data

    def sample_mongo_data(self, collection_name):
        # Connect to MongoDB and access the specified collection
        db = self.connect_mongo()
        collection = db[collection_name]

        # Retrieve a sample of up to 5 documents from the collection
        sample_data = list(collection.find().limit(5))

        # If no data is found, return empty structures for columns and data
        if not sample_data:
            return [], []

        # Extract column names and types, excluding the '_id' field
        columns = []
        for key, value in sample_data[0].items():
            if key != '_id':
                columns.append((key, type(value).__name__))

        # Format sample data into tuples based on the extracted columns
        formatted_data = []
        for doc in sample_data:
            row = []
            for col in columns:
                row.append(doc[col[0]])
            formatted_data.append(tuple(row))

        # Return the extracted column metadata and formatted data
        return columns, formatted_data

    def generate_query(self, dataset_name, db_type, query_type = None):
        if db_type == 'sql':
            return self.generate_sql_queries(dataset_name, query_type)
        else:
            return self.generate_mongo_queries(dataset_name, query_type)
    
    def generate_sql_queries(self, table_name, query_type=None):
        # Retrieve column metadata and split into column name and types
        columns, _ = self.sample_sql_data(table_name)
        
        # Extract all column names from data
        cols = self.extract_column_names(columns)
        
        # Extract numeric columns (e.g., int, float)
        numeric_cols = self.extract_columns_by_type(columns, ('int', 'float', 'double', 'decimal'))
        
        # Extract text columns (e.g., varchar, text)
        text_cols = self.extract_columns_by_type(columns, ('varchar', 'text', 'char'))
        
        # Fallback to all columns if no numeric or text columns are found
        if not numeric_cols:
            numeric_cols = cols
        if not text_cols:
            text_cols = cols

        # SQL query templates and descriptions for different types
        patterns = self.get_query_patterns()
        
        # Store generated queries
        queries = []
        
        if query_type:
            # If a query type is provided, generate queries specific to that type
            if query_type in patterns:
                pattern_list = patterns[query_type]
                for pattern in pattern_list:
                    params = self.generate_query_parameters(cols, numeric_cols, text_cols, table_name)
                    
                    # Format the SQL template with dynamic parameters
                    query = pattern['template'].format(**params)
                    
                    # Format the description with the same parameters
                    description = pattern['description'].format(**params)
                    
                    # Append the generated query and its description
                    queries.append({
                        'description': description,
                        'query': query
                    })
        else:
            # For mixed queries, take one random pattern from each type
            for pattern_list in patterns.values():
                pattern = random.choice(pattern_list)
                params = self.generate_query_parameters(cols, numeric_cols, text_cols, table_name)
                
                # Format the SQL template and description
                query = pattern['template'].format(**params)
                description = pattern['description'].format(**params)
                
                # Append the generated query and its description
                queries.append({'description': description, 'query': query})

        # Ensure exactly 5 queries by duplicating if necessary
        while len(queries) < 5:
            queries.extend(queries[:5 - len(queries)])  # Add duplicates to make up the count
        
        return queries[:5]  # Return only the first 5 queries

    # extract column names
    def extract_column_names(self, columns):
        """Extracts the first element (column name) from a list of column tuples."""
        col_names = []
        for col in columns:
            col_names.append(col[0])
        return col_names

    # extract columns by data type
    def extract_columns_by_type(self, columns, data_types):
        """
        Filters columns by matching their data types against a list of types.
        Returns a list of column names.
        """
        filtered_columns = []
        for col in columns:
            if col[1].lower() in data_types:
                filtered_columns.append(col[0])
        return filtered_columns
    
    def get_query_patterns(self):
        return {
            'group_by': [
                {
                    'template': "SELECT {group_col}, COUNT(*) as count FROM {table} GROUP BY {group_col}",
                    'description': "Count of items by {group_col}"
                },
                {
                    'template': "SELECT {group_col}, SUM({numeric_col}) as total FROM {table} GROUP BY {group_col}",
                    'description': "Sum of {numeric_col} by {group_col}"
                },
                {
                    'template': "SELECT {group_col}, AVG({numeric_col}) as average FROM {table} GROUP BY {group_col}",
                    'description': "Average {numeric_col} by {group_col}"
                }
            ],
            'having': [
                {
                    'template': "SELECT {group_col}, COUNT(*) as count FROM {table} GROUP BY {group_col} HAVING count > {min_count}",
                    'description': "Groups with more than {min_count} items"
                },
                {
                    'template': "SELECT {group_col}, SUM({numeric_col}) as total FROM {table} GROUP BY {group_col} HAVING total > {threshold}",
                    'description': "Groups with total {numeric_col} exceeding {threshold}"
                }
            ],
            'order_by': [
                {
                    'template': "SELECT * FROM {table} ORDER BY {ord_col} DESC LIMIT {limit}",
                    'description': "Top {limit} items by {ord_col} descending"
                },
                {
                    'template': "SELECT * FROM {table} ORDER BY {ord_col1} DESC, {ord_col2} ASC LIMIT {limit}",
                    'description': "Top {limit} items ordered by multiple columns"
                }
            ],
            'where': [
                {
                    'template': "SELECT * FROM {table} WHERE {numeric_col} > {threshold}",
                    'description': "Items where {numeric_col} exceeds {threshold}"
                },
                {
                    'template': "SELECT * FROM {table} WHERE {text_col} LIKE '%{pattern}%'",
                    'description': "Items where {text_col} contains '{pattern}'"
                }
            ],
            'sum': [
                {
                    'template': "SELECT {group_col}, SUM({numeric_col}) as total FROM {table} GROUP BY {group_col}",
                    'description': "Total {numeric_col} by {group_col}"
                },
                {
                    'template': "SELECT SUM({numeric_col}) as grand_total FROM {table}",
                    'description': "Grand total of {numeric_col}"
                }
            ],
            'count': [
                {
                    'template': "SELECT {group_col}, COUNT(*) as count FROM {table} GROUP BY {group_col}",
                    'description': "Count by {group_col}"
                },
                {
                    'template': "SELECT COUNT(DISTINCT {col}) as unique_count FROM {table}",
                    'description': "Count of unique {col} values"
                }
            ],
            'avg': [
                {
                    'template': "SELECT {group_col}, AVG({numeric_col}) as average FROM {table} GROUP BY {group_col}",
                    'description': "Average {numeric_col} by {group_col}"
                },
                {
                    'template': "SELECT AVG({numeric_col}) as overall_average FROM {table}",
                    'description': "Overall average of {numeric_col}"
                }
            ],
            'aggregation': [
                {
                    'template': "SELECT {group_col}, COUNT(*) as count, SUM({numeric_col}) as total, AVG({numeric_col}) as average FROM {table} GROUP BY {group_col}",
                    'description': "Multiple aggregations by {group_col}"
                },
                {
                    'template': "SELECT {group_col}, MIN({numeric_col}) as min_value, MAX({numeric_col}) as max_value FROM {table} GROUP BY {group_col}",
                    'description': "Min and max {numeric_col} by {group_col}"
                }
            ]
        }

    # Helper function: Generate query parameters
    def generate_query_parameters(self, cols, numeric_cols, text_cols, table_name):
        params = {
            'table': table_name,
            'group_col': random.choice(text_cols),
            'numeric_col': random.choice(numeric_cols),
            'text_col': random.choice(text_cols),
            'ord_col': random.choice(cols),
            'ord_col1': random.choice(cols),
            'ord_col2': random.choice([c for c in cols if c != random.choice(cols)]),
            'col': random.choice(cols),
            'min_count': random.randint(2, 5),
            'threshold': random.randint(10, 100),
            'limit': random.randint(5, 10),
            'pattern': random.choice(['A%', '%B', '%C%'])
        }
        return params
    
    # Helper function: Extract field names
    def extract_field_names(self, columns):
        field_names = []
        for col in columns:
            field_names.append(col[0])
        return field_names


    # Helper function: Extract fields by data type
    def extract_fields_by_type(self, columns, data_types):
        """
        Filters fields based on matching their data types with the specified types.
        """
        filtered_fields = []
        for col in columns:
            if col[1] in data_types:
                filtered_fields.append(col[0])
        return filtered_fields
    
    def generate_mongo_queries(self, collection_name, query_type=None):
        # Retrieve column metadata and split into names and types
        columns, _ = self.sample_mongo_data(collection_name)
        
        # Extract all field names
        fields = self.extract_field_names(columns)
        
        # Extract numeric fields
        numeric_fields = self.extract_fields_by_type(columns, ('int', 'float'))
        
        # Extract text fields
        text_fields = self.extract_fields_by_type(columns, ('str', 'string'))
        
        # Fallback to all fields if no numeric or text fields are found
        if not numeric_fields:
            numeric_fields = fields
        if not text_fields:
            text_fields = fields
        
        # Define MongoDB query patterns for different query types
        patterns = self.get_mongo_query_patterns(fields, numeric_fields, text_fields)
        
        # Generate queries based on the query type
        if query_type and query_type in patterns:
            # Fetch patterns specific to the query type
            pattern_list = patterns[query_type]
            
            # Ensure there are at least 5 queries
            while len(pattern_list) < 5:
                pattern_list.extend(pattern_list[:5 - len(pattern_list)])
            
            # Return exactly 5 queries
            return pattern_list[:5]
        else:
            # Generate a mix of queries from all patterns
            all_queries = []
            for pattern_group in patterns.values():
                all_queries.extend(pattern_group)
            
            # Return 5 randomly selected queries
            return random.sample(all_queries, 5)
    
    def get_mongo_query_patterns(self, fields, numeric_fields, text_fields):
        chosen_field = random.choice(fields)
        chosen_numeric = random.choice(numeric_fields)
        chosen_text = random.choice(text_fields)
        sample_fields = random.sample(fields, min(3, len(fields)))
        
        return {
            'find': [
                {
                    'title': "Basic Find",
                    'description': "Find all documents in the collection to explore the complete dataset",
                    'query': {'type': 'find', 'filter': {}},
                    'mongo_command': "db.collection_name.find({})"
                },
                {
                    'title': "Find with Limit",
                    'description': "Find a sample of documents to quickly preview the data structure and content",
                    'query': {'type': 'find', 'filter': {}, 'limit': 5},
                    'mongo_command': "db.collection_name.find({}).limit(5)"
                }
            ],
            'find_criteria': [
                {
                    'title': "Find with Simple Condition",
                    'description': f"Find documents that contain the {chosen_field} field to understand data completeness",
                    'query': {'type': 'find', 'filter': {chosen_field: {'$exists': True}}},
                    'mongo_command': f"db.collection_name.find({{ {chosen_field}: {{ $exists: true }} }})"
                },
                {
                    'title': "Find with Comparison",
                    'description': f"Find documents where {chosen_numeric} is greater than 50 to identify significant entries",
                    'query': {'type': 'find', 'filter': {chosen_numeric: {'$gt': 50}}},
                    'mongo_command': f"db.collection_name.find({{ {chosen_numeric}: {{ $gt: 50 }} }})"
                },
                {
                    'title': "Find with Multiple Criteria",
                    'description': f"Find documents matching multiple conditions to analyze complex patterns in {chosen_field} and {chosen_numeric}",
                    'query': {
                        'type': 'find',
                        'filter': {
                            chosen_field: {'$exists': True},
                            chosen_numeric: {'$gt': 50}
                        }
                    },
                    'mongo_command': f"db.collection_name.find({{ {chosen_field}: {{ $exists: true }}, {chosen_numeric}: {{ $gt: 50 }} }})"
                }
            ],
            'projection': [
                {
                    'title': "Basic Projection",
                    'description': "Select specific fields to create a focused view of the data",
                    'query': {
                        'type': 'find',
                        'filter': {},
                        'projection': {field: 1 for field in sample_fields}
                    },
                    'mongo_command': f"db.collection_name.find({{}}, {{ {', '.join(f'{field}: 1' for field in sample_fields)} }})"
                },
                {
                    'title': "Exclude Fields",
                    'description': "Remove specific fields to simplify the data view and focus on relevant information",
                    'query': {
                        'type': 'find',
                        'filter': {},
                        'projection': {field: 0 for field in sample_fields[:2]}
                    },
                    'mongo_command': f"db.collection_name.find({{}}, {{ {', '.join(f'{field}: 0' for field in sample_fields[:2])} }})"
                }
            ],
            'aggregate': [
                {
                    'title': "Simple Group and Count",
                    'description': f"Group documents by {chosen_field} and count occurrences to understand data distribution",
                    'query': {
                        'type': 'aggregate',
                        'pipeline': [
                            {'$group': {'_id': f"${chosen_field}", 'count': {'$sum': 1}}},
                            {'$sort': {'count': -1}}
                        ]
                    },
                    'mongo_command': f"db.collection_name.aggregate([{{ $group: {{ _id: '${chosen_field}', count: {{ $sum: 1 }} }} }}, {{ $sort: {{ count: -1 }} }}])"
                },
                {
                    'title': "Aggregate with Sum",
                    'description': f"Calculate the total of {chosen_numeric} for each {chosen_text} category to identify patterns and trends",
                    'query': {
                        'type': 'aggregate',
                        'pipeline': [
                            {'$group': {
                                '_id': f"${chosen_text}",
                                'total': {'$sum': f"${chosen_numeric}"}
                            }},
                            {'$sort': {'total': -1}}
                        ]
                    },
                    'mongo_command': f"db.collection_name.aggregate([{{ $group: {{ _id: '${chosen_text}', total: {{ $sum: '${chosen_numeric}' }} }} }}, {{ $sort: {{ total: -1 }} }}])"
                }
            ],
            'group': [
                {
                    'title': "Group with Multiple Aggregations",
                    'description': f"Calculate multiple statistics (count, average, maximum) for {chosen_text} to provide comprehensive insights",
                    'query': {
                        'type': 'aggregate',
                        'pipeline': [
                            {'$group': {
                                '_id': f"${chosen_text}",
                                'count': {'$sum': 1},
                                'avg': {'$avg': f"${chosen_numeric}"},
                                'max': {'$max': f"${chosen_numeric}"}
                            }},
                            {'$sort': {'count': -1}}
                        ]
                    },
                    'mongo_command': f"db.collection_name.aggregate([{{ $group: {{ _id: '${chosen_text}', count: {{ $sum: 1 }}, avg: {{ $avg: '${chosen_numeric}' }}, max: {{ $max: '${chosen_numeric}' }} }} }}, {{ $sort: {{ count: -1 }} }}])"
                }
            ],
            'match': [
                {
                    'title': "Match and Group",
                    'description': f"Filter documents where {chosen_numeric} exceeds threshold, then group by {chosen_text} to analyze patterns in significant entries",
                    'query': {
                        'type': 'aggregate',
                        'pipeline': [
                            {'$match': {chosen_numeric: {'$gt': 50}}},
                            {'$group': {
                                '_id': f"${chosen_text}",
                                'count': {'$sum': 1}
                            }},
                            {'$sort': {'count': -1}}
                        ]
                    },
                    'mongo_command': f"db.collection_name.aggregate([{{ $match: {{ {chosen_numeric}: {{ $gt: 50 }} }} }}, {{ $group: {{ _id: '${chosen_text}', count: {{ $sum: 1 }} }} }}, {{ $sort: {{ count: -1 }} }}])"
                }
            ]
        }

    def execute_query(self, query, dataset_name, db_type):
        try:
            if db_type == 'sql':
                self.execute_sql_query(query['query'])
            else:
                print("\nExecuting query...")
                # MongoDB queries may depend on the dataset name for collection identification
                self.execute_mongo_query(query['query'], dataset_name)
        except Exception as e:
            # Catch and handle exceptions that may arise during query execution
            print(f"Error executing query: {e}")

    def execute_sql_query(self, query):
        # Establish a connection to the MySQL database
        cnx = self.connect_mysql()
        # Create a cursor object for executing SQL commands
        cursor = cnx.cursor()
        # Execute the provided SQL query
        cursor.execute(query)
        # Fetch all results from the executed query
        results = cursor.fetchall()

        if results:
            # Extract column names from the cursor's description attribute (contains metadata about the results)
            headers = [desc[0] for desc in cursor.description]
            print("\nResults:")
            # Print the headers in a row, separated by pipes
            print(" | ".join(headers))
            # Print a divider line matching the header length
            print("-" * len(" | ".join(headers)))

            # Iterate through the results and print up to 8 rows
            for i, row in enumerate(results):
                if i < 8:
                    # Convert each value in the row to a string and join them with pipes for display
                    print(" | ".join(str(val) for val in row))
                elif i == 8:
                    # Print an ellipsis after the 8th row to indicate truncation
                    print("...")
                elif i == len(results) - 1:
                    # Print the last row if results exceed the display limit
                    print(" | ".join(str(val) for val in row))
            
            # Display the total number of rows in the result set
            print(f"\nTotal rows: {len(results)}")
        else:
            # Handle the case where no rows are returned by the query
            print("No results found.")
        
        cnx.close()

    def execute_mongo_query(self, query: Dict, collection_name):
        db = self.connect_mongo()
        collection = db[collection_name]

        if query['type'] == 'find':
            # Extract the filter criteria from the query dictionary (default to an empty filter if not provided)
            filter_dict = query.get('filter', {})
            # Extract the projection (field selection) from the query dictionary if available
            projection = query.get('projection', None)
            # Perform a find operation with the filter and projection and convert results to a list
            results = list(collection.find(filter_dict, projection))
        elif query['type'] == 'aggregate':
            # Execute the aggregation pipeline defined in the query and convert results to a list
            results = list(collection.aggregate(query['pipeline']))

        if results:
            print("\nResults:")
            # Iterate through the results and print up to 8 documents
            for i, doc in enumerate(results):
                if i < 8:
                    # Remove the `_id` field from each document to simplify display
                    if '_id' in doc:
                        del doc['_id']
                    print(doc)
                elif i == 8:
                    # Print an ellipsis after the 8th document to indicate truncation
                    print("...")
                elif i == len(results) - 1:
                    # Print the last document if results exceed the display limit
                    if '_id' in doc:
                        del doc['_id']
                    print(doc)
            
            # Display the total number of documents in the result set
            print(f"\nTotal documents: {len(results)}")
        else:
            # Handle the case where no documents match the query
            print("No results found.")


def main():
    chatdb = ChatDB()
    chatdb.display_available_databases()
    
    while True:
        try:
            command = input(f"\n{chatdb.get_prompt()} ").strip().lower()
            
            if command == 'commands':
                chatdb.get_commands()
            
            elif command == 'switch database':
                chatdb.current_db_type = None
                chatdb.current_dataset = None
                chatdb.display_available_databases()
                
                # Get dataset name
                dataset = input("\nEnter dataset name: ").strip()
                
                # Try to set the dataset
                if not chatdb.set_current_dataset(dataset):
                    print(f"Dataset '{dataset}' not found")
                
            elif command == 'upload dataset':
                db_type = input("Enter database type (sql/mongo): ").strip().lower()
                if db_type in ['sql', 'mongo']:
                    file_path = input("Enter CSV file path: ").strip()
                    dataset = input(f"Enter new {db_type} dataset name: ").strip()
                    chatdb.current_db_type = db_type
                    chatdb.upload_csv(file_path, dataset, db_type)
                else:
                    print("Invalid database type")
                
            elif command == 'explore database':
                if chatdb.current_dataset:
                    chatdb.show_sample_data(chatdb.current_dataset, chatdb.current_db_type)
                else:
                    print("Please select a dataset first")
                    
            elif command == 'generate queries':
                if not chatdb.current_dataset:
                    print("Please select a dataset first")
                    continue
                
                while True:  # Create submenu loop
                    if chatdb.current_db_type == 'sql':
                        print("\nAvailable SQL query types:")
                        print("example sql queries")
                        print("example query with group by")
                        print("example query with having")
                        print("example query with where")
                        print("example query with order by")
                        print("example query with aggregation")
                        print("example sum query")
                        print("example count query")
                        print("example avg query")
                        print("\nType 'exit' to return to main menu")
                    else:
                        print("\nAvailable MongoDB query types:")
                        print("example find functions")
                        print("example aggregate function")
                        print("example find function with query criteria")
                        print("example find function with projection")
                        print("example aggregate with group")
                        print("example aggregate with match")
                        print("\nType 'exit' to return to main menu")
                    
                    query_command = input("\nEnter query type: ").strip().lower()
                    
                    if query_command == 'exit':
                        print("\nType 'commands' to see the list of available commands")
                        break
                    
                    db_type, query_type = chatdb.process_command(query_command)
                    
                    if db_type and db_type == chatdb.current_db_type:
                        queries = chatdb.generate_query(chatdb.current_dataset, db_type, query_type)
                        print("\nGenerated Queries:")
                        for i, query in enumerate(queries, 1):
                            print(f"\n{i}. {query.get('description', query.get('title', 'Query'))}:")
                            if db_type == 'sql':
                                print(query['query'])
                            else:
                                # Print MongoDB command and parameters
                                if 'mongo_command' in query:
                                    mongo_command = query['mongo_command'].replace('collection_name', chatdb.current_dataset)
                                    print(f"MongoDB Command:")
                                    print(mongo_command)
                                print(f"\nQuery type: {query['query']['type']}")
                                # print(f"Parameters: {query['query']}")
                            print("\nExecuting query...")
                            chatdb.execute_query(query, chatdb.current_dataset, db_type)
                        print("\nType 'commands' to see the list of available commands")
                    else:
                        print("Invalid query type")
                    
            elif command == 'exit':
                print("Thank you for using ChatDB!")
                break
                
            else:
                # Check if it's a dataset name
                if not chatdb.current_dataset and chatdb.set_current_dataset(command):
                    continue
                
                # Check if it's a query command
                db_type, query_type = chatdb.process_command(command)
                if db_type:
                    if db_type != chatdb.current_db_type:
                        print(f"Current database type is {chatdb.current_db_type}, but command is for {db_type}")
                else:
                    print("Invalid command. Type 'commands' to see available commands.")
                
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
