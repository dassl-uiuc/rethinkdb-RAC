import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError, RqlDriverError

# Function to connect to the RethinkDB instance and create the database if it doesn't exist
def connect_to_rethinkdb(host='localhost', port=28015, db='test'):
    try:
        # Instantiate the RethinkDB class and use it for the connection
        rdb = r.RethinkDB()
        connection = rdb.connect(host=host, port=port)
        print(f"Connected to RethinkDB on {host}:{port}")
        
        # Check if the database exists, if not create it
        if db not in rdb.db_list().run(connection):
            rdb.db_create(db).run(connection)
            print(f"Database '{db}' created.")
        else:
            print(f"Database '{db}' already exists.")
        
        # Set the connection to use the desired database
        connection.use(db)
        
        return connection, rdb
    except RqlDriverError as e:
        print(f"Failed to connect to RethinkDB: {e}")
        return None, None

# Function to create a table if it doesn't exist
def create_table(connection, rdb, table_name):
    try:
        if table_name not in rdb.db(connection.db).table_list().run(connection):
            rdb.db(connection.db).table_create(table_name).run(connection)
            print(f"Table '{table_name}' created.")
        else:
            print(f"Table '{table_name}' already exists.")
    except RqlRuntimeError as e:
        print(f"Error creating table: {e}")

# Function to insert N values into the table
def insert_values(connection, rdb, table_name, values):
    try:
        result = rdb.table(table_name).insert(values).run(connection)
        print(f"Inserted {result['inserted']} values into '{table_name}'.")
    except RqlRuntimeError as e:
        print(f"Error inserting values: {e}")

# Function to read values from the table based on keys
def read_values(connection, rdb, table_name, keys):
    try:
        # Fetch the records from the table by their IDs (keys)
        results = rdb.table(table_name).get_all(*keys).run(connection)
        
        print(f"Reading {len(keys)} values from '{table_name}':")
        for result in results:
            print(result)
    except RqlRuntimeError as e:
        print(f"Error reading values: {e}")

# Main function to load and read N values
def load_and_read_values(N):
    # Connect to RethinkDB
    connection, rdb = connect_to_rethinkdb()

    if connection and rdb:
        table_name = 'my_tablesss'
        create_table(connection, rdb, table_name)

        # Generate N values to insert (as a list of dictionaries)
        values = [{'id': i, 'value': f"value_{i}"} for i in range(N)]

        # Insert N values into the table
        insert_values(connection, rdb, table_name, values)

        # Read the inserted values (based on the keys which are the 'id' fields)
        keys = [i for i in range(N)]
        read_values(connection, rdb, table_name, keys)

        # Close the connection
        connection.close()
        print("Connection closed.")

if __name__ == '__main__':
    # Specify the number of values to load and read
    N = 100  # Example: Load and read 100 values into/from the database
    load_and_read_values(N)
