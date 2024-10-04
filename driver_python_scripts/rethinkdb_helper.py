import rethinkdb as r
import os
import json
from rethinkdb.errors import RqlRuntimeError, RqlDriverError
from concurrent.futures import ThreadPoolExecutor
from itertools import cycle
import random
from datetime import datetime

VALUE_SIZE = 4096
db_name = 'test-rac'

def connect_to_rethinkdb_per_thread(host='localhost', port=28015, db=db_name):
    try:
        rdb = r.RethinkDB()
        connection = rdb.connect(host=host, port=port)
        connection.use(db)  # Switch to the appropriate database
        return connection, rdb
    except RqlDriverError as e:
        print(f"Failed to connect to RethinkDB: {e}")
        return None, None

# Function to connect to RethinkDB instance and create the database if it doesn't exist
def connect_to_rethinkdb(host='localhost', port=28015, db=db_name):
    try:
        rdb = r.RethinkDB()
        connection = rdb.connect(host=host, port=port)
        print(f"Connected to RethinkDB on {host}:{port}")

        # Check if the database exists, if not create it
        if db not in rdb.db_list().run(connection):
            rdb.db_create(db).run(connection)
            print(f"Database '{db}' created.")
        else:
            print(f"Database '{db}' already exists.")
        
        connection.use(db)
        return connection, rdb
    except RqlDriverError as e:
        print(f"Failed to connect to RethinkDB: {e}")
        return None, None

# Function to delete existing tables in the database
def delete_existing_tables(connection, rdb):
    try:
        tables = rdb.db(connection.db).table_list().run(connection)
        for table in tables:
            rdb.db(connection.db).table_drop(table).run(connection)
            print(f"Table '{table}' deleted.")
    except RqlRuntimeError as e:
        print(f"Error deleting tables: {e}")

# Function to create a table if it doesn't exist and set replication to 3
def create_table(connection, rdb, table_name):
    try:
        if table_name not in rdb.db(connection.db).table_list().run(connection):
            rdb.db(connection.db).table_create(table_name, shards=1, replicas=3).run(connection)
            # rdb.db(connection.db).table_create(table_name, shards=1, replicas=2).run(connection)
            print(f"Table '{table_name}' created with replication set to 3.")
        else:
            print(f"Table '{table_name}' already exists.")
    except RqlRuntimeError as e:
        print(f"Error creating table: {e}")

# Function to generate a database file with 1M key-value pairs if it doesn't exist
def generate_db_file(file_name='db_data.json', size=1000000):
    if not os.path.exists(file_name):
        print(f"Generating database file '{file_name}' with 1M key-value pairs...")
        # data = [{'id': i, 'value': f"value_{i}"} for i in range(size)]
        data = [{'id': i, 'value': f"value_{i}".ljust(VALUE_SIZE, '0')} for i in range(size)]
        with open(file_name, 'w') as f:
            json.dump(data, f)
        print(f"Database file '{file_name}' created.")
    else:
        print(f"Database file '{file_name}' already exists.")

def insert_batch(thread_id, table_name, batch, batch_num, host, port, db, chunk_size=1000):
    connection, rdb = connect_to_rethinkdb_per_thread(host, port, db)
    if connection is None:
        print(f"Thread {thread_id}: Failed to connect to RethinkDB")
        return
    
    try:
        # Break down the batch into smaller chunks
        for i in range(0, len(batch), chunk_size):
            sub_batch = batch[i:i + chunk_size]
            result = rdb.table(table_name).insert(sub_batch).run(connection)
            print(f"Thread {thread_id}: Inserted sub-batch {i // chunk_size + 1} - {len(sub_batch)} records into '{table_name}'.")

    except Exception as e:
        print(f"Thread {thread_id}: Error inserting batch {batch_num}: {e}")
    finally:
        connection.close()



# Function to read data from the database file and insert into the database in parallel
def insert_from_db_file(host='localhost', port=28015, db=db_name, table_name='my_table', file_name='db_data.json', batch_size=50000, num_threads=10):
    if os.path.exists(file_name):
        print(f"Reading data from '{file_name}' and inserting into the table '{table_name}' with {num_threads} threads...")
        with open(file_name, 'r') as f:
            data = json.load(f)

            # Use ThreadPoolExecutor for parallel inserts
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    batch_num = i // batch_size + 1
                    executor.submit(insert_batch, batch_num, table_name, batch, batch_num, host, port, db)
        print(f"All batches submitted for insertion.")
    else:
        print(f"Database file '{file_name}' not found.")

# Function to read from RethinkDB based on the keys in the file
def read_from_db_file(host='localhost', port=28015, db=db_name, table_name='my_table', file_name='db_data.json', batch_size=1000):
    # Connect to the database
    connection, rdb = connect_to_rethinkdb(host, port, db)
    if connection is None:
        print("Failed to connect to RethinkDB for reading.")
        return

    if os.path.exists(file_name):
        print(f"Reading keys from '{file_name}' and fetching data from table '{table_name}' in batches of {batch_size}...")

        with open(file_name, 'r') as f:
            data = json.load(f)
            ids = [entry['id'] for entry in data]

            # Fetch records in batches
            total_records = 0
            for i in range(0, len(ids), batch_size):
                batch_ids = ids[i:i + batch_size]
                results = rdb.table(table_name).get_all(*batch_ids).run(connection, read_mode='outdated')

                # Count and print the number of records in the current batch
                batch_records = len(list(results))
                total_records += batch_records
                print(f"Batch {i // batch_size + 1}: {batch_records} records fetched.")

        print(f"{total_records} total records fetched from the table.")
        connection.close()
        print("Reading completed and connection closed.")
    else:
        print(f"Database file '{file_name}' not found.")

def connect_to_multiple_hosts_and_read_round_robin(hosts, port, db, table_name, file_name='db_data.json', batch_size=1000, num_threads=3):
    if not os.path.exists(file_name):
        print(f"Database file '{file_name}' not found.")
        return

    # Define the list of hosts (IP addresses of the RethinkDB instances)
    host_cycle = cycle(hosts)  # Cycle through the hosts for round-robin scheduling

    # Read data from the file
    print(f"Reading data from '{file_name}' and fetching from the table '{table_name}'...")
    with open(file_name, 'r') as f:
        data = json.load(f)
        ids = [entry['id'] for entry in data]

    # Use ThreadPoolExecutor for parallel reads
    total_records = 0
    start_time = datetime.now()
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i:i + batch_size]
            
            # Get the next host from the cycle (round-robin)
            host = next(host_cycle)
            
            # Submit the batch for reading on the current host
            futures.append(executor.submit(read_batch, host, port, db, table_name, batch_ids, i // batch_size + 1))

        # Process results as they complete
        for future in futures:
            batch_records = future.result()
            total_records += batch_records
            print(f"Batch completed with {batch_records} records fetched.")
    end_time = datetime.now()
    
    total_time = (end_time - start_time).total_seconds()
    print(f"{total_records} total records fetched from the table across hosts: {', '.join(hosts)}.")
    print(f"Time taken: {total_time} seconds.")
    print(f"Throughput: {total_records / total_time} records per second.")

# Helper function for reading a batch from a specific host
def read_batch(host, port, db, table_name, batch_ids, batch_num):
    connection, rdb = connect_to_rethinkdb_per_thread(host=host, port=port, db=db)
    if connection is None:
        print(f"Failed to connect to RethinkDB on {host} for batch {batch_num}.")
        return 0

    try:
        print(f"Fetching batch {batch_num} from host {host}...")
        results = rdb.table(table_name).get_all(*batch_ids).run(connection, read_mode='outdated')
        records_count = len(list(results))
        print(f"Batch {batch_num} fetched {records_count} records from host {host}.")
        return records_count
    except Exception as e:
        print(f"Error reading batch {batch_num} from host {host}: {e}")
        return 0
    finally:
        connection.close()

def connect_to_multiple_hosts_and_update_round_robin(hosts, port, db, table_name, file_name='db_data.json', batch_size=1000, num_threads=3, update_percentage=0.1):
    if not os.path.exists(file_name):
        print(f"Database file '{file_name}' not found.")
        return

    # Define the list of hosts (IP addresses of the RethinkDB instances)
    host_cycle = cycle(hosts)  # Cycle through the hosts for round-robin scheduling

    # Read data from the file
    print(f"Reading data from '{file_name}' to update keys in the table '{table_name}'...")
    with open(file_name, 'r') as f:
        data = json.load(f)
        ids = [entry['id'] for entry in data]

    # Calculate the number of records to update (10% of the dataset)
    num_updates = int(len(ids) * update_percentage)
    ids_to_update = random.sample(ids, num_updates)

    # Use ThreadPoolExecutor for parallel updates
    total_updated = 0
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(0, len(ids_to_update), batch_size):
            batch_ids = ids_to_update[i:i + batch_size]
            
            # Get the next host from the cycle (round-robin)
            host = next(host_cycle)
            
            # Submit the batch for updating on the current host
            futures.append(executor.submit(update_batch, host, port, db, table_name, batch_ids, i // batch_size + 1))

        # Process results as they complete
        for future in futures:
            batch_updated = future.result()
            total_updated += batch_updated
            print(f"Batch completed with {batch_updated} records updated.")

    print(f"{total_updated} total records updated in the table across hosts: {', '.join(hosts)}.")

# Helper function for updating a batch from a specific host
def update_batch(host, port, db, table_name, batch_ids, batch_num):
    connection, rdb = connect_to_rethinkdb_per_thread(host=host, port=port, db=db)
    if connection is None:
        print(f"Failed to connect to RethinkDB on {host} for batch {batch_num}.")
        return 0

    try:
        print(f"Updating batch {batch_num} on host {host}...")
        records_updated = 0

        # Fetch the original records
        results = rdb.table(table_name).get_all(*batch_ids).run(connection, read_mode='outdated')

        for record in results:
            new_value = generate_new_value_same_size(record)  # Generate new value with same size as old value
            rdb.table(table_name).get(record['id']).update({'value': new_value}).run(connection)
            records_updated += 1

        print(f"Batch {batch_num} updated {records_updated} records on host {host}.")
        return records_updated
    except Exception as e:
        print(f"Error updating batch {batch_num} on host {host}: {e}")
        return 0
    finally:
        connection.close()

# Helper function to generate a new value with the same size as the old value
def generate_new_value_same_size(record):
    old_value = record['value']
    new_value = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(len(old_value)))
    return new_value