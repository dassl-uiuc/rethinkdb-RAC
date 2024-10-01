from rethinkdb_helper import (
    connect_to_rethinkdb, 
    delete_existing_tables, 
    create_table, 
    generate_db_file, 
    insert_from_db_file, 
    read_from_db_file
)
hosts = ["10.10.1.1", "10.10.1.2", "10.10.1.3"]

def load_values_into_db():
    # Step 1: Connect to RethinkDB
    connection, rdb = connect_to_rethinkdb()
    table_name = 'my_table'

    if connection and rdb:
        # Step 2: Delete existing tables
        # delete_existing_tables(connection, rdb)

        # Step 3: Create the table with replication set to 3
        # create_table(connection, rdb, table_name)

        # Step 4: Generate a database file with 1M key-value pairs if it doesn't exist
        # generate_db_file('db_data.json', 1000000)

        # Step 5: Read from the database file and insert data into the table in parallel threads
        # insert_from_db_file(host='localhost', port=28015, db='test', table_name=table_name, num_threads=10)

        read_from_db_file(host='localhost', port=28015, db='test', table_name=table_name)
        # connect_to_multiple_hosts_and_read_round_robin(hosts, port=28015, db='test', table_name='my_table')
        connection.close()
        print("Connection closed.")

if __name__ == '__main__':
    load_values_into_db()
