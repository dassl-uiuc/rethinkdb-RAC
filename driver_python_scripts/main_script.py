from rethinkdb_helper import (
    connect_to_rethinkdb, 
    delete_existing_tables, 
    create_table, 
    generate_db_file, 
    insert_from_db_file, 
    read_from_db_file,
    connect_to_multiple_hosts_and_read_round_robin,
    connect_to_multiple_hosts_and_update_round_robin
)
hosts = ["10.10.1.1", "10.10.1.2", "10.10.1.3"]

load_db = False
# load_db = True
db_name = 'test-rac'

def load_values_into_db():
    # Step 1: Connect to RethinkDB
    connection, rdb = connect_to_rethinkdb()
    table_name = 'my_table'

    if connection and rdb:
        if load_db:
            delete_existing_tables(connection, rdb)
            create_table(connection, rdb, table_name)
            generate_db_file('db_data.json', 1000000)
            insert_from_db_file(host='localhost', port=28015, db=db_name, table_name=table_name, num_threads=10)

        # read_from_db_file(host='localhost', port=28015, db=db_name, table_name=table_name)

        connect_to_multiple_hosts_and_read_round_robin(hosts, port=28015, db=db_name, table_name='my_table')
        
        # connect_to_multiple_hosts_and_update_round_robin(hosts, port=28015, db=db_name, table_name='my_table')
        connection.close()
        print("Connection closed.")

if __name__ == '__main__':
    load_values_into_db()
