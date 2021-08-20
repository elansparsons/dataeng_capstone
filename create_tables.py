import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    """
    Creates and connects to capstonedb
    
    Returns: cursor, connection of capstonedb
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create scapstone database with UTF8 encoding based on fresh template0
    cur.execute("DROP DATABASE IF EXISTS capstonedb")
    cur.execute("CREATE DATABASE capstonedb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to capstone database
    conn = psycopg2.connect("host=127.0.0.1 dbname=capstonedb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    
    Returns: None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
 
def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    
    Returns: None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

        
def main():
    """
    Drops capstonedb database if exists then creates it. Establishes connection to
    capstonedb, gets cursor, drops all tables, creates all tables, then closes connection.
    
    Returns: None
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
