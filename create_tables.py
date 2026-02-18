import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all existing tables defined in drop_table_queries.

    Iterates through each SQL statement in `drop_table_queries`,
    executes it using the provided cursor, and commits the transaction.
    If any query fails, the exception is caught and printed.

    Args:
        cur (psycopg2.extensions.cursor): Active database cursor used to execute SQL commands.
        conn (psycopg2.extensions.connection): Active database connection used to commit transactions.

    Returns:
        None
    """
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
        print ("All tables dropped successfully")
    except Exception as e:
        print (f"Failed to drop table {query}")
        print (e)


def create_tables(cur, conn):
    """
    Create all tables defined in create_table_queries.

    Iterates through each SQL statement in `create_table_queries`,
    executes it using the provided cursor, and commits the transaction.
    If any query fails, the exception is caught and printed.

    Args:
        cur (psycopg2.extensions.cursor): Active database cursor used to execute SQL commands.
        conn (psycopg2.extensions.connection): Active database connection used to commit transactions.

    Returns:
        None
    """
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
        print ("All tables created successfully")
    except Exception as e:
        print (f"Failed to create table {query}")
        print (e)     


def main():
    """
    Orchestrate the database setup process.

    This function performs the following steps:
    1. Reads cluster configuration details from 'dwh.cfg'.
    2. Establishes a connection to the Redshift database.
    3. Drops existing tables.
    4. Creates new tables.
    5. Closes the database connection.

    The configuration file must contain a [CLUSTER] section
    with host, dbname, user, password, and port values.

    Returns:
        None
    """
    # Reading config file dwh.cfg
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Creating database connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Close database connection
    conn.close()


if __name__ == "__main__":
    main()