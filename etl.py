import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from datetime import datetime


def load_staging_tables(cur, conn):
    """
    Execute COPY commands to load data into staging tables.

    This function iterates through the list of COPY SQL queries defined
    in `copy_table_queries`, executes each query using the provided
    database cursor, and commits the transaction after each execution.
    It also logs the start and completion time for each query.

    Args:
        cur (psycopg2.extensions.cursor): Active database cursor used to execute queries.
        conn (psycopg2.extensions.connection): Active database connection used to commit transactions.

    Returns:
        None
    """

    print ("Loading staging tables")
    for query in copy_table_queries:
        print(f"Query {query} processing started at {datetime.now().strftime('%Y%m%d %H:%M:%S')}")
        cur.execute(query)
        conn.commit()
        print (f"Query {query} processing completed at {datetime.now().strftime('%Y%m%d %H:%M:%S')}")


def insert_tables(cur, conn):
    """
    Execute INSERT commands to populate final analytics tables.

    This function iterates through the list of INSERT SQL queries defined
    in `insert_table_queries`, executes each query using the provided
    database cursor, and commits the transaction after each execution.
    It logs the start and completion time for each query.

    Args:
        cur (psycopg2.extensions.cursor): Active database cursor used to execute queries.
        conn (psycopg2.extensions.connection): Active database connection used to commit transactions.

    Returns:
        None
    """

    print ("Insert data into final tables")
    for query in insert_table_queries:
        print(f"Query {query} processing started at {datetime.now().strftime('%Y%m%d %H:%M:%S')}")
        cur.execute(query)
        conn.commit()
        print (f"Query {query} processing completed at {datetime.now().strftime('%Y%m%d %H:%M:%S')}")


def main():
    """
    Orchestrate the ETL process for the data warehouse.

    This function performs the following steps:
    1. Reads database configuration from 'dwh.cfg'.
    2. Establishes a connection to the Redshift cluster.
    3. Loads data into staging tables using COPY commands.
    4. Inserts transformed data into final analytics tables.
    5. Closes the database connection.

    The configuration file must contain a [CLUSTER] section with
    host, dbname, user, password, and port values.

    Returns:
        None
    """
    # Reading config file dwh.cfg
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Creating database connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # Close database connection
    conn.close()


if __name__ == "__main__":
    main()