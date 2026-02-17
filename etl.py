import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from datetime import datetime


def load_staging_tables(cur, conn):
    print ("Loading staging tables")
    for query in copy_table_queries:
        print(f"Query {query} processing started at {datetime.now().strftime('%Y%m%d %H:%M:%S')}")
        cur.execute(query)
        conn.commit()
        print (f"Query {query} processing completed at {datetime.now().strftime('%Y%m%d %H:%M:%S')}")


def insert_tables(cur, conn):
    print ("Insert data into final tables")
    for query in insert_table_queries:
        print(f"Query {query} processing started at {datetime.now().strftime('%Y%m%d %H:%M:%S')}")
        cur.execute(query)
        conn.commit()
        print (f"Query {query} processing completed at {datetime.now().strftime('%Y%m%d %H:%M:%S')}")


def main():
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