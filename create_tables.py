import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop any existing tables"""
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
        print ("All tables dropped successfully")
    except Exception as e:
        print (f"Failed to drop table {query}")
        print (e)


def create_tables(cur, conn):
    """Create new tables"""
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
        print ("All tables created successfully")
    except Exception as e:
        print (f"Failed to create table {query}")
        print (e)     


def main():

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