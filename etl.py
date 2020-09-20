import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copies log_data and song json files from Udacity s3 storage to staging_events and staging_songs tables, by executing copy_table_queries found in sql_queries.py 
    Takes arguments:

    - cur: cursor object that points to sparkify db
    - conn: psycopg2 connection to sparkify database in AWS redshift

    Output:
    - log_data loaded into staging_events table.
    - song_data loaded into staging_songs table.
    """    
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    loads staging_events and staging_song tables into fact and dimension tables as defined below: 
    - Fact table: songplays
    - Dim tables: users, songs, artists, time

    Takes 2 arguments:
    - cur: cursor object that points to sparkify db
    - conn: psycopg2 connection to sparkify database in AWS redshift

    Output:
    - staging_events data is loaded into songplays, users, artist, and time tables
    - staging_songs data is loaded into songs and artists table
    """ 
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Reads 'dwh.cfg' config file and to get connection parameters for Redshift database

    Creates connection to udacityprojectdev db
    Creates cursor object that points to udacityprojectdev db

    Executes load_staging_tables and insert_tables functions, as defined above
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()