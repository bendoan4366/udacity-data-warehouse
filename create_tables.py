import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops existing tables from existing sparkify database, if tables exist, by executing drop table queries found in sql_queries.py; Takes 2 arguments:
    - cur: cursor object that points to sparkify db
    - conn: psycopg2 connection to sparkify database in AWS redshift
    """ 
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    creates both staging and udacityprojectdev tables by executing create_table_queries found in sql_queries.py; Takes 2 arguments:
    - cur: cursor object that points to udacityprojectdev db
    - conn: psycopg2 connection to udacityprojectdev database in AWS redshift cluster
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Reads 'dwh.cfg' config file and to get connection parameters for Redshift database

    Creates connection to udacityprojectdev db
    Creates cursor object that points to udacityprojectdev db

    Executes drop_tables and create_tables objects, as defined above
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()