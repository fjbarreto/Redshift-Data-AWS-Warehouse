import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
     """Load staging tables on amazon redshift datawarehouse.

    Running this function copies data on our S3 bucket to amazon redshift datawarehouse.
    This tables will then be used to create our star schema model.

    Args:
        cur: psycopg2 cursor to execute queries in amazon redshift's database.
        conn : connection to the database. It's needed to apply commit function after 
        running a query. """
        
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables to our final datawarehouse schema
    tables on amazon redshift datawarehouse.

    Running this function will filter information on staging tables and inserted into 
    our final tables as desired in the queries.

    Args:
        cur: psycopg2 cursor to execute queries in amazon redshift's database.
        conn : connection to the database. It's needed to apply commit function after 
        running a query. """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connection succesful')
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()