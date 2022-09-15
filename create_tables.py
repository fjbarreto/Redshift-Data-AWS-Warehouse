import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop tables from amazon redshift datawarehouse.

    Running this function drops the tables in drop_table_queries list of queries.

    Args:
        cur: psycopg2 cursor to execute queries in amazon redshift's database.
        conn : connection to the database. It's needed to apply commit function after 
        running a query. """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
     """Create tables on amazon redshift datawarehouse.

    Running this function creates the tables in create_table_queries list of queries.

    Args:
        cur: psycopg2 cursor to execute queries in amazon redshift's database.
        conn : connection to the database. It's needed to apply commit function after 
        running a query. """
        
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()