import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'Successfully ran the query: {query}')

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'Successfully ran the query: {query}')

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # read in the Redshift configuration and connect prior to loading staging tables 
    # After dropping any possible existing tables we create the tables that will be used for staging
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config['CLUSTER']['HOST'],
                                                                                   config['CLUSTER']['DB_NAME'],
                                                                                   config['CLUSTER']['DB_USER'],
                                                                                   config['CLUSTER']['DB_PASSWORD'],
                                                                                   config['CLUSTER']['DB_PORT']
                                                                                   ))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()