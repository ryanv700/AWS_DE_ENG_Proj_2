import configparser
import psycopg2
import pandas as pd
from sql_queries import copy_table_queries, insert_table_queries, final_test_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'Successfully ran the query: {query}')

def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'Successfully ran the query: {query}')
        
def final_test(cur, conn):
    for query in final_test_queries:
        cur.execute(query)
        #conn.commit()
        # Fetch all the results
        print(f'Successfully ran the query: {query}') 
        print(f'Displaying the results below:')
        
        rows = cur.fetchall()

        # Get the column names from the cursor's description
        columns = [desc[0] for desc in cur.description]

        # Create a DataFrame with the rows and column names
        df = pd.DataFrame(rows, columns=columns)
        print(df)
    

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # read in the Redshift configuration and connect prior to loading staging tables 
    # After loading staging tables we insert final star schema tables into our Redshift DWH
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config['CLUSTER']['HOST'],
                                                                                   config['CLUSTER']['DB_NAME'],
                                                                                   config['CLUSTER']['DB_USER'],
                                                                                   config['CLUSTER']['DB_PASSWORD'],
                                                                                   config['CLUSTER']['DB_PORT']
                                                                                   ))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    final_test(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()