import sys
sys.path.append('./')

import configparser
import psycopg2
import pandas as pd
from sql_queries import *
import time
from create_tables import main
from sql_queries import copy_table_queries, insert_table_queries

# from etl import etl_main
from create_cluster import createCluster

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


if __name__ == "__main__":
    
    print('First, check if cluster exists then create it')
    createCluster()

    print('Now, it\'s ready for ETL')
    main()
 