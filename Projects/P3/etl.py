import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    print("Copying data into stagging tables")
    for table, query in copy_table_queries.items():
        cur.execute(query)
        conn.commit()
        print(f"- Data copied into '{table}'")


def insert_tables(cur, conn):
    print("Loading data from stagging tables")
    for table, query in insert_table_queries.items():
        cur.execute(query)
        conn.commit()
        print(f"- Data loaded into '{table}'")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn_str = "host={} dbname={} user={} password={} port={}"
    conn = psycopg2.connect(conn_str.format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()