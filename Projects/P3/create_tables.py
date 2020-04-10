import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        Drops all the tables in using the queries form 'drop_table_queries'
        
        Args:
            cur:   sql cursor
            conn:  psycopg2 connection object to the database
    """

    print("Droping all existing tables")
    for table, query in drop_table_queries.items():
        cur.execute(query)
        conn.commit()
        print(f"- Table '{table}' dropped")


def create_tables(cur, conn):
    """
        Creates all the tables in using the queries form 'create_table_queries'
        
        Args:
            cur:   sql cursor
            conn:  psycopg2 connection object to the database
    """

    print("Creating all tables")
    for table, query in create_table_queries.items():
        cur.execute(query)
        conn.commit()
        print(f"- Table '{table}' created")


def main():
    """
        Drops all tables and the it creates them.
    """

    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn_str = "host={} dbname={} user={} password={} port={}"
    conn = psycopg2.connect(conn_str.format(*config["CLUSTER"].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
