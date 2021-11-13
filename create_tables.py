"""
Sparkify Data ETL for Postgres DB

This file will create or recreate the database and tables each time.
"""

# import libraries
import psycopg2

# load SQL queries from sql_queries.py
from sql_queries import create_table_queries, drop_table_queries

# define host IP
host_ip = "127.0.0.1"


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """

    # connect to default database
    try:
        conn = psycopg2.connect(f"host={host_ip} dbname=postgres user=postgres password=Region76=bicycle")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
    except psycopg2.Error as e:
        print(f"ERROR: Could not connect to default database.\n> {e}")
        return 0, 0

    # drop sparkify database and user if it exists
    cur.execute("DROP DATABASE IF EXISTS sparkifydb;")
    cur.execute("DROP ROLE IF EXISTS student;")

    # create sparkify database with UTF8 encoding and assign student user as owner
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0;")
    cur.execute("CREATE ROLE student SUPERUSER LOGIN PASSWORD 'student';")
    cur.execute("ALTER DATABASE sparkifydb OWNER TO student;")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    try:
        conn = psycopg2.connect(f"host={host_ip} dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    except psycopg2.Error as e:
        print(f"ERROR: Could not connect to 'sparkifydb'\n> {e}")
        return 0, 0

    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.

    - Establishes connection with the sparkify database and gets cursor to it.

    - Drops all the tables.

    - Creates all tables needed.

    - Finally, closes the connection.
    """
    cur, conn = create_database()

    if conn and cur != 0:

        drop_tables(cur, conn)
        create_tables(cur, conn)

        conn.close()


if __name__ == "__main__":
    main()
