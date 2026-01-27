import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries,count_table_queries, analysis_queries, analyze_questions, tables


def load_staging_tables(cur, conn):
    """fill or extract the basic two tables: Staging events and Staging songs, whic h iwill transform the other tables """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print("staging table are loaded")
        except Exception as e:
            conn.rollback()
            print(f" Query failed:{e}")


def insert_tables(cur, conn):
    """ I will trabsform the date in staging tables to five tables to ease the ability to read and analyse"""
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print("all data are loaded")
        except Exception as e:
            conn.rollback()
            print(f" Query failed:{e}")

def count_tables(cur, conn):
    """ Give the count of each table to confirm it transformed correctly """
    for query,query2 in zip(count_table_queries,tables):
        try:
            cur.execute(query)
            count = cur.fetchone()[0]   
            print(f"{query2} has: {count} row")
        except Exception as e:
            print(f"[ERROR] Counting failed for: {query}\nReason: {e}")

def analyze_tables(cur, conn):
    """Give some stats about the newly created tables"""
    for query, query2 in zip(analysis_queries, analyze_questions):
        try:
            print(query2)
            cur.execute(query)    
            result = cur.fetchall()
            print("Result:")
            for row in result:
                print("   ", row)
        except Exception as e:
            print(f"[ERROR] Analysis failed: {e}")

def main():

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    count_tables(cur, conn)
    analyze_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()