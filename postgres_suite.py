import pandas as pd
import pandas.errors
import psycopg2
import json
import sys
from io import StringIO


def connect_to_server():
    try:
        with open('connection_creds.json', 'r') as conn_config_f:
            conn_details = json.load(conn_config_f)
            host = conn_details['host']
            database = conn_details['database']
            port = conn_details['port']
            user = conn_details['user']
            password = conn_details['password']
    except FileNotFoundError as err:
        print('Couldnt find connection_creds.json. Cannot connect to server. Aborting')
        sys.exit(1)
    try:
        print('Connecting to the server')
        conn = psycopg2.connect(dbname=database, host=host, port=port, user=user, password=password)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        print('Couldnt connect to the server')
        return None
    print("Connection successful")
    return conn


def publish_dataframe_to_server(dataframe, conn):
    if dataframe.empty:
        return
    cur = conn.cursor()
    temp_buf = StringIO()
    dataframe.to_csv(temp_buf, index=False, header=False, quotechar="'")
    # print(temp_buf.getvalue())
    temp_buf.seek(0)
    cur = conn.cursor()
    try:
        cur.copy_from(temp_buf, 'news', sep=",", columns=['article_id', 'link', 'title', 'date_published',
                                                            'date_found', 'keyword'])
        conn.commit()
        print("uploaded to server")
    except (Exception, psycopg2.DatabaseError) as err:
        print(err)
        conn.rollback()
        cur.close()
        return 1

    cur.close()


def get_all_from_db_df_form(conn):
    cur = conn.cursor()
    try:
        cur.execute('SELECT * from news')
    except (Exception, psycopg2.DatabaseError) as err:
        print(err)
        cur.close()
        return None
    print("Number of rows returned: {}".format(cur.rowcount))
    tuples = cur.fetchall()
    df_all = pd.DataFrame(tuples, columns=['article_id', 'link', 'title', 'date_published', 'date_found', 'keyword'])

    cur.close()

    return df_all


def add_and_align_cves_to_df(df_to_add_to):
    try:
        temp_df = pd.read_csv('cve_out.csv')
        ser = pd.Series(range(0, len(temp_df)))
        temp_df.insert(loc=0, column='article_id', value=ser)
        df_to_add_to = pd.concat([df_to_add_to, temp_df], ignore_index=True)
    except pandas.errors.EmptyDataError as err:
        print("No cve to align. Skipping")

    return df_to_add_to








