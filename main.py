import os
import argparse
import sys
import tempfile
import pandas as pd
import requests
import json
from datetime import datetime
import random
import string
import urllib
import psycopg2
import numpy as np
import postgres_suite as ps
import time

from scrapy.crawler import CrawlerProcess
import news_spider as ns

google_api_key = "AIzaSyAKqqJvfGlVwpkJXE4CfBLpa3QhuQ2beaM"
bing_api_key = "cc7ac1fd33a84802b62b5192307f99bc"
BING_ENDPOINT = "https://api.bing.microsoft.com/v7.0/custom/search?"
BING_SUBSCRIPTION_HEADER = "Ocp-Apim-Subscription-Key"
HEADER = {BING_SUBSCRIPTION_HEADER: bing_api_key}

TIMEOUT = 1.5


def init_arg_parser():
    # Initialize argument parser
    parser = argparse.ArgumentParser(prog="Cyber News Scrapper")
    parser.add_argument('saas_file', type=str)
    parser.add_argument('keywords_file', type=str)
    parser.add_argument('output_file', type=str)
    parser.add_argument('--CanAccessDB', choices=['yes', 'no'])
    parser.add_argument('--LocalFile', type=str)
    return vars(parser.parse_args())


def get_config_params():
    config = None
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
    except FileNotFoundError as err:
        print("config.json file does not exist. Creating new one.\n"
              "Note: Config file is critical for counting number of queries sent to Bing. Might cause errors")
        config = {"today_bing_counter": "0", "last_run_date": str(datetime.now())}
        with open('config.json', 'w+') as f:
            json.dump(config, f)
    return config['last_run_date'], config['today_bing_counter']


def get_products(args):
    lst = []
    try:
        with open('saas.txt', 'r') as f:
            lst = [line for line in f]
            lst = [prod.replace("\n", "") for prod in lst]
    except FileNotFoundError as err:
        print("Yo! {} does not exists".format(str(args['saas_file'])))
    return lst


def get_saas_keywords_dict(args):
    try:
        df_saas = pd.read_csv(args['saas_file'], header=None)
    except FileNotFoundError as err:
        print("Yo! {} does not exists".format(str(args['saas_file'])))
    try:
        df_keywords = pd.read_csv(args['keywords_file'], header=None)
    except FileNotFoundError as err:
        print("Yo! {} does not exists".format(str(args['keywords_file'])))

    df_saas.columns = ['Saas']
    df_keywords.columns = ['Keywords']
    df_saas['key'] = 1
    df_keywords['key'] = 1

    merged = pd.merge(df_saas, df_keywords, on='key').drop('key', 1)
    del df_saas
    del df_keywords

    return merged


def get_next_params(sass_str, keyword_str):
    random_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
    lst = [sass_str, keyword_str]
    payload = {
        "q": " ".join(lst),
        "customconfig": "a3a78e07-e5de-4307-b068-ebbb46b0e393",
        "mkt": "en-US",
        "setLang": "EN",
        "count": "40"
    }
    params = urllib.parse.urlencode(payload, quote_via=urllib.parse.quote)
    return params


def read_sql_tmpfile(query, db_engine):
    with tempfile.TemporaryFile() as tmpfile:
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
            query=query, head="HEADER"
        )
        conn = db_engine.raw_connection()
        cur = conn.cursor()
        cur.copy_expert(copy_sql, tmpfile)
        tmpfile.seek(0)
        df = pd.read_csv(tmpfile)
        return df


def bing_request(params):
    try:
        res = requests.get(BING_ENDPOINT, params=params, headers=HEADER)
        json_response = json.loads(res.text)
        # pprint(json_response)
        return json_response
    except requests.exceptions.RequestException as e:
        print("An error occurred: {}".format(e.errno))
        return {}


def push_to_dataframe(df, bing_response, now_time):
    values = bing_response['webPages']['value']
    entry = []
    keyword = bing_response['queryContext']['originalQuery']
    i = 0
    for item in values:
        link, title = item['url'], item['name']
        title = title.replace(",", "\,")
        # print(title)
        # Not all posts have publication dates.
        if 'datePublished' in item:
            pub_date = item['datePublished']
        else:
            pub_date = now_time

        entry = [i, link, title, pub_date, now_time, keyword]
        df.loc[i] = entry
        i = i + 1


def remove_duplicate_links_of_df1(df_to_filter, df_to_check_in):

    if df_to_filter.empty:
        return
    link_lst = df_to_filter['link'].to_list()
    to_delete = list()
    # Fast way to iterrate rows
    for index, i in enumerate(link_lst):
        for id, row in df_to_check_in.iterrows():
            if i == row.link:
                to_delete.append(index)
    df_to_filter.drop(to_delete, inplace=True)


def reset_article_id(new_df, old_len):
    if new_df.empty:
        return new_df
    i = 0
    for index, row in new_df.iterrows():
        new_df.loc[index, 'article_id'] = old_len + i
        i = i + 1
    return new_df


if __name__ == '__main__':
    # Init argparser
    args = init_arg_parser()

    # Get configurations from JSON config file
    last_date_run, today_run_counter = get_config_params()

    # Program will exit if couldn't connect
    conn = ps.connect_to_server()
    # Check if local DB file was provided.
    # If not, try to connect to the DB
    host, port, database, user, password, cur = None, None, None, None, None, None
    # TODO: In the future will fix it so it would try to connect by itself. Removing user input on this.
    if args['CanAccessDB'] == 'no' and args['LocalFile']:
        # TODO: Collect data from file and adjust the current iteration
        pass
    # Each call to postgres disconnects from the server as well
    old_df = None
    old_df = ps.get_all_from_db_df_form(conn)
    old_len = len(old_df)
    # Get the cross product of Saas list and Keywords list
    df_cross = get_saas_keywords_dict(args)
    products = get_products(args)
    # Prepare for Bing custom search API calls
    # Each call must be at least 1 second apart since Bing will block otherwise
    # Using a generator here for better performance
    new_df = pd.DataFrame(columns=['article_id', 'link', 'title', 'date_published', 'date_found', 'keyword'])
    now = datetime.now()
    for index, row in df_cross.iterrows():
        cur_saas = row["Saas"]
        cur_keyword = row["Keywords"]

        params = get_next_params(cur_saas, cur_keyword)
        bingResponseDict = bing_request(params=params)
        if bingResponseDict is {}:
            print("Bing request returned error. Aborting Bing search")
            break
        else:
            push_to_dataframe(new_df, bingResponseDict, now)
            # print(new_df)

        # Should add a small time out for Bing not to block us
        time.sleep(TIMEOUT)
    # Scrapping CVEs with Scrapy.
    # There are similiar services like RedHat's but I wanted to show some web scrapping
    remove_duplicate_links_of_df1(new_df, old_df)
    process = CrawlerProcess(settings={
        'FEED_URI' : 'cve_out.csv',
        'FEED_FORMAT' : 'csv',
        'DOWNLOAD_DELAY': 2
    })
    process.crawl(ns.CveSpider, products_list=products)
    process.start()

    # Temporary file cve_out.csv contains the newly found CVE on cvedetails.com
    new_df = ps.add_and_align_cves_to_df(new_df)
    remove_duplicate_links_of_df1(new_df, old_df)

    # Reset indexing in new_df before upload. Needed for article_id column
    reset_article_id(new_df, old_len)
    ret = ps.publish_dataframe_to_server(dataframe=new_df, conn=conn)
    new_df.to_csv('results.csv', index=False)

    if conn is not None:
        print('Closing connection to server')
        conn.close()
    # remove temp file after running scrapy
    os.remove('cve_out.csv')
