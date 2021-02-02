from math import ceil
import pandas as pd
import time
from prawcore.exceptions import RequestException
from pathlib import Path
import praw


def get_submissions(reddit, ids, max_retries=5):
    current_tries = 0
    submissions_list = []
    while current_tries < max_retries:
        try:
            submissions_info = reddit.info(fullnames=ids)
            for submission in submissions_info:
                submission_dict = vars(submission)
                submissions_list.append(submission_dict)
            return submissions_list
        except RequestException:
            print("Request Exception")
            time.sleep(1)
            current_tries += 1
    return submissions_list


def get_praw_data(reddit, ids):
    # if ids is empty return empty df
    start_index = 0
    ids_len = len(ids)
    num_requests = int(ceil(ids_len / 100))
    end_index = min(100, ids_len)

    dict_data = []
    failed_requests = []
    for i in range(num_requests):
        print(str(i) + "/" + str(num_requests))
        ids_sub_list = ids[start_index:end_index]
        # submission fullname has t3 prefix. Pushshift returns ids without prefix
        prefix = 't3_'
        full_ids = [i if i.startswith(prefix) else (prefix + i) for i in ids_sub_list]
        submissions = get_submissions(reddit, full_ids)
        if len(submissions) == len(full_ids):
            dict_data.extend(submissions)
        else:
            failed_requests.extend(full_ids)
            print("failed " + str(start_index) + " - " + str(end_index))
        start_index = start_index + 100
        end_index = min(end_index + 100, ids_len)
    return pd.DataFrame(dict_data), pd.DataFrame(failed_requests)


def get_praw_failed_requests(reddit, ids):
    return get_praw_data(reddit, ids)


# TODO: It is recommended to use a praw.ini file in order to keep your authentication information separate from your code.
file_path = ""
with open(file_path, 'r') as reddit_credentials:
    lines = reddit_credentials.readlines()
    credentials_dict = {}
    for line in lines:
        credentials = line.split('=')
        credentials_dict[credentials[0]] = credentials[1].strip()
    reddit_credentials.close()

CLIENT_ID = credentials_dict['CLIENT_ID']
CLIENT_SECRET = credentials_dict['CLIENT_SECRET']
PASSWORD = credentials_dict['PASSWORD']
USERNAME = credentials_dict['USERNAME']
USERAGENT = credentials_dict['USERAGENT']

reddit = praw.Reddit(client_id=CLIENT_ID,
                     client_secret=CLIENT_SECRET,
                     password=PASSWORD,
                     username=USERNAME,
                     user_agent=USERAGENT)

folder_path = ''
pushshift_file_path = Path(folder_path, 'pushshift_askreddit_older_posts.csv')
pushift_data_df = pd.read_csv(pushshift_file_path)
ids_list = pushift_data_df['id'].tolist()

submissions_df, failed_requests_df = get_praw_data(reddit, ids_list)

praw_file_path = Path(folder_path, 'praw_askreddit_older_posts.csv')
submissions_df.to_csv(praw_file_path, index=False)

failed_requests_file_path = Path(folder_path, 'praw_failed_requests_older_posts.csv')
failed_requests_df.to_csv(failed_requests_file_path, index=False)

