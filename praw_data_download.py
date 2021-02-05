from math import ceil
import pandas as pd
import time
from prawcore.exceptions import RequestException
from pathlib import Path
import praw
import argparse



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
        print(str(i+1) + "/" + str(num_requests))
        ids_sub_list = ids[start_index:end_index]
        # submission fullname has t3 prefix. Pushshift returns ids without prefix
        prefix = 't3_'
        full_ids = [i if i.startswith(prefix) else (prefix + i) for i in ids_sub_list]
        submissions = get_submissions(reddit, full_ids)
        if len(submissions) > 0:
            dict_data.extend(submissions)
        else:
            failed_requests.extend(full_ids)
            print("failed " + str(start_index) + " - " + str(end_index))
        start_index = start_index + 100
        end_index = min(end_index + 100, ids_len)
    return pd.DataFrame(dict_data), pd.DataFrame(failed_requests)


def get_praw_failed_requests(reddit, ids):
    return get_praw_data(reddit, ids)


reddit = praw.Reddit(site_name="developer", user_agent="developer user agent")

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument("folder_path", type=str, help="directory to save the data")
parser.add_argument("file_name", type=str, help="how to name the data file")
parser.add_argument("ids_path", type=str, help="location of submission ids file")


args = parser.parse_args()

folder_path = args.folder_path
file_name = args.file_name
ids_path = args.ids_path


ids_file_path = Path(ids_path+'.csv')
data = pd.read_csv(ids_file_path)
if 'id' in data.columns:
    ids_list = data['id'].tolist()
    submissions_df, failed_requests_df = get_praw_data(reddit, ids_list)
    praw_file_path = Path(folder_path, file_name+'.csv')
    submissions_df.to_csv(praw_file_path, index=False)
    failed_requests_file_path = Path(folder_path, file_name+'_failed_requests.csv')
else:
    print("missing id column")




