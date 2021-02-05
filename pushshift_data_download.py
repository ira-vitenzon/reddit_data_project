import requests
import time
import pandas as pd
import json
from pathlib import Path
import argparse


def make_request(request, max_retries=5):
    current_tries = 0
    json_response = {}
    while current_tries < max_retries:
        try:
            response = requests.get(request)
            json_response = json.loads(response.content)
            return json_response
        except requests.ConnectionError:
            print("ConnectionError")
        except requests.HTTPError:
            print("HTTPError")
        except requests.RequestException:
            print("RequestException")
        except json.JSONDecodeError:
            print("Decoding JSON has failed")
        except IOError:
            print("IOError")
        time.sleep(1)
        current_tries += 1

    print("Request failed " + request)
    return json_response


def get_pushshift_data(subreddit, before, size, num_requests):
    pushshift_df = pd.DataFrame()
    failed_requests = []
    request_prefix = 'https://api.pushshift.io/reddit/search/submission/?subreddit='
    for x in range(num_requests):
        request = request_prefix + subreddit + '&before=' + str(before) + '&size=' + str(size)
        print(str(x+1) + "/" + str(num_requests) + " : " + request)
        json_response = make_request(request)
        if 'data' in json_response:
            data = json_response['data']
            df2 = pd.DataFrame(data)
            pushshift_df = pushshift_df.append(df2, ignore_index=True)
            if 'created_utc' in df2.columns:
                before = df2['created_utc'].min()
            else:
                before = before - 100 * 60
        else:
            # request submissions one minute before
            before = before - 100 * 60
            failed_requests.append(request)
    return pushshift_df, pd.DataFrame(failed_requests, columns=['request'])


def get_pushshift_requests(requests):
    pushshift_df = pd.DataFrame()
    for request in requests:
        json_response = make_request(request)
        if 'data' in json_response:
            data = json_response['data']
            df2 = pd.DataFrame(data)
            pushshift_df = pushshift_df.append(df2, ignore_index=True)
    return pushshift_df


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument("folder_path", type=str, help="folder to save the data in quotation marks")
parser.add_argument("file_name", type=str, help="how to name the data file - no spaces")
parser.add_argument("before", type=int, help="time in millis")
parser.add_argument("size", type=int, help="number of submissions per request. Maximum is 100.")
parser.add_argument("num_requests", type=int, help="how many requests to make")
parser.add_argument("subreddit", type=str, help="subreddit name")

args = parser.parse_args()

folder_path = args.folder_path
file_name = args.file_name
before = args.before
size = args.size
num_requests = args.num_requests
subreddit = args.subreddit


pushshift_df, failed_requests_df = get_pushshift_data(subreddit, before, size, num_requests)

pushshift_file_path = Path(folder_path, file_name + '.csv')
pushshift_df.to_csv(pushshift_file_path, index=False)

failed_requests_file_path = Path(folder_path, file_name + '_failed_requests.csv')
failed_requests_df.to_csv(failed_requests_file_path, index=False)



