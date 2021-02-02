import requests
import time
import pandas as pd
import json
from pathlib import Path


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


def get_pushshift_failed_requests(requests):
    pushshift_df = pd.DataFrame()
    for request in requests:
        json_response = make_request(request)
        if 'data' in json_response:
            data = json_response['data']
            df2 = pd.DataFrame(data)
            pushshift_df = pushshift_df.append(df2, ignore_index=True)
    return pushshift_df


subreddit = 'AskReddit'
# before = int(dt.datetime(2020, 11, 30, 11, 59, 59).timestamp())  # November 30, 2020 11:59:59 PM
before = 1594524441  # 2020-07-12 06:27:21
size = 100  # maximum of submissions per response
num_requests = 10000  # 1 million submissions
folder_path = ''

pushshift_df, failed_requests_df = get_pushshift_data(subreddit, before, size, num_requests)

pushshift_file_path = Path(folder_path, 'pushshift_askreddit_older_posts.csv')
pushshift_df.to_csv(pushshift_file_path, index=False)

failed_requests_file_path = Path(folder_path, 'pushshift_failed_requests_older_posts.csv')
failed_requests_df.to_csv(failed_requests_file_path, index=False)






