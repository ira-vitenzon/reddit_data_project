import pandas as pd
from pathlib import Path
import datetime as dt
from sklearn.cluster import KMeans
from sklearn.tree import DecisionTreeClassifier
from sklearn import metrics
from sklearn.metrics import classification_report


def create_features():
    title_lenght = praw_askreddit_df['title'].apply(lambda x: len(x.split())).rename('title_lenght')

    def epoch_to_hour(x):
        date = dt.datetime.fromtimestamp(x)
        return int(date.strftime("%H"))

    hour_of_day = praw_askreddit_df['created_utc'].apply(lambda x: epoch_to_hour(x)).rename('hour_of_day')

    def epoch_to_day_of_week(x):
        date = dt.datetime.fromtimestamp(x)
        return int(date.strftime("%w"))

    day_of_week = praw_askreddit_df['created_utc'].apply(lambda x: epoch_to_day_of_week(x)).rename(
        'day_of_week')

    def epoch_to_week_of_year(x):
        date = dt.datetime.fromtimestamp(x)
        return int(date.strftime("%U"))

    week_of_year = praw_askreddit_df['created_utc'].apply(lambda x: epoch_to_week_of_year(x)).rename(
        'week_of_year')

    def epoch_to_date(x):
        date = dt.datetime.fromtimestamp(x)
        return date.strftime("%x")

    date = praw_askreddit_df['created_utc'].apply(lambda x: epoch_to_date(x)).rename('date')

    serious_replies = (praw_askreddit_df['link_flair_text'] == 'Serious Replies Only').astype(int).rename(
        'serious_replies')

    is_over_18 = praw_askreddit_df['over_18'].astype(int)
    is_spoiler = praw_askreddit_df['spoiler'].astype(int)
    score = praw_askreddit_df['score']

    features_list = [title_lenght, hour_of_day, day_of_week, week_of_year, serious_replies, is_over_18, is_spoiler,
                     score]
    features_df = pd.concat(features_list, axis=1)


def cluster_samples_by_score(features_df):
    X = features_df['score'].to_numpy()
    X = X.reshape(-1, 1)

    num_of_clusters = 8
    KM = KMeans(n_clusters=num_of_clusters, random_state=0)
    KM.fit(X)
    labels = pd.DataFrame(KM.labels_, columns=['label'], index=features_df.index)

    labeled_df = pd.concat([features_df, labels], axis=1)
    labeled_df.drop('score', axis=1, inplace=True)


def train_validation_set(labeled_df):
    median_week = labeled_df['week_of_year'].median()
    train = labeled_df[labeled_df['week_of_year'] <= median_week]
    validation = labeled_df[labeled_df['week_of_year'] > median_week]


def train(train, validation):
    feature_cols = ['title_lenght', 'hour_of_day', 'day_of_week', 'serious_replies', 'over_18', 'spoiler']
    X_train = train[feature_cols]
    y_train = train['label']
    X_valid = validation[feature_cols]
    y_valid = validation['label']
    clf = DecisionTreeClassifier()
    clf = clf.fit(X_train, y_train)
    y_pred = clf.predict(X_valid)

    print("Accuracy:", metrics.accuracy_score(y_valid, y_pred))
    labels_list = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    print(classification_report(y_valid, y_pred, labels=labels_list))






folder_path = ""
praw_data_file_path = Path(folder_path, "praw_askreddit_clean_data.csv")
praw_askreddit_df = pd.read_csv(praw_data_file_path, low_memory=False)