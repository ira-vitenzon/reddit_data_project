import pandas as pd
from pathlib import Path


def drop_columns():
    columns = list(praw_askreddit_df.columns)
    columns_to_drop = []
    for column in columns:
        if len(praw_askreddit_df[column].unique()) == 1:
            columns_to_drop.append(column)
    # work on a copy of the DataFrame
    praw_askreddit_df.drop(columns_to_drop, axis=1, inplace=True)

    columns_to_drop = ['_reddit', 'selftext', 'link_flair_css_class', 'link_flair_text_color', 'ups', 'link_flair_type',
                       'allow_live_comments', 'selftext_html', 'suggested_sort', 'treatment_tags', 'distinguished',
                       'removal_reason', 'link_flair_background_color', 'whitelist_status', 'author_flair_text_color',
                       'stickied', 'author_flair_richtext', 'author_flair_type', 'author_patreon_flair',
                       'link_flair_template_id', 'post_hint', 'preview']

    praw_askreddit_df.drop(columns_to_drop, axis=1, inplace=True)


def drop_rows():
    #outliers - posts by moderators
    #TODO: change to - drop stickied = True + drop distinguished = MODERATOR
    praw_askreddit_df.drop([893067, 719805], axis=0, inplace=True)

    # This post is referenced to 'Ask me anything' subreddit.
    #TODO: change to - ['link_flair_richtext'] == "[{'e': 'text', 't': '/r/IAmA Request'}]"]
    praw_askreddit_df.drop(333541, axis=0, inplace=True)

    low_score_rows = praw_askreddit_df[praw_askreddit_df['score'] < 50].index
    praw_askreddit_df.drop(low_score_rows, inplace=True)


folder_path = ""
praw_data_file_path = Path(folder_path, "praw_askreddit_all_data.csv")
praw_askreddit_df = pd.read_csv(praw_data_file_path, low_memory=False)
