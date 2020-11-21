import twint
import logging
import time
import pandas as pd
from datetime import timedelta
from itertools import chain
import datetime
import dateparser
import numpy as np
from Util import get_db, time_parser
# Twitter related Function

def fetech_tweet(query):
    tweets = twint.Config()
    tweets.Retries_count = 2
    tweets.Store_object = True
    tweets.Store_object_tweets_list = []
    tweets.Search = query
    tweets.Limit = 100
    tweets.Hide_output = True
    twint.run.Search(tweets)
    df = [vars(i) for i in tweets.Store_object_tweets_list]
    return df

def get_replies(conversation_id, screen_name, created_at):
    # replies, likes, retweets
    replies = twint.Config()

    logging.info("screen_name {}, Created at {}, Conversation ID {}".format(screen_name, created_at, conversation_id))
    print("screen_name {}, Created at {}, Conversation ID {}".format(screen_name, created_at, conversation_id))
    replies.Retries_count = 2
    replies.Store_object = True
    replies.Store_object_tweets_list = []
    replies.Search = "(to:{})".format(screen_name)
    replies.Limit = 1000
    replies.Hide_output = True

    max_try = 2
    try_times = 0
    time_delta = 1
    df_list = []
    while try_times < max_try:
        time.sleep(1)
        if created_at:
            search_end = created_at + timedelta(time_delta)
            search_end_str = search_end.strftime("%Y-%m-%d")
            created_at_str = created_at.strftime("%Y-%m-%d")
            replies.Until = search_end_str
            replies.Since = created_at_str

        twint.run.Search(replies)
        df = pd.DataFrame([vars(i) for i in replies.Store_object_tweets_list])
        replies.search_tweet_list = []
        df = df.rename(columns={"data-conversation-id":"conversation_id","date":"created_at","data-item-id":"id"})
        df.drop_duplicates(inplace=True, subset=['id_str'])
        if len(df) == 0:
            time_delta = 2 * time_delta
            try_times += 1
            continue
        df['username'] = df['username'].apply(lambda x:x.replace("@",""))
        df['nreplies'] = df['replies_count']
        df['nretweets'] = df['retweets_count']

        return_replies_df = []
        print(len(df))
        if len(df) > 0:
            df['id'] = df['id'].apply(lambda x: int(x))
            return_replies_df = df[df['conversation_id'].apply(lambda x:str(x)==str(conversation_id)) ]
            # return_replies_df = df

            logging.info("There are {} replies for {}, {}".format(len(return_replies_df), conversation_id, screen_name))
        df_list.append(df)
        if len(return_replies_df) < 10:
            time_delta = 2 * time_delta
            try_times += 1
        else:
            break



    if len(df_list) == 0:
        return_replies_list = []
        unrelated_replies = []
    else:
        df = pd.concat(df_list)
        df.drop_duplicates(inplace=True, subset=['id'])
        df = df.astype({"id":"int64"})
        return_replies_df = df[df['conversation_id'].apply(lambda x:str(x)==str(conversation_id))]
        return_replies_list = return_replies_df.to_dict(orient="record")
        unrelated_replies = df.to_dict(orient="record")
        print("There are {} related tweets".format(len(return_replies_list)))


    return return_replies_list, unrelated_replies

def get_twint_replies(tweet_id, screen_name,
                      created_at, conversation_id,
                      level,
                      user_collection, tweet_collection,
                      tweet_relation_collection):
    replies, all_replies = get_replies(conversation_id, screen_name, created_at)
    try:
        replies, all_replies = get_replies(conversation_id, screen_name, created_at)
    except Exception as e:
        logging.error("ERROR in get replies {}, {}, {}".format(conversation_id, screen_name, created_at))
        print("ERROR in get replies {}, {}, {}".format(conversation_id, screen_name, created_at))
        logging.error(str(e))
        return []

    user_replies = list(chain.from_iterable([[{"tweet_id":i['id'], "reply_screen_name":j['screen_name']} for j in i['reply_to']] for i in all_replies if len(i['reply_to']) > 0]))

    for i in all_replies:
        screen_name_here = i['username']
        if user_collection.find_one({"screen_name": screen_name_here}) is None:
            user_collection.insert({"post_tweet": [], "screen_name": screen_name_here})
        user_collection.find_one_and_update({"screen_name": screen_name_here},
                                            {"$addToSet": {"post_tweet": i['id']}}, upsert=True)
    for i in user_replies:
        if user_collection.find_one({"screen_name":i['reply_screen_name']}) is None:
            user_collection.insert({"reply_from":[],"screen_name":i['reply_screen_name']})
        user_collection.find_one_and_update({"screen_name":i['reply_screen_name']}, {"$addToSet":{"reply_from":i['tweet_id']}})
    for i in all_replies:
        tweet_collection.find_and_modify({"id": i['id']},{"$set": i}, upsert=True)
    conversation_thread_tweets = [i for i in replies if i['nreplies'] > 0]
    print("There are {} tweets looking for replies at level {}".format(len(conversation_thread_tweets), level))
    replies_ids = [i['id'] for i in replies]

    if level < 3:
        for search_one in conversation_thread_tweets:

            search_one['id'] = int(search_one['id'])

            created_at = time_parser(created_at)

            is_get_replies = search_one['nreplies'] > 0
            if is_get_replies:
                engagement = get_twint_replies(screen_name=search_one['username'],
                                                                   created_at=created_at,
                                                                   conversation_id=conversation_id,
                                                                   tweet_id=search_one['id'],
                                                                   level=level + 1,
                                                     user_collection=user_collection,
                                               tweet_collection=tweet_collection,
                                               tweet_relation_collection=tweet_relation_collection,
                                                     )

                # engagement = {key:{"$each":values} for key, values in engagement.items()}
                engagement = {"replies":{"$each":engagement}}
                if len(engagement) == 0:
                    continue
                if tweet_relation_collection.find_one({"tweet_id": search_one['id']}) is None:
                    tweet_relation_collection.find_one_and_update({"tweet_id": search_one['id']}, {"$set": {"tweet_id": search_one['id'],
                                                                                                   "replies": [], }},
                                                                  upsert=True)
                tweet_relation_collection.find_one_and_update({"tweet_id": search_one['id']}, {"$addToSet": engagement})

    return replies_ids

from tqdm import tqdm
def tweet_in_one(paper_id, query, user_collection, tweet_collection, paper_tweet_collection, tweet_relation_collection):
    tweets = fetech_tweet(query)

    for i in tweets:
        tweet_collection.find_one_and_update({"id": i['id']},{"$set": i}, upsert=True)
    tweets_update = {"tweets": {"$each": [i['id'] for i in tweets]}}
    if paper_tweet_collection.find_one({"paper_id": paper_id}) is None:
        paper_tweet_collection.find_one_and_update({"paper_id": paper_id}, {"$set": {"paper_id": paper_id,
                                                                                       "tweets": [] }},
                                                      upsert=True)
    # store the tweets
    paper_tweet_collection.find_one_and_update({"paper_id":paper_id}, {"$addToSet": tweets_update})

    reply_tweets = [i for i in tweets if i['replies_count'] > 0]
    # get replies recursively
    for i in tqdm(reply_tweets):
        created_at = time_parser(i['datestamp'])
        engagement = get_twint_replies(
        tweet_id=i['id'],
        conversation_id = i['conversation_id'],
        created_at=created_at,
        screen_name = i['username'],
        level=0,
        user_collection = user_collection,
        tweet_collection = tweet_collection,
        tweet_relation_collection = tweet_relation_collection)

        engagement = {"replies": {"$each": engagement}}
        if len(engagement) == 0:
            continue
        if tweet_relation_collection.find_one({"tweet_id": i['id']}) is None:
            tweet_relation_collection.find_one_and_update({"tweet_id": i['id']}, {"$set": {"tweet_id": i['id'],
                                                                                       "replies": [],}},
                                                     upsert=True)
        tweet_relation_collection.find_one_and_update({"tweet_id": i['id']}, {"$addToSet": engagement})




if __name__ == '__main__':
    db = get_db()
    query = "https://arxiv.org/abs/2004.01732"
    data = fetech_tweet(query)
    user_collection = db["c"]
    tweet_collection = db["tweets"]
    tweet_relation_collection = db['tweet_relation']
    tweet_in_one(query, user_collection, tweet_collection, tweet_relation_collection)
