from Util import get_db
from download_paper import paper_crawler
from download_twitter_discussion import tweet_in_one
from download_reddit_discussion import reddit_in_one, init_reddit
from tqdm import tqdm
import argparse

def all_in_one(query_list, conference_list):
    # get the interested paper
    db = get_db()
    conference_collection = db["paper"]
    user_collection = db["twitter_user"]
    tweet_collection = db["tweets"]
    tweet_relation_collection = db['tweet_tweet_relation']
    paper_tweet_relation = db['paper_tweet_relation']
    reddit_user_collection = db['reddit_user']
    reddit_collection = db['reddits']
    reddit_comment_collection = db['reddit_reddit_relation']
    sub_reddit, reddit = init_reddit()
    # for query in query_list:
    #     paper_crawler(query, conference_list, conference_collection)
    for conference in conference_collection.find():
        tweet_query_list = [conference['id'], conference['title']]
        paper_id = conference['id']
        for query in tqdm(tweet_query_list):
            # tweet_in_one(paper_id, query, user_collection, tweet_collection, paper_tweet_relation, tweet_relation_collection)
            if "//" in query:
                query = 'url:"{}"'.format(query)
            reddit_in_one(reddit,sub_reddit, paper_id, query, reddit_user_collection, reddit_collection,
                              paper_tweet_relation, reddit_comment_collection)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper_key_word", '-kw', required=True, type=str)
    parser.add_argument("--conference_list",'-cl',type='str', default="emnlp,acl")
    args = parser.parse_args()
    # conference_list = ['emnlp', 'acl', 'nips', 'aaai', 'iclr', 'nurips', 'cikm', 'sdm', 'kdd', 'sigir']
    args.cl = args.cl.split(",")
    all_in_one(query_list=args.kw, conference_list=args.cl)

