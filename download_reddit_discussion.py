import praw
from praw.models import MoreComments
from praw.models.reddit.subreddit import Subreddit
from praw.models.comment_forest import CommentForest
import yaml
def init_reddit(config_yml):
    with open(config_yml) as f1:
        docs = yaml.load_all(f1, Loader=yaml.FullLoader)
        config = next(docs)
    reddit = praw.Reddit(client_id=config['client_id'], client_secret=config['client_secret'],
                         user_agent=config['user_agent'])
    sub_reddit = reddit.subreddit(config['sub_reddit'])
    return sub_reddit, reddit


def get_reddit_post(paper_id, query, sub_reddit_instance, paper_twitter_collection, reddit_collection, reddit_user_collection):
    submission = list(sub_reddit_instance.search(query))
    submission_id = [i.id for i in submission]
    th = paper_twitter_collection.find_one({"paper_id": paper_id})
    if th is None or "reddit" not in th.keys():
        paper_twitter_collection.find_one_and_update({"paper_id": paper_id},{"$set": {"post_reddit":submission_id}}, upsert=True)
    else:
        paper_twitter_collection.find_one_and_update({"paper_id": paper_id},  {"$addToSet": {"post_reddit": submission_id}})

    for i in submission:
        try:
            author = str(i.author.name)
        except:
            author = "[deleted]"

        i = vars(i)
        i['author'] = author
        i.pop("subreddit")
        i.pop("_reddit")
        reddit_collection.find_one_and_update({"reddit_id": i['id']}, {"$set":i}, upsert=True)
        if reddit_user_collection.find_one({"author":author}) is None:
            reddit_user_collection.insert({"author":author}, {"author":author, "reddit_post":[i['id']]})
        else:
            reddit_user_collection.find_one_and_update({"author": author}, {"$addToSet": {"reddit_post": i['id']}})

    return [i.id for i in submission]
# this will recursive get all the replies
def get_replies(reddit, reddit_id, reddit_comment_collection):
    submission = reddit.submission(id=reddit_id)
    submission.comments.replace_more(limit=None)
    all_comments = submission.comments.list()
    output = generator2list(all_comments)
    for i in output:
        reddit_comment_collection.find_one_and_update({"id":i['id']},{"$set":i}, upsert=True)
    return output


def generator2list(all_comments):
    output = []
    for comment in all_comments:
        replies = comment.replies
        try:
            author = str(comment.author.name)
        except:
            author = "[deleted]"
        comment = vars(comment)
        comment = {key: value for key, value in comment.items() if "_" != key[0] and type(value) is not Subreddit
                   and type(value) is not CommentForest}
        comment['replies'] = generator2list(replies)
        comment['author'] = author
        output.append(comment)
    return output

def reddit_in_one(reddit_instance, sub_reddit_instance, paper_id, query, reddit_user_collection, reddit_collection, paper_twitter_collection, reddit_comment_collection):
    submission_ids = get_reddit_post(paper_id, query, sub_reddit_instance, paper_twitter_collection, reddit_collection, reddit_user_collection)
    print("Finish {} reddit post extraction, there are {} reddit post".format(paper_id, len(submission_ids)))
    th = 0
    for reddit_id in submission_ids:
        output = get_replies(reddit_instance, reddit_id, reddit_comment_collection)
        th += len(output)
    print("There are {} top level post".format(th))

if __name__ == '__main__':
    reddit = init_reddit()
    th = reddit.search("weak supervision")
    th = list(th)


