from pymongo import MongoClient
from datetime import datetime
import dateparser
import numpy as np

def get_db():
    host = "localhost"
    port = 27017
    db_name = "arxiv"

    client = MongoClient(host, port)
    db = client[db_name]
    return db

def time_parser(created_at):
    if type(created_at) is not datetime:
        created_at = datetime.fromtimestamp(created_at / 1000) if type(
            created_at) is int else dateparser.parse(created_at)
        if created_at is None:
            created_at = dateparser.parse(" ".join(np.array(created_at.split())[[1, 2, -1]]))


    return created_at