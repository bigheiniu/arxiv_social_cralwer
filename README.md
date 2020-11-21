## Arxiv Social Crawler
This tool can allow you to crawl the interested paper in arxiv and in the meantime crawl the social 
discussion about this paper on Twitter and Reddit and store all the data in MongoDB database. 

### Install requirement packages
```shell script
pip install -r requirements.txt
```

### Crawl data
```shell script
python all_in_one.py --paper_key_word "weak supervision" \
--conference_list "acl,emnlp"
```