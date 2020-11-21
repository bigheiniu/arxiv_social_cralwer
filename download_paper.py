import arxiv
from tqdm import tqdm
import pandas as pd

def paper_crawler(query, conference_list, conference_collection, output_dir=None):
    return_list_in = []
    return_list_noin = []
    for i in tqdm(range(0, 500, 100)):
        th = arxiv.query(query=query, max_results=100, start=i)
        # filter uninterested paper
        th_conference = [i for i in th if any([c in i['arxiv_comment'].lower() for c in conference_list if i['arxiv_comment']])]
        th_notconference = [i for i in th if any([c in i['arxiv_comment'].lower() for c in conference_list
                                                  if i['arxiv_comment']]) is False]
        # download the pdf
        # for paper in tqdm(th_conference):
        #     arxiv.download(paper, dirpath=output_dir)
        # for paper in tqdm(th_notconference):
        #     arxiv.download(paper, dirpath=output_dir+"_no")
        return_list_in.extend(th_conference)
        return_list_noin.extend(th_notconference)
    in_conference = pd.DataFrame(return_list_in)
    notin_conference = pd.DataFrame(return_list_noin)
    if output_dir:
        in_conference.to_csv(output_dir+"/output.csv")
        notin_conference.to_csv(output_dir+"_no"+"/output.csv")
    else:
        in_conference = in_conference.to_dict(orient='record')
        notin_conference = notin_conference.to_dict(orient='record')
        for element in in_conference:
            for i in conference_list:
                if i in element['arxiv_comment']:
                    element['conference'] = i
            element['interested_tag'] = query
            conference_collection.find_one_and_update({"id": element['id']}, {"$set":element}, upsert=True)

        for element in notin_conference:
            element['conference'] = "uninterested"
            element['interested_tag'] = query
            conference_collection.find_one_and_update({"id": element['id']}, {"$set":element}, upsert=True)

    return in_conference, notin_conference


if __name__ == '__main__':
    query = "weak supervision"
    conference_list=['emnlp','acl','nips','aaai','iclr','nurips','cikm','sdm', 'kdd','sigir']
    output_dir = "download"
    paper_crawler(query, conference_list, output_dir)
