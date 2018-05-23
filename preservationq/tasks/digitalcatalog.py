from celery.task import task
from celery import signature, group
import json,os,sys,requests

#Required
api_token = os.getenv('APITOKEN',None)
base_url =os.getenv('APIBASE',"https://geo.colorado.edu/api")
api_url_tmpl ="{0}/data_store/data/{1}/{2}/.json"

def updateMetadata(bag,metadata):
    query='{{"filter":{{"bag":"{0}"}}}}'.format(bag)
    catalogData=queryRecords(query)
    if catalogData['count']>0:
        cdata=catalogData['results'][0]
        cdata.update(metadata)
        digitalcatalog(cdata)
    else:
        metadata['bag']=bag
        digitalcatalog(metadata)
def queryRecords(query,collection='digital_objects',database='catalog'):
    api_url="{0}?query={1}".format(api_url_tmpl.format(base_url,database,collection),query)
    headers={'Content-Type':'application/json','Authorization':'Token {0}'.format(api_token)}
    req = requests.get(api_url,headers=headers)
    return req.json()

@task()
def digitalcatalog(data,collection='digital_objects',database='catalog'):
    """
    Task catalogs digital objects and records in API
    """
    api_url=api_url_tmpl.format(base_url,database,collection)
    if not api_token:
        raise Exception('Environmental Variable: APITOKEN is required')

    headers={'Content-Type':'application/json','Authorization':'Token {0}'.format(api_token)}
    req = requests.post(api_url,data=json.dumps(data),headers=headers)
    return {'status_code':req.status_code,'url':"{0}{1}/.json".format(api_url.replace('.json',''),req.json())}


if __name__ == '__main__':
    #Test values to check if post works
    api_url = sys.argv[1]
    metadata ={"TEST":"TEST"}
    print(digitalcatalog(api_url,metadata))
