from celery.task import task
from celery import signature, group
import json,os,sys,requests

#Required
api_token = os.getenv('APITOKEN',None)

@task()
def digitalcatalog(api_url,data):
    """
    Task catalogs digital objects and records in API
    """

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
