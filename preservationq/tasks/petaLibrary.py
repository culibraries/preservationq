from celery.task import task
from celery import signature, group, states
from celery.exceptions import Ignore
from subprocess import call,check_output,STDOUT,check_call
from bag import validateBag
from datetime import datetime
import os
from digitalcatalog import updateMetadata, digitalcatalog , queryRecords
#Required
petaLibraryUser = os.getenv('petaLibraryUser',None)
#Optional
petaLibraryNode=os.getenv('petaLibraryNode','dtn.rc.colorado.edu')
petaLibraryArchivePath=os.getenv('petaLibraryArchivePath','/archive/libdigicoll')
petaLibrarySubDirectory = os.getenv('petaLibrarySubDirectory','digitalobjects/etds')

def updateBagValidatationMetadata(bag,validationMetadata):
    query='{{"filter":{{"bag":"{0}"}}}}'.format(bag)
    catalogData=queryRecords(query)
    if catalogData['count']>0:
        cdata=catalogData['results'][0]
        cdata['validation'].append(validationMetadata)
        digitalcatalog(cdata)
    else:
        digitalcatalog({'bag':bag,'validation':[validationMetadata]})
@task()
def archiveBag(bags,queue):
    """
    List of paths to bags to move to PetaLibrary
    args: bags - list of paths to bag object
    """
    grouptasks=[]
    notValid=[]
    valid=[]
    for bag in bags:
        vBag=validateBag(bag,fast=True)
        if vBag['valid']:
            source=bag
            updateBagValidatationMetadata(bag,{"valid":True,"timestamp":datetime.now().isoformat()})
            valid.append({"bag":source.split('/')[-1],"valid":datetime.now()})
            destination= os.path.join(petaLibrarySubDirectory,source.split('/')[-1])
            grouptasks.append(scpPetaLibrary.si(source,destination).set(queue=queue))
        else:
            updateBagValidatationMetadata(bag,{"valid":False,"timestamp":datetime.now().isoformat()})
            notValid.append(bag)
        #print(source,destination)
    res = group(grouptasks)()
    return {"subtasks":len(grouptasks),"valid":valid,"notvalid":notValid}


@task(bind=True)
def scpPetaLibrary(self,source,destination,user=petaLibraryUser):
    try:
        if destination.strip()[0]=='/':
            raise ValueError('Argument: destination must be relative path within Library Archive Location')
        if not user:
            raise Exception('Environmental Variable: petaLibraryUser is required')

        scp_dest = "{0}@{1}:{2}".format(user,petaLibraryNode,os.path.join(petaLibraryArchivePath,destination))
        check_call(["scp","-o","StrictHostKeyChecking=no","-o",
                    "UserKnownHostsFile=/dev/null", "-i","id_rsa_dt",
                    "-r",source,scp_dest])
    except Exception as inst:
        self.update_state(
            state = states.FAILURE,
            meta = str(inst)
        )
        raise Ignore

    metadata = {"bag":"{0}".format(destination.split('/')[-1]) ,
            "petaLibrary": "{0}".format(os.path.join(petaLibraryArchivePath,destination))}
    updateMetadata(metadata["bag"],metadata)
    return metadata
