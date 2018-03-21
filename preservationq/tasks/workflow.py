from celery.task import task
from celery import signature, group
import os
from etdpreservation import runExtractRename
from bag import createBag, updateBag, validateBag
from petaLibrary import scpPetaLibrary

ETDSRC = os.getenv('ETDSRC','/data/libetd-archive/')
ETDTGT = os.getenv('ETDTGT','/data/libetd-preservation/')
petaLibrarySubDirectory = os.getenv('petaLibrarySubDirectory','digitalobjects/etds')

@task()
def archiveBag(bags):
    """
    List of bags to move to PetaLibrary
    args: bags - list of bag object
    """
    grouptasks=[]
    for bag in bags:
        source=bag.__str__()
        destination= os.path.join(petaLibrarySubDirectory,source.split('/')[-1])
        grouptasks.append(scpPetaLibrary.si(source,destination))
        print(source,destination)
        res = group(grouptasks)()
        return "Successfully submitted subtasks to scpPetaLibrary"

@task()
def preserveETDWorkflow(zipname):
    queuename = preserveETDWorkflow.request.delivery_info['routing_key']
    bagmetadata={"Source-organization": "University of Colorado Boulder"}
    print(zipname)
    res = (runExtractRename.s(zipname).set(queue=queuename) | createBag.s(bagmetadata).set(queue=queuename) | archiveBag.s().set(queue=queuename))()
    return "Successfully submitted {0} for preservation workflow. Please see childern for workflow progress."
