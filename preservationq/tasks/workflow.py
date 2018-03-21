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
def archiveBag(bags,queue):
    """
    List of bags to move to PetaLibrary
    args: bags - list of bag object
    """
    grouptasks=[]
    for bag in bags:
        source=bag
        destination= os.path.join(petaLibrarySubDirectory,source.split('/')[-1])
        grouptasks.append(scpPetaLibrary.si(source,destination).set(queue=queue))
        print(source,destination)
        res = group(grouptasks)()
        return "Successfully submitted {0} scpPetaLibrary subtask(s)".format(len(grouptasks))

@task()
def preserveETDWorkflow(zipname):
    queuename = preserveETDWorkflow.request.delivery_info['routing_key']
    bagmetadata={"Source-organization": "University of Colorado Boulder"}
    res = (runExtractRename.s(zipname).set(queue=queuename) |
        createBag.s(bagmetadata).set(queue=queuename) |
        archiveBag.s(queuename).set(queue=queuename))()
    return "Successfully submitted {0} for preservation workflow. Please see childern for workflow progress."
