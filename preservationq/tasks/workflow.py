from celery.task import task
from celery import group
import request,os
from ./etd-preservation import runExtractRename
from ./bag import createBag, updateBag, validateBag
from ./scpPetaLibrary import scpPetaLibrary

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
        grouptasks.append(scpPetaLibrary.s(source,destination))
        res = group(grouptasks)()
        return res.join()

@task()
def preserveETDWorkflow(zipname):
    bagmetadata={"Source-organization": "University of Colorado Boulder"}
    res = (runExtractRename.s(zipname) | createBag.s(bagmetadata) | archiveBag.s())()
    return "Successfully submitted {0} for preservation workflow. Please see childern for workflow progress."
