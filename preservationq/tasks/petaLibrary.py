from celery.task import task
from celery import signature, group, states
from celery.exceptions import Ignore
from subprocess import call,check_output,STDOUT,check_call
from bag import validateBag
import os

petaLibraryNode=os.getenv('petaLibraryNode','dtn.rc.colorado.edu')
petaLibraryArchivePath=os.getenv('petaLibraryArchivePath','/archive/libdigicoll')
petaLibraryUser = os.getenv('petaLibraryUser','pleaseSetUser')
petaLibrarySubDirectory = os.getenv('petaLibrarySubDirectory','digitalobjects/etds')

@task()
def archiveBag(bags,queue):
    """
    List of paths to bags to move to PetaLibrary
    args: bags - list of paths to bag object
    """
    grouptasks=[]
    notValid=[]
    for bag in bags:
        vBag=validateBag(bag,fast=True)
        if vBag['valid']:
            source=bag
            destination= os.path.join(petaLibrarySubDirectory,source.split('/')[-1])
            grouptasks.append(scpPetaLibrary.si(source,destination).set(queue=queue))
        else:
            notValid.append(bag)
        #print(source,destination)
    res = group(grouptasks)()
    return "Successfully submitted {0} PetaLibrary subtask(s). Not Valid:{1}".format(len(grouptasks),notValid)


@task(bind=True)
def scpPetaLibrary(self,source,destination,user=petaLibraryUser):
    if destination.strip()[0]=='/':
        raise ValueError('Argument: destination must be relative path within Library Archive Location')
    scp_dest = "{0}@{1}:{2}".format(user,petaLibraryNode,os.path.join(petaLibraryArchivePath,destination))
    print(scp_dest)
    try:
        check_call(["scp","-o","StrictHostKeyChecking=no","-o",
                    "UserKnownHostsFile=/dev/null", "-i","id_rsa_dt",
                    "-r",source,scp_dest])
    except Exception as inst:
        self.update_state(
            state = states.FAILURE,
            meta = str(inst)
        )
        raise Ignore
    return "PetaLibrary Location: {0}".format(os.path.join(petaLibraryArchivePath,destination))
