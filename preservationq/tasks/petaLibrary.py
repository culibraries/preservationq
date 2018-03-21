from celery.task import task
from subprocess import call,check_output,STDOUT
import os

petaLibraryNode=os.getenv('petaLibraryNode','dtn.rc.colorado.edu')
petaLibraryArchivePath=os.getenv('petaLibraryArchivePath','/archive/libdigicoll')
petaLibraryUser = os.getenv('petaLibraryUser','pleaseSetUser')
@task()
def scpPetaLibrary(source,destination,user=petaLibraryUser):
    if destination.strip()[0]=='/':
        raise ValueError('Argument: destination must be relative path within Library Archive Location')
    scp_dest = "{0}@{1}:{2}".format(user,petaLibraryNode,os.path.join(petaLibraryArchivePath,destination))
    print(scp_dest)
    result = check_output(["scp","-o","StrictHostKeyChecking=no","-o",
                            "UserKnownHostsFile=/dev/null", "-i","id_rsa_dt",
                            "-r",source,scp_dest])
    return result
