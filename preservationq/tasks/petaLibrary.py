#from dockertask import docker_task
from subprocess import call,check_output,STDOUT
import request, bagit,os

petaLibraryNode=os.getenv('petaLibraryNode','dtn.rc.colorado.edu')
petaLibraryArchivePath=os.getenv('petaLibraryArchivePath','/archive/libdigicoll')

@task()
def scpPetaLibrary(source,destination):
    if destination.strip()[0]=='/':
        raise ValueError('destination argument must be relative path within Library Archive Location')
    scp_dest = "{0}:{1}".format(petaLibraryNode,os.path.join(petaLibraryArchivePath,destination))
    result = check_output(["scp","-i","id_rsa_dt","-r",source,scp_dest])
    return result
