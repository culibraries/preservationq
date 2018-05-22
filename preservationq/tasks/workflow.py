from celery.task import task
from celery import signature, group
import os
from etdpreservation import runExtractRename
from bag import createBag, updateBag, validateBag
from petaLibrary import archiveBag


@task()
def preserveETDWorkflow(pattern):
    """
    ETD preservation workflow.
    args:
    pattern - Glob pattern or full zip filename

    Result:
    Unzips, Moves and renames folder, Creates Bag, and Secure copy to PetaLibrary
    """
    queuename = preserveETDWorkflow.request.delivery_info['routing_key']
    bagmetadata={"Source-organization": "University of Colorado Boulder"}
    res = (runExtractRename.s(pattern).set(queue=queuename) |
        createBag.s(bagmetadata).set(queue=queuename) |
        archiveBag.s(queuename).set(queue=queuename))()
    return "Succefully submitted pattern({0}) for matching ETD zipfiles for preservation workflow.".format(pattern)
