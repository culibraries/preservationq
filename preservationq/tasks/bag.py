
from celery.task import task
import bagit

@task()
def createBag(path,metadata):
    """
    This task will create a bagit bag and return the bag as an objectself.
    args:
    path - path to folder to bagit
    metadata - Dict object

    returns a created bag or a list of created bags
    """
    if isinstance(path,str):
        return bagit.make_bag(path,metadata)
    elif isinstance(path,list):
        result_bags=[]
        for itm in path:
            result_bags.append(bagit.make_bag(itm,metadata))
        return result_bags
    else:
        raise Exception("Argument Error: path must be a string or list of strings")

@task()
def updateBag(path,metadata,manifests=False):
    """
    Task will update metadata and provide a way to update manifest if files were
    added or deleted from the data directory.
    args:
    path - path to bag folder
    metadata - Dict object with items to update
    kwargs:
    manifests - default False - Set to True if added or deleted file in data folder
    """
    bag = bagit.Bag(path)
    for key, value in sorted(metadata.iteritems()):
        bag.info[key]= value
    bag.save(manifests=manifests)

@task()
def validateBag(path,fast=False):
    """
    Task validates bag
    args:
    path - path to bag
    kwargs:
    fast - if bag has Oxum it will not recalculate fixities
    """
    bag = bagit.Bag(path)
    validation_errors=[]
    state=False
    try:
        state=bag.validate(fast=fast)
    except bagit.BagValidationError as e:
        for d in e.details:
            if isinstance(d, bagit.ChecksumMismatch):
                validation_errors.append("expected %s to have %s checksum of %s but found %s" %
                    (d.path, d.algorithm, d.expected, d.found))
    return {"valid":state,"Errors":validation_errors}
