Preservation Queue
====

This task queue works on preservation of digital objects.

#### Individual tasks

1. ETD unzip and naming of bag
2. Bag creation, update, and validation
3. Secure copy to petaLibrary

#### Workflows

1.  preserveETDWorkflow - Unzips, Names ETD folder to appropriate bag name, Bags, copies to Petalibrary.


#### Environmental Variables and keys

##### Required
* id_rsa_dt : Private key used to access the PetaLibrary. This file needs to be located next to celeryconfig.py file.
* petaLibraryUser - user that has access to CU PetaLibrary

##### Optional with default values

* ETDSRC - Default => '/data/libetd-archive/'
* ETDTGT - Default => '/data/libetd-preservation/'
* LOGFILE - Default => '/data/libetd-preservation/process-log.txt'
* petaLibraryNode - Default => 'dtn.rc.colorado.edu'
* petaLibraryArchivePath - Default => '/archive/libdigicoll'
* petaLibrarySubDirectory - Default =>'digitalobjects/etds'
