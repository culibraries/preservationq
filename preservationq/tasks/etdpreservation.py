#!/usr/bin/env python

# Prepares ETD archive files for preservation
#
# Preservation of ETD files that have already been uploaded to CU Scholar
# is a multistep process. The first involves decompressing the ETD zip
# files and moving the contents to a individual folder. The next step
# prepares the files for transfer to long-term storage using BagIt. The
# final step is the transfer of the digital "bag" to long-term storage
# followed by cleanup.
#
# Revision History
# ----------------
# 1.0 2018-03-15 FS Initial release
from celery.task import task
from celery import signature
import glob
import os, json, xmltodict
import zipfile
import xml.etree.ElementTree as ET
import tempfile
import shutil
import datetime
import string
from digitalcatalog import updateMetadata
# Constants for Windows dev only
#ETDSRC = 'C:\\Users\\frsc8564\\My Code\\ETD-Utils\\source\\'
#ETDTGT = 'C:\\Users\\frsc8564\\My Code\\ETD-Utils\\target\\'
#LOGFILE = 'C:\\Users\\frsc8564\\My Code\\ETD-Utils\\process-log.txt'

# Constants to define source, target, and log file locations
ETDSRC = os.getenv('ETDSRC','/data/libetd-archive/')
ETDTGT = os.getenv('ETDTGT','/data/libetd-preservation/')
LOGFILE = os.getenv('ETDLOG','/data/libetd-preservation/process-log.txt')
base_url =os.getenv('APIBASE',"https://geo.colorado.edu/api")

def getRootForXML(xml):
    """
    Utility function for XML parsing
    """
    tree = ET.parse(xml)
    return tree.getroot()

def getSubmissionDate(xml):
    """
    Returns the current contact effective date (close to the submission date)
    """
    root = getRootForXML(xml)
    cdate = root.find('.//DISS_contact_effdt').text.split('/')
    return (cdate[2] + cdate[0] + cdate[1])

def getAuthor(xml):
    """
    Returns the author's lastname and first initial
    """
    root = getRootForXML(xml)
    lname = root.find('.//DISS_surname').text.upper()
    fname = root.find('.//DISS_fname').text[0]
    return lname + '_' + fname

def getID(xml):
    """
    Returns the ProQuest identifer associated with the ETD
    """
    ID = xml.partition('.')
    ID = ID[0][len(ID[0])-10:len(ID[0])-5]
    return ID

def log(file):
    """
    Writes to the log file -- format is date-time stamp followed by the ETD zip file name
    """
    dt = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    logfile = open(LOGFILE, 'a')
    logfile.write('{0} | {1}\n'.format(dt, file))
    logfile.close()

def extractZipCheckXML(f,td):
    """
    Unzip the ETD package to the temp directory
    """
    try:
        z = zipfile.ZipFile(f)
        z.extractall(td)
        xml = glob.glob(td + '*.xml')[0]
        return xml
    except Exception as inst:
        log("ERROR: {0} - {1}".format(os.path.basename(f),str(inst)))
        return False
def createRenameFolder(xml,td):
    """
    Create the target directory -- format is submission date, author, and ID
    """
    xml = glob.glob(td + '*.xml')[0]
    sdate = getSubmissionDate(xml)
    author = getAuthor(xml)
    ID = getID(xml)
    return sdate + '_' + author.replace(' ', '') + '_' + ID

def checkExists(path):
    """
    check if path exists and if it does not => create path
    """
    if not os.path.exists(path):
        os.mkdir(path)

def createMetadata(bag,zipfile,processLocation,task_id):
    return {'bag':bag,
            'locations':{'local':{
                'hostname':os.uname()[1],
                'zipfile':zipfile,
                'processLocation':processLocation,
                'validation':[]
             },'petalibrary':{}},
             'workflow':{'initialtask':{'taskid':task_id,
             'result':"{0}/queue/task/{1}/".format(base_url,task_id)}}
            }
def convertXML2JSON(xml):
    f1= open(xml,'r').read()
    xmldict=xmltodict.parse(f1.encode('utf-8','ignore'))
    return json.loads(json.dumps(xmldict).replace('@','').replace('DISS_',''))

@task()
def runExtractRename(pattern):
    """
    Begin processing all the zip files in the source directory
    """
    task_id = str(runExtractRename.request.id)
    #check if directories present
    checkExists(os.path.join(ETDTGT))
    checkExists(os.path.join(ETDTGT,'processed'))
    checkExists(os.path.join(ETDTGT,'trouble'))
    checkExists(os.path.join(ETDTGT,'bags'))
    if not os.getenv('APITOKEN',None):
        raise Exception('Environmental APITOKEN')
    created_dirs=[]
    files = glob.glob(os.path.join(ETDSRC,pattern))
    for f in files:
        # Create a temp directory to work in
        #td = tempfile.mkdtemp() + '\\' # Windows only
        td = tempfile.mkdtemp() + '/'
        xml = extractZipCheckXML(f,td)
        # Check xml and a pdf file exists
        if xml and len(glob.glob(td + '*.pdf'))>0:
            newpath = createRenameFolder(xml,td)
            destination = os.path.join(ETDTGT,'bags',newpath)
            if os.path.exists(destination):
                log("ERROR: File has already been processed. Check trouble folder for zipfile({0})".format(os.path.basename(f)))
                shutil.move(f, os.path.join(ETDTGT,'trouble'))
            else:
                #Move folder to destination folder
                shutil.move(td,destination)
                #add destination to list to pass to bag section of workflow
                created_dirs.append(destination)
                #copy original zipfile into bag to preserve provenance
                shutil.copy(f,destination)
                # Log the transacton
                log(os.path.basename(f))
                bag=newpath
                processLocation=destination
                zipfile=os.path.join(ETDTGT,'processed',f.split('/')[-1])
                metadata=createMetadata(bag,zipfile,processLocation,task_id)
                try:
                    metadata['metadata']=convertXML2JSON(os.path.join(destination,xml.split('/')[-1]))
                except Exception as inst:
                    metadata['metadata']=str(inst)
                updateMetadata(bag,metadata)
                shutil.move(f, os.path.join(ETDTGT,'processed',f.split('/')[-1]))
            log("----------------------------------------------------------------------------------------------------")
        else:
            # Log the transacton
            log("ERROR:{0}".format(os.path.basename(f)))
            shutil.move(f, os.path.join(ETDTGT,'trouble'))
        #shutil.rmtree(td)
    return created_dirs

if __name__ == '__main__':
    pattern='*.zip' #default pattern all ZipFile
    #pass argument e.g. exact filename 20140711-etdadmin_upload_101625.zip to run one file
    # or 201407*.zip to load all files submitted in July 2014
    if len(sys.argv)>1:
        pattern= sys.argv[1]
    runExtractRename(pattern)

# EOF
