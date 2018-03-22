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
import os
import zipfile
import xml.etree.ElementTree as ET
import tempfile
import shutil
import datetime
import string

# Constants for Windows dev only
#ETDSRC = 'C:\\Users\\frsc8564\\My Code\\ETD-Utils\\source\\'
#ETDTGT = 'C:\\Users\\frsc8564\\My Code\\ETD-Utils\\target\\'
#LOGFILE = 'C:\\Users\\frsc8564\\My Code\\ETD-Utils\\process-log.txt'

# Constants to define source, target, and log file locations
ETDSRC = os.getenv('ETDSRC','/data/libetd-archive/')
ETDTGT = os.getenv('ETDTGT','/data/libetd-preservation/')
LOGFILE = os.getenv('ETDLOG','/data/libetd-preservation/process-log.txt')

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

@task()
def runExtractRename(pattern):
    """
    Begin processing all the zip files in the source directory
    """
    print(pattern)
    created_dirs=[]
    files = glob.glob(os.path.join(ETDSRC,pattern))
    for f in files:
        # Create a temp directory to work in
        #td = tempfile.mkdtemp() + '\\' # Windows only
        td = tempfile.mkdtemp() + '/'
        # Unzip the ETD package to the temp directory
        z = zipfile.ZipFile(f)
        z.extractall(td)
        # Create the target directory -- format is submission date, author, and ID
        xml = glob.glob(td + '*.xml')[0]
        sdate = getSubmissionDate(xml)
        author = getAuthor(xml)
        ID = getID(xml)
        newpath = sdate + '_' + author.replace(' ', '') + '_' + ID
        os.mkdir(ETDTGT + newpath)
        created_dirs.append(ETDTGT + newpath)
        # Move ETD files from temp to target directory
        for etd in os.listdir(td):
            try:
                shutil.move(td + etd, ETDTGT + newpath)
                # Log the transacton
                log(os.path.basename(f))
            except Exception as inst:
                log("{0}: {1}\n".format(os.path.basename(f),inst))
        # Cleanup the temp directory
        shutil.rmtree(td)
    return created_dirs

if __name__ == '__main__':
    pattern='*.zip' #default pattern all ZipFile
    #pass argument e.g. exact filename 20140711-etdadmin_upload_101625.zip to run one file
    # or 201407*.zip to load all files submitted in July 2014
    if len(sys.argv)>1:
        pattern= sys.argv[1]
    runExtractRename(pattern)

# EOF
