
# importing required modules
from zipfile import ZipFile
import os
import sys
import zipfile
from pathlib import Path
import fnmatch


#Extracting data zip file
#----------------------------------------------
# specifying the zip file name
file_name = "tennis_data.zip"
rootPath = os.getcwd()
pattern = '*.zip'
# opening the zip file in READ mode
with ZipFile(file_name, 'r') as zip:
    # printing all the contents of the zip file
    zip.printdir()

    # extracting all the files
    print('Extracting all the files now...')
    zip.extractall(path= rootPath + '/tennisData')
    print('Done!')
#---------------------------------------------------

#extracting subfiles in seperate folders
#-------------------------------------------------
for root, dirs, files in os.walk(rootPath):
    for filename in fnmatch.filter(files, pattern):
        print(os.path.join(root, filename))
        zipfile.ZipFile(os.path.join(root, filename)).extractall(os.path.join(root, os.path.splitext(filename)[0]))
#------------------------------------------------


