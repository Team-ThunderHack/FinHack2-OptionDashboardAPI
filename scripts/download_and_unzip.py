# ! reference https://stackoverflow.com/questions/5710867/downloading-and-unzipping-a-zip-file-without-writing-to-disk
from fileinput import filename
from io import BytesIO
from urllib.request import urlopen
import urllib
from zipfile import ZipFile
from datetime import datetime
import pytz,os,glob
import requests

class external_methods:

#   this function will return the address of all the files if downloading and unzipping is succesful
#   else it will return 0  
    def download_and_unzip(address):
        # if the file is already downloaded then skip
        dt = datetime.now().date()
        newDate=dt.strftime('%d''%b''%Y').upper()
        
        # Try making the request
        try:
            url = urllib.request.urlopen(address,timeout=2)
    
    #   if exceptions is handled function won't terminate
    #   you can use (return, raise, exit(0))
    #   but for some unknown reason return not working here
        except Exception as e:
            print(f'########{str(e)}########')
            return 0
            
    #   it executes only when there is no error
        else:
            files=[]        
            with ZipFile(BytesIO(url.read())) as my_zip_file:
                for contained_file in my_zip_file.namelist():                    
                    with open(( "./datafiles/market/market/"+contained_file ), "wb") as output:                        
                        for line in my_zip_file.open(contained_file).readlines():
                            # print(line)
                            output.write(line)
                        
                        files.append("./datafiles/market/market/"+contained_file)
            return files
        
    
    # Logic for deleting files at given address
    def deleteFiles(address):
        # Get a list of all the file paths 
        # that ends with the pattern in the directory
        fileList=glob.glob(address)
        for filePath in fileList:
            try:
                os.remove(filePath)
            except:
                print(f"Error while deleting file : {filePath}")

