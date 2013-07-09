import csv
import time
import datetime
import subprocess
import getopt
import sys
from subprocess import Popen, PIPE, STDOUT
import urllib2
import urllib

## global variables
from conf import *

def eod_extract(date,se,plant,outpath):
        ##
        ##
        exp_range = date +"-" + date
        print "date : %s" % exp_range
        try:
                subprocess.check_call([se,plant,"-userlevel","user","-password",sma_password,\
                                                "-exportdir",outpath,"-exportrange", exp_range, "-export", "energy5min"])
        except subprocess.CalledProcessError:
                print "error on SE extract: " + subprocess.STDOUT
                sys.exit(3)

def batch_upload_data(data_file,key,id,r):
    """
     load  SMA data from data file and upload into www.pvoutput.org
    """
    count = 0
    index = 0
    found = 0
    start_energy = 0
    sma_data = []
    reader = csv.reader(open(data_file,"r"),delimiter=";")

    ## load SMA data in single run
    for i in reader:
                count = count + 1
                ## a, ignore 1st 9 lines
                ##print i
                if count > 9 and float(i[2]) > 0:
                        sma_data.append( (i[0][6:10] + i[0][3:5] + i[0][0:2], i[0][11:16], float(i[1]),float(i[2]) ) )
                        index = index + 1
                ## b, found the start energy
                if count > 9 and float(i[2]) == 0 and found == 0:
                            start_energy = float(i[1])
                            found = 1
                            print start_energy

    ## reverse data
    if r:
        sma_data.reverse()
    _tmp = []
    
    for i in sma_data:
        _tmp.append(i)
        if len(_tmp) == 15:
            bulk_update(start_energy,_tmp,key,id)
            del _tmp[0:]
    
    if len(_tmp) > 0:
        bulk_update(start_energy,_tmp,key,id)

        
def bulk_update(init_energy,data,_key,_id):
    """
    """
    _data = "data="
    for i in data:
        item = "%s,%s,%d,%d;" %(i[0],i[1],round((i[2] - init_energy)*1000),round(i[3] * 1000)) 
        print item
        _data = _data + item
        
    print "batch uploading..."
    # using urllib2 to post data
    _header = {'X-Pvoutput-Apikey':  key, 'X-Pvoutput-SystemId' : id}
    _url = 'http://pvoutput.org/service/r2/addbatchstatus.jsp'
    request = urllib2.Request(_url, _data, _header)
    opener = urllib2.build_opener(urllib2.HTTPHandler(debuglevel=1))
    #print request
    response = opener.open(request)
    print response.read()
    time.sleep(sleep_time)
    

def query_stat(key,id):
    """
        query last updated date
        using http://pvoutput.org/service/r1/addbatchstatus.jsp upload data in batch
    """

    cmd = "curl.exe -s -H \"X-Pvoutput-Apikey:%s\" -H \"X-Pvoutput-SystemId:%s\" http://pvoutput.org/service/r1/getstatistic.jsp" %(key,id)

    #print cmd
    p = Popen(cmd,shell=False,stdout=PIPE)
    output = p.stdout.read().split(",")

    print output[8]
    return output[8]
    
    
def main():
    today = datetime.date.today().strftime("%Y%m%d")
    rev = False
    ext = False
  
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'erd:')
    except getopt.GetoptError, err:
        print str(err) 
        print "usage: prog -d yyyymmdd {default: today} -e {extract from plant, default: No} -r {reverse data during load, default: No}"
        sys.exit(2)

    for o, a in opts:
        if o == "-d":
            today = a
        elif o == "-r":
            rev = True
        elif o == "-e":
            ext = True
    print today
    if ext:
        eod_extract(today,se_path,se_plant_data_file,data_path)
        print "complete extract data for: %s" % today
    
    ## data file name with full path
    data_file = data_path + "/" + sys_name + "-" + today + ".csv"
    print "upload...  " + data_file
    ##
    #upload_data(data_file,key,id,rev)

    batch_upload_data(data_file,key,id,rev)
if __name__ == "__main__":
    main()
