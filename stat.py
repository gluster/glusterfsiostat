# glusterfsiostat - A client side tool to gather I/O stats from every Gluster mount.
# Author : Vipul Nayyar <nayyar_vipul@yahoo.com>

import commands
import re
import os
import json
import sys
import time
import copy
from optparse import OptionParser

status, output=commands.getstatusoutput('mount')
mountlines = output.split("\n")

if status != 0:
    print "Unable to gather mount statistics"
    exit(1)

mntarr = []
maxlen = -1

def read_meta(dev):
    for i in mountlines:
        matchobj = re.search(r" type fuse.glusterfs \(.*\)$",i)
        if matchobj:
            i = i.replace(matchobj.group(),"")
            i = i.split(" on ")
            
            mntname = i[0]
            mntpath = i[1]

            if(len(dev) > 0 and dev != mntname):
                continue
            
            temp = {}
            temp["mount_path"] = mntpath
            temp["name"] = mntname
            mntarr.append(temp)

    for i in xrange(0,len(mntarr)):
        os.chdir(mntarr[i]["mount_path"])
        os.chdir(".meta")
        os.chdir("graphs/active")

        status, output=commands.getstatusoutput("ls")
        if status != 0:
            print mntpath + ": components not accessible"
            continue
        
        lsarr = output.split('\n')

        for j in lsarr:
            io_stats_path = ""
            status, output=commands.getstatusoutput("cat "+ j + "/type")
            if output == "debug/io-stats":
                os.chdir(j)
                io_stats_path = os.getcwd()
                break
        if io_stats_path == "": continue

        priv_content = commands.getstatusoutput("cat private")
        if(len(priv_content[1])==0 or priv_content[1] == ""):
            print "Profiling needs to be enabled on volume: " + mntarr[i]["name"]
            mntarr[i] = None
            continue

        xl_name = commands.getstatusoutput("cat name")[1]

        priv_content = priv_content[1].split('\n')

        mntarr[i]["fops"] = {}
        mntarr[i]["data_read"] = {}
        mntarr[i]["data_written"] = {}

        for j in priv_content:

            match = re.search(xl_name+r"\.incremental\.(.*) = (.*),(.*),(.*),(.*),(.*)$",j)
            if(match):
                mntarr[i]["fops"][match.group(1)] = {}
                mntarr[i]["fops"][match.group(1)]["count"] = match.group(2)
                mntarr[i]["fops"][match.group(1)]["latency_sum"] = match.group(3)
                mntarr[i]["fops"][match.group(1)]["min"] = match.group(4)
                mntarr[i]["fops"][match.group(1)]["max"] = match.group(5)
                mntarr[i]["fops"][match.group(1)]["avg"] = match.group(6)  
                

            match = re.search(r"incremental.data_read = (.*)$",j)
            if(match):
                mntarr[i]["data_read"] = match.group(1)
            
            match = re.search(r"incremental.data_written = (.*)$",j)
            if(match):
                mntarr[i]["data_written"] = match.group(1)

parser = OptionParser()

parser.add_option("-j", "--json", action = "store_true", dest = "json", help = "Get extra output in json format", default = False)
parser.add_option("-m", "--mb", action = "store_true", dest = "mb", help = "Get speed in MB/s", default = False)
parser.add_option("-k", "--kb", action = "store_true", dest = "kb", help = "Get speed in KB/s", default = False)
parser.add_option("-i", "--interval", action = "store_true", dest = "interval", help = "Get continuous stats with a fixed interval", default = False)

(options, args) = parser.parse_args()

devcol = []
readcol = []
writecol = []
opcol = []
ropcol = []
wopcol = []

def max_length(arr):
    
    length = -1
    
    for i in xrange(0,len(arr)):
        if(len(str(arr[i])) > length):
            length = len(str(arr[i]))

    return length    

def fill_space(arr):
    
    maxlen = max_length(arr)
    
    for i in xrange(0,len(arr)):
        for j in xrange(0, maxlen - len(str(arr[i]))):
                arr[i] = str(arr[i]) + " "

def calculate():

    devcol.append("Device:")
    if(options.mb == True):
        readcol.append("MB_read/s")
        writecol.append("MB_wrtn/s")
    else:
        readcol.append("kB_read/s")
        writecol.append("kB_wrtn/s")

    opcol.append("ops/s")
    ropcol.append("rops/s")
    wopcol.append("wops/s")


    for i in xrange(0,len(mntarr)):

        if(mntarr[i] == None):
            continue

        devcol.append(mntarr[i]["name"])

        if(len(mntarr[i]["data_read"]) > 0 and int(mntarr[i]["fops"]["READ"]["latency_sum"]) > 0 ):
            r_speed = ((int(mntarr[i]["data_read"])*1000000)/int(mntarr[i]["fops"]["READ"]["latency_sum"]))/1024
        else:
            r_speed = 0

        if(len(mntarr[i]["data_written"]) > 0 and int(mntarr[i]["fops"]["WRITE"]["latency_sum"]) > 0):
            w_speed = ((int(mntarr[i]["data_written"])*1000000)/int(mntarr[i]["fops"]["WRITE"]["latency_sum"]))/1024
        else:
            w_speed = 0

        if(options.mb == True):
            r_speed = float(r_speed)/1024
            w_speed = float(w_speed)/1024

        readcol.append(r_speed)
        writecol.append(w_speed)

        count = 0
        total_latency = 0

        for j in mntarr[i]["fops"]:
            count = count + int(mntarr[i]["fops"][j]["count"])
            total_latency = total_latency + int(mntarr[i]["fops"][j]["latency_sum"])

        if(count == 0 or total_latency == 0):
            opcol.append("0")
        else:
            opcol.append((count*1000000)/total_latency)

        readcount = int(mntarr[i]["fops"]["READ"]["count"])
        readlatency = int(mntarr[i]["fops"]["READ"]["latency_sum"])

        writecount = int(mntarr[i]["fops"]["WRITE"]["count"])
        writelatency = int(mntarr[i]["fops"]["WRITE"]["latency_sum"])

        if(readcount == 0 or readlatency == 0):
            ropcol.append("0")
        else:
            ropcol.append((readcount * 1000000) / readlatency)

        if(writecount == 0 or writelatency == 0):
            wopcol.append("0")
        else:
            wopcol.append((writecount * 1000000) / writelatency)

dev = ""
read_meta(dev)

if(options.json == True):
    print json.dumps(mntarr)

elif(options.interval):
    pre = []

    while True:
        mntarr = []
        read_meta(dev)

        devcol = []
        readcol = []
        writecol = []
        opcol = []
        ropcol = []
        wopcol = []

        print ""

        if(len(pre) > 0 and len(pre)==len(mntarr)):
            
            for i in xrange(0,len(pre)):

                if(mntarr[i] == None):
                    continue
                
                if(mntarr[i]["mount_path"] != pre[i]["mount_path"]):
                    pass

                devcol.append(mntarr[i]["name"])

                pre_value = int(pre[i]["data_read"])
                cur_value = int(mntarr[i]["data_read"])
                pre_time = int(pre[i]["fops"]["READ"]["latency_sum"])
                cur_time = int(mntarr[i]["fops"]["READ"]["latency_sum"])

                if(cur_time - pre_time != 0 ):
                    readcol.append((((cur_value - pre_value)*1000000)/(cur_time - pre_time))/1024)
                else:
                    readcol.append("0")

                pre_value = int(pre[i]["data_written"])
                cur_value = int(mntarr[i]["data_written"])
                pre_time = int(pre[i]["fops"]["WRITE"]["latency_sum"])
                cur_time = int(mntarr[i]["fops"]["WRITE"]["latency_sum"])

                if(cur_time - pre_time != 0 ):
                    writecol.append((((cur_value - pre_value)*1000000)/(cur_time - pre_time))/1024)
                else:
                    writecol.append("0")

                pre_count = 0
                pre_total_latency = 0
                cur_count = 0
                cur_total_latency = 0

                for j in mntarr[i]["fops"]:
                    cur_count = cur_count + int(mntarr[i]["fops"][j]["count"])
                    cur_total_latency = cur_total_latency + int(mntarr[i]["fops"][j]["latency_sum"])
                    pre_count = pre_count + int(pre[i]["fops"][j]["count"])
                    pre_total_latency = pre_total_latency + int(pre[i]["fops"][j]["latency_sum"])

                if(cur_total_latency - pre_total_latency != 0):
                    opcol.append(((cur_count - pre_count)*1000000)/(cur_total_latency - pre_total_latency))
                else:
                    opcol.append("0")

                pre_readcount = int(pre[i]["fops"]["READ"]["count"])
                pre_readlatency = int(pre[i]["fops"]["READ"]["latency_sum"])
                cur_readcount = int(mntarr[i]["fops"]["READ"]["count"])
                cur_readlatency = int(mntarr[i]["fops"]["READ"]["latency_sum"])

                pre_writecount = int(pre[i]["fops"]["WRITE"]["count"])
                pre_writelatency = int(pre[i]["fops"]["WRITE"]["latency_sum"])
                cur_writecount = int(mntarr[i]["fops"]["WRITE"]["count"])
                cur_writelatency = int(mntarr[i]["fops"]["WRITE"]["latency_sum"])

                if(cur_readlatency - pre_readlatency != 0):
                    ropcol.append(((cur_readcount-pre_readcount)*1000000)/(cur_readlatency - pre_readlatency))
                else:
                    ropcol.append("0")
                    

                if(cur_writelatency - pre_writelatency != 0):
                    wopcol.append(((cur_writecount-pre_writecount)*1000000)/(cur_writelatency - pre_writelatency))
                else:
                    wopcol.append("0")                   

            fill_space(readcol)
            fill_space(writecol)
            fill_space(opcol)
            fill_space(ropcol)
            fill_space(wopcol)

            for i in xrange(0,len(devcol)):
                sys.stdout.write(str(devcol[i]) + "\t" + str(readcol[i]) + "\t" + str(writecol[i]))
                sys.stdout.write("\t" + str(opcol[i]) + "\t" + str(ropcol[i]) + "\t" + str(wopcol[i]) + "\n")

        pre = copy.deepcopy(mntarr)
        time.sleep(1)

else:
   
    if(len(mntarr) == 0):
        print "No gluster mounts found."
        exit(1)

    calculate()
    
    fill_space(devcol)
    fill_space(readcol)
    fill_space(writecol)
    fill_space(opcol)
    fill_space(ropcol)
    fill_space(wopcol)

    for i in xrange(0,len(devcol)):
        sys.stdout.write(str(devcol[i]) + "\t" + str(readcol[i]) + "\t" + str(writecol[i]))
        sys.stdout.write("\t" + str(opcol[i]) + "\t" + str(ropcol[i]) + "\t" + str(wopcol[i]) + "\n")