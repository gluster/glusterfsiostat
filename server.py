#!/usr/bin/python
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from os import curdir, sep

PORT_NUMBER = 8080


import commands
import re
import os
import json
import sys
import time
import copy
from thread import *
from optparse import OptionParser

statarr = []
statlock = 0

def read_meta(dev):
    
    mntarr = []
    
    status, output=commands.getstatusoutput('mount')
    mountlines = output.split("\n")

    if status != 0:
        print "Unable to gather mount statistics"
        exit(1)

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

    return mntarr

def statsthread():
    
    pre = []
    global statlock

    while True:
        mntarr = read_meta("")

        if(len(pre) > 0):
            
            for i in xrange(0,len(mntarr)):
                
                if(mntarr[i]["mount_path"] != pre[i]["mount_path"]):
                    pass

                while (statlock) :
                    pass

                statlock = 1

                temp = {}
                temp["time"] = int(time.time())
                temp["mount_path"] = mntarr[i]["mount_path"]
                temp["name"] = mntarr[i]["name"]
                temp["sent"] = 0

                pre_value = int(pre[i]["data_read"])
                cur_value = int(mntarr[i]["data_read"])
                pre_time = int(pre[i]["fops"]["READ"]["latency_sum"])
                cur_time = int(mntarr[i]["fops"]["READ"]["latency_sum"])

                if(cur_time - pre_time != 0 ):
                    temp["r_speed"] = (((cur_value - pre_value)*1000000)/(cur_time - pre_time))/1024
                else:
                    temp["r_speed"] = 0

                pre_value = int(pre[i]["data_written"])
                cur_value = int(mntarr[i]["data_written"])
                pre_time = int(pre[i]["fops"]["WRITE"]["latency_sum"])
                cur_time = int(mntarr[i]["fops"]["WRITE"]["latency_sum"])

                if(cur_time - pre_time != 0 ):
                    temp["w_speed"] = (((cur_value - pre_value)*1000000)/(cur_time - pre_time))/1024
                else:
                    temp["w_speed"] = 0

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
                    temp["ops/s"] = ((cur_count - pre_count)*1000000)/(cur_total_latency - pre_total_latency)
                else:
                    temp["ops/s"] = "0"

                pre_readcount = int(pre[i]["fops"]["READ"]["count"])
                pre_readlatency = int(pre[i]["fops"]["READ"]["latency_sum"])
                cur_readcount = int(mntarr[i]["fops"]["READ"]["count"])
                cur_readlatency = int(mntarr[i]["fops"]["READ"]["latency_sum"])

                pre_writecount = int(pre[i]["fops"]["WRITE"]["count"])
                pre_writelatency = int(pre[i]["fops"]["WRITE"]["latency_sum"])
                cur_writecount = int(mntarr[i]["fops"]["WRITE"]["count"])
                cur_writelatency = int(mntarr[i]["fops"]["WRITE"]["latency_sum"])

                if(cur_readlatency - pre_readlatency != 0):
                    temp["rops/s"] = ((cur_readcount-pre_readcount)*1000000)/(cur_readlatency - pre_readlatency)
                else:
                    temp["rops/s"] = "0"
                    

                if(cur_writelatency - pre_writelatency != 0):
                    temp["wops/s"] = ((cur_writecount-pre_writecount)*1000000)/(cur_writelatency - pre_writelatency)
                else:
                    temp["wops/s"] = "0"                   


                statarr.append(temp)
                statlock = 0
           
        pre = copy.deepcopy(mntarr)
        time.sleep(1)

class myHandler(BaseHTTPRequestHandler):
    
    def do_GET(self):
        if self.path=="/":
            self.path="index.html"

        global mntarr
        global statlock

        if self.path=="/data":
            self.send_response(200)
            self.send_header('Content-type',"text/json")
            self.end_headers()

            send = []
            while (statlock) :
                pass

            statlock = 1
            for i in xrange(0,len(statarr)):
                if(statarr[i]["sent"] == 0):
                    send.append(statarr[i])
                    statarr[i]["sent"] = 1

            statlock = 0

            print send

            self.wfile.write(json.dumps(send))
            return

        try:

            sendReply = False
            if self.path.endswith(".html"):
                mimetype='text/html'
                sendReply = True
            if self.path.endswith(".jpg"):
                mimetype='image/jpg'
                sendReply = True
            if self.path.endswith(".gif"):
                mimetype='image/gif'
                sendReply = True
            if self.path.endswith(".js"):
                mimetype='application/javascript'
                sendReply = True
            if self.path.endswith(".css"):
                mimetype='text/css'
                sendReply = True

            if sendReply == True:
                print "vip " + curdir + sep + self.path 
                print os.getcwd()
                f = open(CWD + "/" + self.path)
                print f
                
                self.send_response(200)
                self.send_header('Content-type',mimetype)
                self.end_headers()
                self.wfile.write(f.read())
                f.close()
            return

            print sendReply
        except IOError, e:
            print e
            self.send_error(404,'File Not Found: %s' % self.path)

try:

    server = HTTPServer(('', PORT_NUMBER), myHandler)
    print 'Started httpserver on port ' , PORT_NUMBER

    CWD = os.getcwd()

    start_new_thread(statsthread,())
    
    server.serve_forever()

except KeyboardInterrupt:
    print '^C received, shutting down the web server'
    server.socket.close()