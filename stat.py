# glusterfsiostat - A client side tool to gather I/O stats from every Gluster mount.
# Author : Vipul Nayyar <nayyar_vipul@yahoo.com>

import commands
import re
import os
import json
import sys
from optparse import OptionParser

status, output=commands.getstatusoutput('mount')
mountlines = output.split("\n")

if status != 0:
	print "Unable to gather mount statistics"
	exit(1)

mntarr = []

for i in mountlines:
	matchobj = re.search(r" type fuse.glusterfs \(.*\)$",i)
	if matchobj:
		i = i.replace(matchobj.group(),"")
		i = i.split(" on ")
		mntname = i[0]
		mntpath = i[1]
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

	priv_content = priv_content[1].split('\n')

	mntarr[i]["read_speed"] = {}
	mntarr[i]["write_speed"] = {}

	for j in priv_content:

		match = re.search(r"write_speed\((.*)\) = (.*)$",j)
		if(match):
			mntarr[i]["read_speed"][match.group(1)] = match.group(2)
		
		match = re.search(r"read_speed\((.*)\) = (.*)$",j)
		if(match):
			mntarr[i]["write_speed"][match.group(1)] = match.group(2)
		
		match = re.search(r"data_read_cumulative = (.*)$",j)
		if(match):
			mntarr[i]["read_cumulative"] = match.group(1)
		
		match = re.search(r"data_read_incremental = (.*)$",j)
		if(match):
			mntarr[i]["read_incremental"] = match.group(1)

		match = re.search(r"data_written_cumulative = (.*)$",j)
		if(match):
			mntarr[i]["write_cumulative"] = match.group(1)

		match = re.search(r"data_written_incremental = (.*)$",j)
		if(match):
			mntarr[i]["write_incremental"] = match.group(1)

parser = OptionParser()

parser.add_option("-j", "--json", action = "store_true", dest = "json", help = "Get extra output in json format", default = False)

(options, args) = parser.parse_args()

if(len(mntarr) == 0):
	print "No gluster mounts found."
	exit(1)

if(options.json == True):
	print json.dumps(mntarr)
else:
	sys.stdout.write("Device:")
	length = len(mntarr[i]["name"]) - 7
	if(length > 0):
		for x in xrange(0,length):
			sys.stdout.write(" ")

	sys.stdout.write("    kB_read/s    kB_wrtn/s    kB_read    kB_wrtn\n")
	for i in xrange(0,len(mntarr)):
		if(len(mntarr[i]["read_speed"]) > 0):
			r_speed = int(mntarr[i]["read_speed"][max(mntarr[i]["read_speed"].iterkeys())])/1024
		else:
			r_speed = 0

		if(len(mntarr[i]["write_speed"]) > 0):
			w_speed = int(mntarr[i]["write_speed"][max(mntarr[i]["write_speed"].iterkeys())])/1024
		else:
			w_speed = 0

		sys.stdout.write(str(mntarr[i]["name"])+"    " + str(r_speed) + "    " + str(w_speed) + "    ")
		sys.stdout.write(str(int(mntarr[i]["read_cumulative"])/1024) + "    ")
		sys.stdout.write(str(int(mntarr[i]["write_cumulative"])/1024))
		sys.stdout.write("\n")
