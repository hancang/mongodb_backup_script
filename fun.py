import zipfile
import shutil
import pymongo
import re
import time
import os
import sys

mongo_shell_path=""
		
### mongodb function


def print_cost_time(msg, begin_time):
	end_time = time.time()
	cost = "%.2f" % (end_time - begin_time)
	print(" %s cost:%s s" % (msg, cost))

# del file or direactory
def del_dir_or_file(filepath):
	if os.path.isfile(filepath):
		os.remove(filepath)
		print(filepath+" removed!")
	elif os.path.isdir(filepath):  
		shutil.rmtree(filepath,True)
		print("direactory "+filepath+" removed!" )
"""	
# compress direactory to zip
def zip_files(path,zipfilename):
	start_time = time.time()
	if not os.path.isdir(path):
		print path + ' No such a direactory'
	else:
		print 'Create new file ' + zipfilename
		zipfp = zipfile.ZipFile(zipfilename, 'w' ,zipfile.ZIP_DEFLATED)
		for dirpath, dirnames, filenames_ in os.walk(path, True):
			for filaname in filenames_:
				print "zip dir "+dirpath
				direactory = os.path.join(dirpath,filaname)
				print 'Compress... ' + direactory
				zipfp.write(direactory)
		# Flush and Close zipfilename at last. 
		zipfp.close()
	print_cost_time("zip files", start_time)
"""		

# compress direactory to zip
def zip_files(dirPath, zipFilePath=None, includeDirInZip=True):
	start_time = time.time();
	if not zipFilePath:
		zipFilePath = dirPath + ".zip";
	if not os.path.isdir(dirPath):
		raise OSError("dirPath "+dirPath+" does not.");
	parentDir, dirToZip = os.path.split(dirPath)
	#Little nested function to prepare the proper archive path
	def trimPath(path):
		archivePath = path.replace(parentDir, "", 1)
		#if parentDir:
		#	archivePath = archivePath.replace(os.path.sep, "", 1)
		if not includeDirInZip:
			archivePath = archivePath.replace(dirToZip + os.path.sep, "", 1);
		return os.path.normcase(archivePath)
	outFile = zipfile.ZipFile(zipFilePath,"w",compression=zipfile.ZIP_DEFLATED,allowZip64=True)
	for(archiveDirPath,dirNames,fileNames) in os.walk(dirPath):
		for fileName in fileNames:
			filePath=os.path.join(archiveDirPath,fileName);
			outFile.write(filePath, trimPath(filePath));
        #Make sure we get empty directories as well
		if not fileNames and not dirNames:
			zipInfo = zipfile.ZipInfo(trimPath(archiveDirPath) + "/")
	outFile.writestr(zipInfo, "");
	outFile.close()
	print_cost_time("zip files", start_time)	

# compress direactory to zip,need linux
def tar_files(dirPath, zipFilePath):
	spath=os.path.split(dirPath)
	parentDirPath=spath[0]
	dirName=spath[1]
	start_time = time.time()
	os.system("tar -C %s -zcvf %s %s"
		%(parentDirPath,zipFilePath,dirName))
	print_cost_time("tar files", start_time)	
	
#backup mongodb to local
def backup_mongodb(db_host, db_user, db_passwd, db_name, db_backup_path):
	start_time = time.time()
	os.system("%smongodump -h %s -u %s -p %s -d %s -o %s"
		%(mongo_shell_path,db_host, db_user, db_passwd, db_name, db_backup_path))
	print_cost_time("backup mongodb", start_time)		


def get_pymongo_connect(db_host, db_port, db_user, db_passwd,db_name):
	client=pymongo.MongoClient(db_host,db_port)
	if db_user:
		client[db_name].authenticate(db_user, db_passwd)
	return client
	
#Return the timestamp of the latest entry in the oplog.
def get_last_oplog_timestamp(conn,db_name):
	start_time = time.time()
	oplog=conn.local.oplog.rs;
	if not db_name:
		curr = oplog.find().sort(
			'$natural', pymongo.DESCENDING
		).limit(1)
	else:
		#{'ns': {'$in': oplog_ns_set}}
		reg="^"+db_name+"\."
		curr = oplog.find(
			{'ns': re.compile(reg)}
		).sort('$natural', pymongo.DESCENDING).limit(1)
	if curr.count(with_limit_and_skip=True) == 0:
		return None
	print_cost_time("get_last_oplog_timestamp ", start_time)	
	return curr[0]['ts']

def backup_full_mongodb(db_host, db_port,db_user, db_passwd, db_name, db_backup_path):
	start_time = time.time()
	add_comm=""
	if db_user:
		add_comm=" -u %s -p %s --authenticationDatabase admin " % (db_user,db_passwd)
	if db_name:
		add_comm=add_comm+" -d %s " % (db_name)
	os.system("%smongodump -h %s --port %s %s -o %s" %(mongo_shell_path,db_host, db_port, add_comm, db_backup_path))
	print_cost_time("backup full mongodb", start_time)	

def dump_oplog_mongodb(db_host,db_port, db_user, db_passwd, db_name, db_backup_path,last_time_long,last_time_inc):
	start_time = time.time()
	add_comm=""
	if db_user:
		add_comm=" -u %s -p %s --authenticationDatabase admin " % (db_user,db_passwd)
	# $gt  $gte
	if not db_name:
		query="{ts:{$gt:{$timestamp:{t:%s,i:%s}}}}" % (last_time_long, last_time_inc)
		if not sys.platform.startswith('win'):
			query="'"+query+"'"
		os.system("%smongodump -h %s --port %s -d local %s -c oplog.rs -q %s -o %s"
		%(mongo_shell_path,db_host, db_port,add_comm,query, db_backup_path))
	else:
		reg="/^"+db_name+"\\\./"
		query="{ts:{$gt:{$timestamp:{t:%s,i:%s}}},ns:%s}" % (last_time_long, last_time_inc ,reg)
		if not sys.platform.startswith('win'):
			query="'"+query+"'"
		comm="%smongodump -h %s --port %s -d local -c oplog.rs %s -q %s -o %s" % (mongo_shell_path,db_host, db_port, add_comm, query, db_backup_path)
		print(comm)
		os.system(comm)
	print_cost_time("dump oplog mongodb ", start_time)		
	
def write_file(file_path,content):
	file_handle  = open(file_path, 'w')
	try:
		file_handle.write ( content) 
	finally:
		file_handle.close()
		
def read_file(file_path):
	if not os.path.exists(file_path):
		return None
	file_handle  = open(file_path)
	try:
		bson_text = file_handle.read( )
	finally:
		file_handle.close()
	if bson_text:
		return bson_text
	return None

def is_file_not_empty(file_path):
	if not os.path.exists(file_path):
		return False
	size=os.path.getsize(file_path)
	if size==0:
		return False
	return True
	
def move_file(old_file_path,new_file_path):
	shutil.move(old_file_path,new_file_path)
	print("move "+old_file_path+" to "+new_file_path)
	
def make_direactory(file_path):
	if os.path.exists(file_path):
		return;
	os.makedirs(file_path)



		

	  





