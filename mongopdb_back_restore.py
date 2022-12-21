import os
import boto3
from loguru import logge
import fun
import time
import datetime
import config
import json
import lock_check
import logger


# aws_access_key_id和aws_secret_access_key
CN_S3_AKI = ''
CN_S3_SAK = ''
CN_REGION_NAME = 'ap-southeast-1'  # 区域
BUCKET_NAME = "s3://de-rt-warehouse-prod/mongodb_backup"  # 存储桶名称
path_s3="s3://de-rt-warehouse-prod/mongodb_backup/"
todat_datatime=datetime.date.today()
# s3 实例
s3 = boto3.client('s3', region_name=CN_REGION_NAME,
                  aws_access_key_id=CN_S3_AKI, aws_secret_access_key=CN_S3_SAK
                  )
 
def upload_files(path_local, path_s3):
    """
    上传（重复上传会覆盖同名文件）
    :param path_local: 本地路径
    :param path_s3: s3路径
    """
    logger.info(f'Start upload files.')
 
    if not upload_single_file(path_local, path_s3):
        logger.error(f'Upload files failed.')
 
    logger.info(f'Upload files successful.')
 
 
def upload_single_file(src_local_path, dest_s3_path):
    """
    上传单个文件
    :param src_local_path:
    :param dest_s3_path:
    :return:
    """
    try:
        with open(src_local_path, 'rb') as f:
            s3.upload_fileobj(f, BUCKET_NAME, dest_s3_path)
    except Exception as e:
        logger.error(f'Upload data failed. | src: {src_local_path} | dest: {dest_s3_path} | Exception: {e}')
        return False
    logger.info(f'Uploading file successful. | src: {src_local_path} | dest: {dest_s3_path}')
    return True
 
 
def download_zip(path_s3, path_local):
    """
    下载
    :param path_s3:
    :param path_local:
    :return:
    """
    retry = 0
    while retry < 3:  # 下载异常尝试3次
        logger.info(f'Start downloading files. | path_s3: {path_s3} | path_local: {path_local}')
        try:
            s3.download_file(BUCKET_NAME, path_s3, path_local)
            file_size = os.path.getsize(path_local)
            logger.info(f'Downloading completed. | size: {round(file_size / 1048576, 2)} MB')
            break  # 下载完成后退出重试
        except Exception as e:
            logger.error(f'Download zip failed. | Exception: {e}')
            retry += 1
 
    if retry >= 3:
        logger.error(f'Download zip failed after max retry.')
 
 
def delete_s3_zip(path_s3, file_name=''):
    """
    删除
    :param path_s3:
    :param file_name:
    :return:
    """
    try:
        # copy
        # copy_source = {'Bucket': BUCKET_NAME, 'Key': path_s3}
        # s3.copy_object(CopySource=copy_source, Bucket=BUCKET_NAME, Key='is-zips-cache/' + file_name)
        s3.delete_object(Bucket=BUCKET_NAME, Key=path_s3)
    except Exception as e:
        logger.error(f'Delete s3 file failed. | Exception: {e}')
    logger.info(f'Delete s3 file Successful. | path_s3 = {path_s3}')
 
 
def batch_delete_s3(delete_key_list):
    """
    批量删除
    :param delete_key_list: [
                {'Key': "test-01/虎式03的副本.jpeg"},
                {'Key': "test-01/tank001.png"},
            ]
    :return:
    """
    try:
        res = s3.delete_objects(
            Bucket=BUCKET_NAME,
            Delete={'Objects': delete_key_list}
        )
    except Exception as e:
        logger.error(f"Batch delete file failed. | Excepthon: {e}")
    logger.info(f"Batch delete file success. ")
 
 
 
def get_files_list(Prefix=None):
    """
    查询
    :param start_after:
    :return:
    """
    logger.info(f'Start getting files from s3.')
    try:
        if Prefix is not None:
            all_obj = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=Prefix)
            
            # 获取某个对象的head信息
            # obj = s3.head_object(Bucket=BUCKET_NAME, Key=Prefix)
            # logger.info(f"obj = {obj}")
        else:
            all_obj = s3.list_objects_v2(Bucket=BUCKET_NAME)
 
    except Exception as e:
        logger.error(f'Get files list failed. | Exception: {e}')
        return
 
    contents = all_obj.get('Contents')
    logger.info(f"--- contents = {contents}")
    if not contents:
        return
 
    file_name_list = []
    for zip_obj in contents:
        # logger.info(f"zip_obj = {zip_obj}")
        file_size = round(zip_obj['Size'] / 1024 / 1024, 3)  # 大小
        # logger.info(f"file_path = {zip_obj['Key']}")
        # logger.info(f"LastModified = {zip_obj['LastModified']}")
        # logger.info(f"file_size = {file_size} Mb")
        # zip_name = zip_obj['Key'][len(start_after):]
        zip_name = zip_obj['Key']
 
        file_name_list.append(zip_name)
 
    logger.info(f'Get file list successful.')
 
    return file_name_list
 

def get_format_time(secs):
	return time.strftime("%Y%m%d%H%M%S",time.localtime(secs)) 

# increment backup
def do_inc_backup():
	is_need_new_circle=False
	# every n day to full backup
	full_backup_period=config.full_backup_period
	#config.db_one_cycle_backup_pre_name="mongodb_cycle_backup_titan_"
	
	local_cycle_info_path=config.db_backup_root_path+"/"+"mongodb_inc_backup_info.json"
	local_cycle_info_con=fun.read_file(local_cycle_info_path)
	last_circle_backup_time=0
	if local_cycle_info_con:
		local_cycle_info=json.loads(local_cycle_info_con)
		last_circle_backup_time_str=local_cycle_info["last_circle_backup_time"]
		last_circle_backup_time=int(last_circle_backup_time_str)
		db_one_cycle_backup_name=local_cycle_info["last_circle_backup_dir_name"]
	else:
		# not backuped 
		is_need_new_circle=True

	now_time=int(time.time())
	if (now_time-last_circle_backup_time)>60*60*24*full_backup_period:
		# need new circle
		db_one_cycle_backup_name=config.db_one_cycle_backup_pre_name+get_format_time(now_time)
		is_need_new_circle=True
	#else:
	#	db_one_cycle_backup_name=config.db_one_cycle_backup_pre_name+get_format_time(last_circle_backup_time)
	
	start_inc_backup(db_one_cycle_backup_name,last_circle_backup_time)
	
	#save inc_backup_info
	if is_need_new_circle:
		local_last_time_info='{"last_circle_backup_time":"%d","last_circle_backup_dir_name":"%s"}' % (now_time,db_one_cycle_backup_name)
		fun.write_file(local_cycle_info_path,local_last_time_info)


def start_inc_backup(db_one_cycle_backup_name,last_circle_backup_time):
	db_host=config.db_host
	db_port=config.db_port
	db_user=config.db_user
	db_passwd=config.db_passwd
	db_name=config.db_name

	upload_flag=0  # 0:not upload  1:upload full backup file 2: upload inc oplog backup file
	db_backup_root_path=config.db_backup_root_path
	#db_one_cycle_backup_name=r"back_%s" %(time.strftime("%Y%m%d"))
	
	db_backup_path=db_backup_root_path+db_one_cycle_backup_name
	db_full_backup_name="full_backup"
	db_full_backup_path=db_backup_path+"/"+db_full_backup_name
	db_inc_backup_name=r"inc_backup/oplog_%s" %(time.strftime("%Y%m%d%H%M%S"))
	db_inc_backup_path=db_backup_path+"/"+db_inc_backup_name

	local_last_time_file_path=db_backup_path+"/"+"last.json"

	conn=fun.get_pymongo_connect(db_host, db_port, db_user, db_passwd,"admin")
	# server oplog last time
	remote_last_time=fun.get_last_oplog_timestamp(conn,db_name)
	last_time_long=None
	last_time_inc=None
	local_last_time_info=fun.read_file(local_last_time_file_path)
	if local_last_time_info:
		json_local_last_time=json.loads(local_last_time_info)
		last_time_long=json_local_last_time["time"]
		last_time_inc=json_local_last_time["inc"]
		fun.dump_oplog_mongodb(db_host, db_port, db_user, db_passwd, db_name, db_inc_backup_path,last_time_long,last_time_inc)
		db_inc_oplog_rs_path=db_inc_backup_path+"/local/oplog.rs.bson"
		if not fun.is_file_not_empty(db_inc_oplog_rs_path): #no new oplog record
			fun.del_dir_or_file(db_inc_backup_path)
		else:
			inc_oplog_rs_mv_to_path=db_inc_backup_path+"/oplog.bson"
			fun.move_file(db_inc_oplog_rs_path,inc_oplog_rs_mv_to_path)
			fun.del_dir_or_file(db_inc_backup_path+"/local/")
			upload_flag=2
	else:
		#full backup
		fun.backup_full_mongodb(db_host,db_port, db_user, db_passwd, db_name, db_full_backup_path)
		upload_flag=1
	local_last_time_info='{"time":"%d","inc":"%d"}' % (remote_last_time.time, remote_last_time.inc)
	fun.write_file(local_last_time_file_path,local_last_time_info)
	if config.is_upload_to_oss!=0 and upload_flag!=0:
		if upload_flag==2:
			print("upload inc oplog backup file...")			
			db_zip_to_backup_path=db_inc_backup_path
			db_backup_zip_name=db_one_cycle_backup_name+"/"+db_inc_backup_name+config.compress_suffix			
			db_backup_zip_path=db_backup_root_path+"/"+db_backup_zip_name
			inc_back_path_s3=path_s3+"/"+todat_datatime+"/mogodb_inc_back/"
			upload_files(db_backup_zip_path,inc_back_path_s3)
			
		# upload full backup file and del last backup direactory
		if upload_flag==1:
			print("upload full db backup file...")
			#full db upload
			db_zip_to_backup_path=db_full_backup_path
			db_backup_zip_name=db_one_cycle_backup_name+"/full_backup"+config.compress_suffix		
			db_backup_zip_path=db_backup_root_path+"/"+db_backup_zip_name
			upload_files(db_zip_to_backup_path,db_backup_zip_name,db_backup_zip_path)
			if last_circle_backup_time!=0:
				#del local last circle's oplog inc files
				last_cycle_backup_name=config.db_one_cycle_backup_pre_name+get_format_time(last_circle_backup_time)
				last_db_backup_path=db_backup_root_path+last_cycle_backup_name
				fun.del_dir_or_file(last_db_backup_path)
				
		
def do_full_backup():
	db_backup_dir=config.db_backup_root_path+config.db_backup_dir_name;
	## backup mongodb to local
	fun.backup_mongodb(config.db_host, config.db_user, config.db_passwd, config.db_name, db_backup_dir)
	if config.is_upload_to_oss!=0:
		db_backup_zip_name=config.db_backup_dir_name+config.compress_suffix;
		db_backup_zip_path=config.db_backup_root_path+db_backup_zip_name;
		full_back_path_s3=path_s3+"/"+todat_datatime+"/mogodb_inc_back/"
		upload_files(db_backup_zip_path,full_back_path_s3)


def restore_full_mongodb(db_host, db_port,db_user, db_passwd, db_name, db_restore_path,is_drop=False,mongo_shell_path=""):
	start_time = time.time()
	add_comm=""
	if is_drop:
		add_comm=" --drop "
	if db_user:
		add_comm=add_comm+" -u %s -p %s --authenticationDatabase admin " % (db_user,db_passwd)
	if db_name:
		add_comm=add_comm+" -d %s " % (db_name)
	cmd="%smongorestore -h %s --port %s  %s %s" %(mongo_shell_path,db_host, db_port, add_comm,db_restore_path)
	print(cmd)
	os.system(cmd)
	fun.print_cost_time("restore mongodb", start_time)

def restore_oplog_mongodb(db_host, db_port,db_user, db_passwd, db_restore_path,mongo_shell_path=""):
	start_time = time.time()
	if db_user:
		cmd="%smongorestore --oplogReplay -h %s --port %s -u %s -p %s %s" %(mongo_shell_path,db_host, db_port,db_user, db_passwd, db_restore_path)
		print(cmd)
		os.system(cmd)
	else:
		cmd="%smongorestore --oplogReplay -h %s --port %s %s" %(mongo_shell_path,db_host, db_port, db_restore_path)
		print(cmd)
		os.system(cmd)
	fun.print_cost_time("restore oplog", start_time)
	
if __name__ == "__main__":
	lock=lock_check.LockCheck();
	is_lock=lock.is_lock()
	if not is_lock:
		try:
			lock.lock();
			start_time = time.time()
			print("begin to dump mongodb database ...");
			
			is_inc_backup=config.is_inc_backup
			fun.mongo_shell_path=config.mongo_shell_path
			if is_inc_backup==1:
				do_inc_backup()
			else:
				do_full_backup()
			#time.sleep(10)
			fun.print_cost_time("all done ", start_time)
			
		except Exception, e:
			raise e
		finally:
			lock.release()
	else:    # TODO test 查询/上传/下载 
    # 查询
		print("now is running backup db,exit!")

'''
     file_name_list = get_files_list()
    logger.info(f"file_name_list = {file_name_list}")
 
    
    path_local = './rootkey.csv'
    path_s3 = 'rootkey.csv'  # s3路径不存在则自动创建
    upload_files(path_local, path_s3)
 
    # 下载
    # path_s3 = './rootkey.csv'
    # path_local = ''    #自定义下载到本地的位置
    # download_zip(path_s3, path_local)
 '''
