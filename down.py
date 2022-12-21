#!/usr/bin/env
import fun
import time
import config
import properties_util

if __name__ == "__main__":
	start_time = time.time()
	
	print("begin ");
	db_backup_dir=config.db_backup_root_path+config.db_backup_dir_name;

	oss=fun.get_oss_connect(config.endpoint, config.accessKeyId, config.accessKeySecret);

	
	db_backup_zip="/data/mongodb_backup/mongodb_backup_realtag.zip"
	db_backup_zip_name="mongodb_backup_realtag.zip"

	fun.download_zip(oss,config.bucket,db_backup_zip,db_backup_zip_name)
	
	fun.print_cost_time("all done ", start_time)
	fun.list_bucket_files(oss,config.bucket)

	


