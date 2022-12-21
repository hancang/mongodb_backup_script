import os
import boto3
from loguru import logger
 
 
BUCKET_NAME = "mongodb_backup"  # 存储桶名称
 
# aws_access_key_id和aws_secret_access_key
CN_S3_AKI = ''
CN_S3_SAK = ''
 
CN_REGION_NAME = 'ap-southeast-1'  # 区域
 
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
 
 
if __name__ == "__main__":
    pass
    # TODO test 查询/上传/下载 
    # 查询
    file_name_list = get_files_list()
    logger.info(f"file_name_list = {file_name_list}")
 
    
    path_local = './rootkey.csv'
    path_s3 = 'rootkey.csv'  # s3路径不存在则自动创建
    upload_files(path_local, path_s3)
 
    # 下载
    # path_s3 = './rootkey.csv'
    # path_local = ''    #自定义下载到本地的位置
    # download_zip(path_s3, path_local)