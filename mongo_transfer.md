

mongo2��mongo3Ǩ��ͬ��
---


1.��¼��ǰoplogʱ���
---

(1)��syn_replay_oplog.py���޸����ã��������úã�
```
### config

## mongo2�����ݿ⣨Դ���ݿ⣩�����ã����ڴӴ˴�����oplog��¼,������second������
src_db_host="127.0.0.1"
src_db_port=27018
# �û���Ҫ�в�ѯlocal���µ�oplog.rs��Ȩ�ޣ�һ����admin���backup��ɫ
src_db_user="testb"
src_db_passwd="testb" 
src_db_name="che"


##   mongo3�����ݿ⣨Ŀ�����ݿ⣩�����ã�����ִ��oplog��¼, ��Ҫprimary����
target_db_host="127.0.0.1"
target_db_port=27017
target_db_user="tests"
target_db_passwd="tests" 
target_db_name="che"

# ���ݻָ�oplog����ʱĿ¼
restore_local_temp_path="H:\\pythoncode\\temp\\oplog_temp\\"

# ���ʹ��mongo�ͻ�����ɫ��ģ�д��mongo�ͻ��˵ľ���·��
mongo_shell_path=""
```
(2) ִ��syn_replay_oplog.py����restore_local_temp_path��Ŀ¼�»�����last.json�ļ�����¼�˵�ǰoplogʱ���


2. ȫ������mongo2�����ݿ�����
---
��1���޸�config.properties���ã��������úã�
```
## ������oss �������ã��˴����ϴ�oss��û�ã�
endpoint= oss.aliyuncs.com
accessKeyId = xxxxxxx
accessKeySecret = xxxxxxx
bucket = db-backup
## mongo2��Դ���ݿ⣩ ��������
# �����ôӿ�ĵ�ַ�����ٶ�����ѹ��
db_host= localhost
db_port= 27017
# ��Ҫ��Ӧ���ݿ�Ȩ�޵��û�
db_user= testb
db_passwd= testb
db_name= che
# ���ݵ����ص���ʱĿ¼ 
db_backup_root_path= /temp/titan_full_temp_backup/
# ���ʹ��mongo�ͻ�����ɫ��ģ�д��mongo�ͻ��˵ľ���·��
mongo_shell_path= /alidata1/dev/hanxuetong/mongodb/mongodb-linux-x86_64-3.0.6/bin/
# �������ݻ���ȫ������ 1: ��������  0:ȫ������ ������ȫ������ѡ0��
is_inc_backup=0
# ÿ���������һ��ȫ������(����û��)
full_backup_period=7
# �Ƿ��ϴ���oss����� 1 ���ϴ��ɹ����ɾ�����ر����ļ���0:���ϴ���oss�����ﲻ�ϴ�oss��ѡ0��
is_upload_to_oss= 0
```
��2��ִ��start.pyȫ�����ݣ����ݺ�db_backup_root_path·���»��б�������



3. ���ղű��ݵ�ȫ������mongo3���ݿ�
---
��1����restore_full.py���޸����ã��������úã�
```
## ������oss ���ã����ﲻ��oos���أ�û�ã�
endpoint="oss.aliyuncs.com"
accessKeyId="xxxxxxx"
accessKeySecret="xxxxxxx"
bucket="db-backup"

## mongodb3��Ŀ��⣩ ���������
db_host="localhost"
db_port=27017
# ���ݿ��Ӧ���û�
db_user="test"
db_passwd="test" 
db_name="che"

#  recent circle backup direactory on oss ���±����ļ����������� (����2��Դ���ݿⱸ�ݵ����ƣ���mongodb_backup_titan_201512242039)
last_circle_backup_dir_name="mongodb_cycle_backup_titan_20151124141133"

#  ���˴����ã�
last_full_backup_file_suffix=".tar.gz"
#  ���ݻָ���Ŀ¼��ʵ��ȫ�����ݵ�·��Ϊ restore_local_temp_path+last_circle_backup_dir_name+db_name   ���˴��ָ���·��Ϊ����2��Դ���ݿⱸ�ݵ�·����db_backup_root_path��
restore_local_temp_path="H:\\pythoncode\\temp\\restore\\"
#  ���ʹ��mongo�ͻ�����ɫ��ģ�д��mongo�ͻ��˵ľ���·��
mongo_shell_path="/alidata1/dev/hanxuetong/mongodb/mongodb-linux-x86_64-3.0.6/bin/"
# backup file has download to local ? if True,will not download backup files from oss
# �Ƿ񱸷��ļ��Ѿ����ص����أ����true���򲻻��oss���غͽ�ѹ���������� �������أ���ΪTrue��
has_download_to_local=True
# �ָ�ʱ�Ƿ���ɾ���ɵ����ݿ� ����ɾ��ԭ���ݿ⣬��ΪTrue��
is_drop_old_restore=True  
```

��2��ִ��restore_full.pyȫ���ָ���mongo3��Ŀ�����ݿ⣩


4.�ظ�������syn_replay_oplog.py����mongo3��Ŀ�����ݿ⣩����������ͬ��
---

5.������վ,��ʼ˫д
---

6. ����syn_replay_oplog.py����ͬ��һ��
---

























































