# Databricks notebook source exported at Tue, 25 Oct 2016 08:17:26 UTC
# MAGIC %md ## Prepare data mounting for each lab

# COMMAND ----------

ACCESS_KEY_ID = "AKIAJBRYNXGHORDHZB4A"
SECRET_ACCESS_KEY = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF" 

mounts_list = [
{'bucket':'databricks-corp-training/common', 'mount_folder':'/mnt/training'},
{'bucket':'db-wikipedia-readonly-use', 'mount_folder':'/mnt/wikipedia-readonly/'},
]

# COMMAND ----------

for mount_point in mounts_list:
  bucket = mount_point['bucket']
  mount_folder = mount_point['mount_folder']
  try:
    dbutils.fs.ls(mount_folder)
    dbutils.fs.unmount(mount_folder)
  except:
    pass
  finally: #If MOUNT_FOLDER does not exist
    dbutils.fs.mount("s3a://"+ ACCESS_KEY_ID + ":" + SECRET_ACCESS_KEY + "@" + bucket,mount_folder)

# COMMAND ----------

display(dbutils.fs.mounts())
