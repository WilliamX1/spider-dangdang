# coding: UTF-8
import sys
import subprocess
from importlib import reload

reload(sys)

cmd = []
# pip install some module
module = ["requests", "scrapy", "pymysql", "pymongo", "bson"]
for ele in module:
    cmd.append(''.join(["pip install ", ele]))

# Warning: this will overwrite dangdang.csv while certainly will assume lots of time
# cmd1 = "python dangdang.py" 

# run loadDataIntoMongoDB.py which writes table 'goods' and table 'users' into MySQL database 'passthemon'
cmd2 = "python loadDataIntoMySQL.py"
cmd.append(cmd2)

# run loadDataIntoMySQL.py which writes collection 'Images' into MongoDB and will also assume some time
cmd3 = "python loadDataIntoMongoDB.py"
# cmd.append(cmd3)

# connect command list above
cmd = " && ".join(cmd)

# run the commands list above
subprocess.Popen(cmd, shell=True)