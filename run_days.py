#encoding=utf8
import sys
import os
import datetime
from src.tools import get_days


if __name__ == "__main__":
    

    day0 = sys.argv[1]      # 2019-12-01 格式
    day1 = sys.argv[2]
    day_list = get_days(day0, day1)
    
    exp_yaml = 'exp/ndj.yaml'
    stream = 'bakup_data'
    for day in day_list:
        print(day)
        cmd = 'python /home/zzzheng/recommandation/logstat_ml/run.py -e ' + exp_yaml + ' -m ' + stream + ' -run_day ' + day
        os.system(cmd)

