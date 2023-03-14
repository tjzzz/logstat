#encoding=utf8
import sys
import os
import json
import importlib
import math


from datetime import datetime, timedelta
import logging
logging.basicConfig(level=logging.INFO)   # 默认的Metric用的INFO, 各自metric中可以定义debug显示
import traceback


"""
PWD = os.path.dirname(os.path.abspath("__file__"))    ## 启动该任务在的位置
print(__file__)            # 脚本的相对路径 xxx/run.py
print(os.path.split(os.path.realpath(__file__))[0])    ## 绝对路径
"""
## run.py 在的位置
#run_script_path = '/'.join(sys.argv[0].split('/')[:-1]) + '/'   # 相对路径
run_script_path = os.path.split(os.path.realpath(__file__))[0] + '/'   # 脚本在的路径
PWD = os.path.dirname(os.path.abspath("__file__"))    ## 启动该任务在的位置
logging.info('Logstat dir: ' +  run_script_path)
sys.path.append(run_script_path)

from readers.reader_spark import get_all_readers
from utils import ExpConf
from utils import StreamTask
from utils import check_table_exists, get_split_job_list

import time
import string
import random
import argparse
from src.send_mail import send_mail

os.environ['PYSPARK_PYTHON'] = str(sys.executable)
os.environ['SPARK_CONF_DIR'] = run_script_path + '/conf/hdfs_new'



def prepare_spark_confs(metric_dir_list, driver_memory):
    '''
    准备spark相关的reader; 有spark相关的reader才配置spark
    '''
    from conf.spark_conf import import_spark
    spark = import_spark(driver_memory)
    
    ## 打包
    suffix = ''.join(random.sample(string.ascii_letters + string.digits, 8))
    my_tar = 'logstat_' + suffix  + '.tar.gz'
    os.system('mkdir -p logstat_tmp')

    for metric_dir in metric_dir_list:
        # upload 主要文件 metric_dir, src, readers
        cmd1 = 'cp ' + metric_dir + '/*.py logstat_tmp/'
        os.system(cmd1)
        
    cmd1 = 'cp ' + run_script_path + '/src/*.py logstat_tmp/ ; cp ' + run_script_path + '/readers/*.py logstat_tmp/'
    #logging.info(cmd1)
    os.system(cmd1)
    
    cmd2 = 'cd logstat_tmp/ ; tar -czf ' + my_tar + ' *.py; mv -f ' + my_tar + ' ' + run_script_path
    #logging.info(cmd2)
    os.system(cmd2)
    
    os.system('rm -rf logstat_tmp')
    spark.sparkContext.addPyFile(run_script_path + my_tar)
        
    return spark, my_tar



def run_task(df_dict, exp_conf, task):
    logging.info('start to run ....... day=' + df_dict.get('run_day', '-'))
    ## logging.info('User final exp conf is ' +  str(df_dict))

    if len(exp_conf.spark_reader_list) > 0 and 'fetch_spark' not in exp_conf.spark_reader_list:
        spark = df_dict['spark']
        print('run task:  ', spark, exp_conf.spark_reader_list)
        spark_data_df, spark_df_name_list = get_all_readers(spark, df_dict, exp_conf.spark_reader_list)
        df_dict.update(spark_data_df)
        df_dict.update({'spark_df_name_list': spark_df_name_list})

    logging.info('User final exp conf is ' +  str(df_dict))

    ## 2 metric
    df_out = {}
    fail_flag = 0
    
    # try:
    #     df_out = task.process(df_dict, exp_conf.metric_pars_dict)
    # except Exception as err:
    #     fail_flag = 1
    #     print('Job Fail...... ', err)
    #     traceback.print_exc()      # 定位出错的地方
    df_out = task.process(df_dict, exp_conf.metric_pars_dict)
            
    del df_dict

    return df_out, fail_flag


def run(exp_yaml_file, m_stream, user_input_dict):
    """
    input:
        exp_yaml_file: the conf file
        m_stream: the task stream to run 
        user_input_dict: 这里泛指用户传入的参数，可能是exp_yaml解析的也可能是直接调用run传入的其他info
    """

    ## 1. exp 配置信息更新
    ## 优先级: exp_yaml < Common < user_input
    df_dict = {}
    exp_conf = ExpConf(exp_yaml_file, m_stream)
    df_dict.update(exp_conf.conf)                       # conf里的一些公用参数
    df_dict.update(exp_conf.common_metric_pars_dict)    # stream增加一些公用的参数

    df_dict.update(user_input_dict)     ### !!!!!

    df_dict['LOGSTAT_DIR'] = run_script_path    # 加入当前logstat的根目录
    df_dict.update(user_input_dict)                     # 更新用户外来传入的参数，user_input_dict可能是解析的exp.yaml也可能是直接调用run传进来的用户参数


    ## new generate的新参数， run_day, brand_city_site
    if 'brand_city_site' not in df_dict and 'brand' in df_dict:
        df_dict['brand_city_site'] = df_dict['brand'] + '_' + df_dict['city'] + '_' + df_dict['site']

    if 'run_day' not in df_dict:
        start_dt = df_dict.get('start_dt', 'day1')
        end_dt = df_dict.get('end_dt', 'day2')
        if start_dt == end_dt:
            run_day = start_dt
        else:
            run_day = start_dt + '_' + end_dt

        df_dict['run_day'] = run_day
        

    # 2.check spark 日志是否ready
    #check_res = check_table_exists(exp_conf.spark_reader_list, df_dict['brand_city_site'], df_dict['run_day'])
    check_res = 0
    if check_res != 0:
        print('log not ready')
        send_mail(df_dict['brand_city_site'], df_dict['run_day'] + ' fail')
        return

    # 3 初始化
    driver_memory = user_input_dict.get('driver', '5g')
    print('run-driver_memory', driver_memory)

    task = StreamTask(exp_conf.metric_dir_list, exp_conf.metric_list)
    if len(exp_conf.spark_reader_list) > 0:
        spark, my_tar = prepare_spark_confs(exp_conf.metric_dir_list, driver_memory)
        df_dict.update({'spark': spark})        # 后续可以公用同一个spark

    ## 4. run 两种run任务的方式: （1）多天一起跑-默认   (2) 单天串联跑
    if int(df_dict.get('run_days', 0)) > 0:
        job_list = get_split_job_list(start_dt, end_dt)
        print('run_串行 tasks.....')
        for job in job_list:
            st, et = job
            #df_dict['run_day'] = day
            df_dict['start_dt'] = st
            df_dict['end_dt'] = et
            df_dict['run_day'] = st + '_' + et

            df_out, fail_flag  = run_task(df_dict, exp_conf, task)
    else:
        df_out, fail_flag = run_task(df_dict, exp_conf, task)


    # 删除metric tar包
    if len(exp_conf.spark_reader_list) > 0:
        # print('delete ' + my_tar + ' ' + run_script_path)
        spark.stop()
        del spark
        os.system('rm -rf ' + run_script_path + my_tar)

    return df_out

        
def parse_args():
    """
    解析传入的参数，对于不同的项目传入的参数可能不同,若没指定可以在exp.yaml中进行配置，或者使用-set 通用模式
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", help="指定实验exp的配置文件exp.yaml,可参照exp/exp.yaml", required=True)
    parser.add_argument("-m", help="指定要运行的是哪个metric模块", required=True)
    
    parser.add_argument("-run_day", help="指定运行的日期，格式是2019-08-30或者20190830", type=str)
    parser.add_argument("-brand", help="brand", type=str)
    parser.add_argument("-city", help="city", type=str)
    parser.add_argument("-site", help="site", type=str)
    parser.add_argument("-set", help="用户在run的时候指定的其他参数-set day:xx,job_name:xx,brand:xx,city:xx,")
    args = parser.parse_args()

    user_input_dict = {}
    tmp_info = vars(args)
    for key in tmp_info:
        if tmp_info[key] is not None:
            if key == 'set':
                kv_list = tmp_info['set'].split(',')
                for kv in kv_list:
                    k, v = kv.split(':', 1)   # 有可能是key:23:00:00,默认第一个是key
                    user_input_dict[k] = v
            else:
                user_input_dict[key] = tmp_info[key]

    
    return user_input_dict


if __name__ == '__main__':
    
    t0 = time.time()
    user_input_dict = parse_args()      # exp_conf

    exp_yaml_file = user_input_dict['e']
    m_stream = user_input_dict['m']
    df_out = run(exp_yaml_file, m_stream, user_input_dict)
    
    logging.info('run time ' + str(time.time() - t0))
