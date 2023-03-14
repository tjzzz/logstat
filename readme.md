##  logstat

基于df的logstat工具

模块化调研流程，支持指标开发、分析模块和model等功能.

wiki: http://wiki.aibee.cn/display/~zzzheng/logstat

当前支持的通用模块:

http://wiki.aibee.cn/pages/viewpage.action?pageId=39743816


## 使用案例

1. 命令行方式 `python run.py -e test.yaml -m test_reader`

默认的参数
```
parser.add_argument("-e", help="指定实验exp的配置文件exp.yaml,可参照exp/exp.yaml", required=True)
parser.add_argument("-m", help="指定要运行的是哪个metric模块", required=True)
parser.add_argument("-d", help="指定运行的日期，格式是2019-08-30或者20190830", type=str)
parser.add_argument("-brand", help="brand", type=str)
parser.add_argument("-city", help="city", type=str)
parser.add_argument("-site", help="site", type=str)
parser.add_argument("-set", help="用户在run的时候指定的其他参数-set day:xx,job_name:xx,brand:xx,city:xx,")
```

2. 界面操作

http://172.16.244.59:8501/




3. 在jupyter中引用

```python
# e.g
from run import run
user_input_dict = {}
df_out = run('exp_jupyter.yaml', 'stream_ana', user_input_dict)
```


4. docker镜像:
registry.aibee.cn/ml/logstat:0.1
可以直接git pull 获取更新


## 结构说明

```
.
├── readers/              #  readers 解析日志
├── metrics/              #  指标计算插件
├── conf/                 #  spark 环境相关配置
├── exp/                  #  exp.yaml 不同实验配置文件
├── streamlitapps/        #  基于streamlit搭建的前端app_ml, app_ba
├── run.py                #  总run
├── utils.py
├── wrapper.sh            # 打包在docker里run

```

## Update log

* 
[ ] 包的懒引用


* 2021-02-19

- 增加mflow.tracking
- examples/ 增加使用案例；飞书说明文档
- exp.yaml中的metric_dir配置改成相对LOGSTAT的路径，即metrics/metrics_xxx 系统会直接认为是logstat根目录下的metric

* 2021-01-31


大量更新 ，代码体系重构，将业务逻辑进行抽象，重构了各个操作模块。正式项目文档 AutoBee https://aibee.feishu.cn/drive/folder/fldcnFDKA33KO3ikGBDaYIAUspc

    - logstat系统: run支持metric的复用，yaml参数设置更灵活
    - 模块组
        - data_generate: 抽象特征提取模块
        - data_preprocess: 数据勘察和特征预处理模块



* 2020-12-16

增加requirements.txt说明，因为积累的功能较多，所以可能实际使用时候用不到这么多的包。

[] 后期需要考虑怎么将包与要实现的功能隔离开.

* 2020-12-07

run的参数中增加 run_days参数，设置是否串联按照天跑，默认不开启直接跑所有

[ ] 工具性插件比如save_data, filter等不设置metric_dir能自动读取

* 2020-11-25 增加streamlit 前端模块 `streamlit/app_ml.py`

* 2020-11-18 readers拆分单独模块

TrackReaders,ExchangeReaders,UserReaders。 用户在exp.yaml中使用不影响，可以使用`fetch_ods_track_v2` 或者是 `track.fetch_ods_track_v2`用来指定特定reader里的，不指定的话会从所有reader里进行匹配

