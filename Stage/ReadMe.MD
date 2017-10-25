# Start celery
> celery -A celery_worker:celery  worker --loglevel=INFO 

# Start beat  
> celery -A celery_worker:celery beat --loglevel=INFO 

# R Package
> rjson,bootstrap,fGrch,fArma,RMySQL,xts,fPortfolio

# Start app 
> gunicorn --worker-class eventlet -w 1 wsgi:wsgi_app --bind 0.0.0.0:5000

> service nginx restart

## R Library dependency
* apt-get install glpk-utils libglpk-dev glpk-doc python-glpk 
* apt-get install r-cran-rsymphony
* apt-get install coinor-libsymphony-dev coinor-libsymphony-doc libxml2-dev



## Redis DB index 
* 0 Celery BACKEND & Result 
* 1 Testing Result 
* 2 
* 3 user fof list cache 
* 4  
* 5 Celery task running status channel

# Finished work
### 用户管理
* 创建用户和删除用户
* 自定义用户权限,可使用的功能,可访问的基金等
* 支持用户自助重置密码

### 后台管理
*  数据库模型管理
*  定时自动执行更新数据
*  手动执行更新数据和压力测试
*  异步任务执行状态可视化
*  数据更新结果以邮件方式通知管理员

### 创建FOF
*  手动创建新的FOF组合
*  根据BL模型智能创建新的FOF组合
*  针对已创建的新组合已异步的方式运行压力测试
*  已完成的压力测试结果邮件通知用户,对于失败的压力测试同时通知管理员和用户
*  压力测试结果浏览和删除

### 已存在的FOF
* 浏览组合中所有基金的净值,当前持仓,调仓记录,策略,底层资产等信息
* 添加,修改和删除基金的净值
* 添加子基金批次
* 修改当前持仓状态
* 基金相关的文件上传,下载和删除
* 对当前的持仓方案进行可视化分析
* 浏览,添加和删除基金相关的日程
* 浏览,添加和删除用户相关的日程

### 投顾管理
* 添加新的投顾
* 浏览和修改投顾的基本信息
* 浏览上传和下载投顾的评估报告
* 浏览上传和下载投顾相关的文件
* 修改投顾相关的产品的级别

### 市场回顾
* 浏览各种策略在用户选择的时间区间内走势

----

### 2017/10/10
* 基金的业绩跟踪，盈亏分析，业绩归因，风险分析
### 2017/10/20
* 基金详情折线图添加图例开关,默认只显示基金和沪深300,X轴移除label
* 历史回顾中，优化策略分位数统计方法
* 股票tick数据保存程序（暂时不加入到任务列表中）
* 修复 fund_nav 数据导入异常问题
### 2017/10/23
* 增加一种新的策略指数计算方法

### 2017/10/25
* 增加 data_handler.stat_fund 基金绩效计算函数
