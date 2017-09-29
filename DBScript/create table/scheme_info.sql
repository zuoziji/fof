drop table scheme_info;

CREATE TABLE `scheme_info` (
   `scheme_name` varchar(100) NOT NULL COMMENT '方案名称',
   `scheme_setupdate` datetime NOT NULL COMMENT '建立日期',
   `create_user` int(11) NOT NULL COMMENT '创建人id',
   `scheme_id` int(11) NOT NULL AUTO_INCREMENT,
   `wind_code_p` varchar(20) DEFAULT NULL COMMENT '如果是从FOF基金建立的，则记录来源FOF的wind_code',
   `backtest_period_start` date DEFAULT NULL COMMENT '回测起始日期',
   `backtest_period_end` date DEFAULT NULL,
   `return_rate` double DEFAULT NULL COMMENT '区间收益率',
   `calmar_ratio` double DEFAULT NULL COMMENT '卡马比率',
   `MDD` double DEFAULT NULL COMMENT '最大回撤',
   `CAGR` double DEFAULT NULL COMMENT '复合年华收益率',
   `ann_volatility` double DEFAULT NULL COMMENT '年化波动率',
   `ann_downside_volatility` double DEFAULT NULL COMMENT '年化下行波动率',
   `nav_min` double DEFAULT NULL COMMENT '最低净值',
   `max_loss_monthly` double DEFAULT NULL COMMENT '最大月亏损',
   `max_profit_monthly` double DEFAULT NULL COMMENT '最大月收益',
   `final_value` double DEFAULT NULL COMMENT '最终净值',
   `mdd_max_period` double DEFAULT NULL COMMENT '最长不创新高（周）',
   `profit_loss_ratio` double DEFAULT NULL COMMENT '盈亏比',
   `sortino_ratio` double DEFAULT NULL COMMENT '索提诺比率',
   `max_loss_weekly` double DEFAULT NULL COMMENT '统计周期最大亏损',
   `max_profit_weekly` double DEFAULT NULL COMMENT '统计周期最大收益',
   `win_ratio` double DEFAULT NULL COMMENT '周胜率',
   PRIMARY KEY (`scheme_id`)
 ) ENGINE=MyISAM AUTO_INCREMENT=185 DEFAULT CHARSET=utf8;