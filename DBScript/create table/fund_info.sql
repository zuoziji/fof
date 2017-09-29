drop table if exists fund_info;

/*==============================================================*/
/* Table: fund_info                                             */
/*==============================================================*/
CREATE TABLE `fund_info` (
   `wind_code` varchar(20) NOT NULL COMMENT '基金代码',
   `sec_name` varchar(100) DEFAULT NULL COMMENT '基金名称',
   `strategy_type` varchar(50) DEFAULT NULL COMMENT '策略类型。\n            如果fund_strategy表对应为空，则使用该数值',
   `fund_setupdate` date DEFAULT NULL COMMENT '基金成立日',
   `fund_maturitydate` date DEFAULT NULL COMMENT '基金终止日',
   `fund_mgrcomp` varchar(200) DEFAULT NULL COMMENT '基金经理',
   `fund_existingyear` double DEFAULT NULL COMMENT '存在年数',
   `fund_ptmyear` double DEFAULT NULL COMMENT '存续年限',
   `fund_type` varchar(50) DEFAULT NULL COMMENT '基金类型',
   `fund_fundmanager` varchar(200) DEFAULT NULL COMMENT '基金管理人信息',
   `fund_status` int(11) DEFAULT NULL COMMENT '基金状态 0 未运行；1 运行',
   `alias` varchar(50) DEFAULT NULL COMMENT '别名',
   `scale_tot` double DEFAULT NULL COMMENT '总规模',
   `scale_a` double DEFAULT NULL COMMENT 'A类份额规模（募集端的份额），不是结构化份额',
   `scale_b` double DEFAULT NULL COMMENT 'B类份额规模（募集端的份额），不是结构化份额',
   `priority_asset` double DEFAULT NULL COMMENT '优先级资产规模',
   `inferior_asset` double DEFAULT NULL COMMENT '劣后级资产规模',
   `priority_interest_rate` double DEFAULT NULL COMMENT '优先级年华收益率',
   `source_mark` int(11) DEFAULT NULL COMMENT '数据源 0：wind；1：手动',
   `rank` int(11) DEFAULT NULL COMMENT '基金评级信息 默认0 未评级；1 不关注；2 观察；3 备选；4 核心池',
   `annual_return` double DEFAULT NULL COMMENT '年化收益率',
   `nav_acc_mdd` double DEFAULT NULL COMMENT '最大回撤率',
   `sharpe` double DEFAULT NULL COMMENT '夏普比率',
   `nav_acc_latest` double DEFAULT NULL COMMENT '最新净值',
   `nav_date_latest` date DEFAULT NULL COMMENT '最新净值日期',
   `trade_date_latest` date DEFAULT NULL COMMENT '最新交易日，用于每次wind_fund_nav表数据更新起始日期参考',
   `fh_inv_manager` varchar(20) DEFAULT NULL COMMENT '投资负责人',
   `fh_prod_manager` varchar(20) DEFAULT NULL COMMENT '产品负责人',
   `fh_channel_manager` varchar(20) DEFAULT NULL COMMENT '渠道负责人',
   `belongtofh` int(11) DEFAULT '0' COMMENT '0 其他公司基金; 1 复华旗下的基金',
   PRIMARY KEY (`wind_code`)
 ) ENGINE=MyISAM;