drop index uk_fof_fund_pct on fof_fund_pct;

drop table if exists fof_fund_pct;

/*==============================================================*/
/* Table: fof_fund_pct                                          */
/*==============================================================*/
 CREATE TABLE `fof_fund_pct` (
   `id` int(11) NOT NULL AUTO_INCREMENT,
   `wind_code_p` varchar(20) NOT NULL COMMENT '母基金代码',
   `wind_code_s` varchar(20) NOT NULL COMMENT ' 已投基金代码',
   `date_adj` date NOT NULL COMMENT '调仓日期',
   `invest_scale` double DEFAULT NULL COMMENT '投资规模',
   PRIMARY KEY (`id`),
   UNIQUE KEY `uk_fof_fund_pct` (`wind_code_p`,`wind_code_s`,`date_adj`)
 ) ENGINE=MyISAM AUTO_INCREMENT=290 DEFAULT CHARSET=utf8