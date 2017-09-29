drop table scheme_fund_pct;

CREATE TABLE `scheme_fund_pct` (
   `id` int(11) NOT NULL AUTO_INCREMENT,
   `scheme_id` int(11) NOT NULL COMMENT '方案ID',
   `wind_code` varchar(20) NOT NULL COMMENT ' 基金代码',
   `invest_scale` double DEFAULT NULL COMMENT '投资规模',
   PRIMARY KEY (`id`),
   UNIQUE KEY `uk_fof_fund_pct` (`scheme_id`,`wind_code`)
 ) ENGINE=MyISAM AUTO_INCREMENT=24 DEFAULT CHARSET=utf8