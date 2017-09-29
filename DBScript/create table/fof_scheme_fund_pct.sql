drop table fof_scheme_fund_pct;

CREATE TABLE `fof_scheme_fund_pct` (
   `id` int(11) NOT NULL AUTO_INCREMENT,
   `wind_code_p` varchar(20) NOT NULL COMMENT '母基金代码',
   `wind_code_s` varchar(20) NOT NULL COMMENT ' 已投基金代码',
   `invest_scale` double DEFAULT NULL COMMENT '投资规模',
   `scheme_id` int(11) DEFAULT NULL,
   PRIMARY KEY (`id`),
   UNIQUE KEY `uk_fof_fund_pct` (`wind_code_p`,`wind_code_s`)
 ) ENGINE=MyISAM DEFAULT CHARSET=utf8;