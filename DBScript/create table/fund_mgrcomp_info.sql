drop table fund_mgrcomp_info;

CREATE TABLE `fund_mgrcomp_info` (
   `mgrcomp_id` int(11) NOT NULL AUTO_INCREMENT,
   `name` varchar(100) DEFAULT NULL,
   `alias` varchar(100) DEFAULT NULL,
   `review_status` int(11) DEFAULT NULL COMMENT '评审状态：0 未评审；1基础池（未通过评审）；2 观察池（待观察）；3 核心池（通过评审）；4 黑名单（不在合作）；',
   `fund_count_tot` int(11) DEFAULT NULL COMMENT '全部历史基金数量',
   `fund_count_existing` int(11) DEFAULT NULL COMMENT '处于存续期的基金数量',
   `fund_count_active` int(11) DEFAULT NULL COMMENT '近期净值更新的基金数量',
   `address` varchar(500) DEFAULT NULL COMMENT '注册地址',
   `description` varchar(5000) DEFAULT NULL COMMENT '描述信息',
   `registered_capital` int(11) DEFAULT NULL COMMENT '注册资本',
   PRIMARY KEY (`mgrcomp_id`),
   UNIQUE KEY `name_UNIQUE` (`name`),
   UNIQUE KEY `alias_UNIQUE` (`alias`)
 ) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='投顾信息表';
