drop table if exists fund_event;

/*==============================================================*/
/* Table: fund_event                                            */
/*==============================================================*/
CREATE TABLE `fund_event` (
   `id` int(11) NOT NULL AUTO_INCREMENT,
   `wind_code` varchar(20) NOT NULL COMMENT '基金代码',
   `event_date` date NOT NULL COMMENT '事件日期',
   `event_type` varchar(20) NOT NULL COMMENT '事件类型',
   `remind_date` date DEFAULT NULL COMMENT '提醒起始日期',
   `handle_status` tinyint(4) DEFAULT NULL COMMENT '处理状态 0：待处理；1：已处理',
   `description` text COMMENT '提示信息',
   `create_date` date DEFAULT NULL COMMENT '事件创建日期',
   `color` varchar(20) DEFAULT NULL,
   `create_user` varchar(45) DEFAULT NULL,
   `private` tinyint(4) DEFAULT NULL,
   PRIMARY KEY (`id`)
 ) ENGINE=MyISAM AUTO_INCREMENT=34 DEFAULT CHARSET=utf8;
