drop index uk_fund_sec_pct on fund_sec_pct;

drop table if exists fund_sec_pct;

/*==============================================================*/
/* Table: fund_sec_pct                                          */
/*==============================================================*/
create table fund_sec_pct
(
   id                   int not null AUTO_INCREMENT,
   wind_code            varchar(20) not null comment 'wind_code 如果是复华自己的子基金或复华投资的未公开产品，则是复华代码',
   sec_code             varchar(20) not null,
   nav_date             date not null comment '净值日期',
   direction            int not null comment '方向：1 做多；0 做空
            股票默认为多
            股指默认保存净空单',
   position             int comment '持仓数量',
   cost_unit            double comment ' 单位成本',
   cost_tot             double comment '总成本',
   cost_pct             double comment '成本占比',
   value_tot            double comment '持仓市值',
   value_pct            double comment '市值占比',
   trade_status         varchar(200),
   sec_type             int comment '证券类型 0：股票；1：期货；2：债券；3：逆回购',
   primary key (id),
   UNIQUE KEY `uk_fund_sec_pct` (`wind_code`,`sec_code`,`nav_date`,`direction`)
) ENGINE=MyISAM;
