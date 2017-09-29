drop table if exists wind_stock_info;

/*==============================================================*/
/* Table: wind_stock_info                                       */
/*==============================================================*/
create table wind_stock_info
(
   wind_code            varchar(20) not null,
   trade_code           varchar(20) comment '股票代码',
   sec_name             varchar(20) comment '股票名称',
   ipo_date             date comment '上市日期',
   delist_date          date comment '摘牌日期',
   exch_city            varchar(20) comment '交易所地址',
   exch_eng             varchar(20) comment '交易所英文',
   mkt                  varchar(20) comment '市场名称：主板、中小板、创业板等',
   prename              varchar(2000) comment '曾用名',
   primary key (wind_code)
) ENGINE=MyISAM;
