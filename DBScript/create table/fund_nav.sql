drop table if exists fund_nav;

/*==============================================================*/
/* Table: fund_nav                                              */
/*==============================================================*/
create table fund_nav
(
   wind_code            varchar(20) not null comment '基金代码',
   nav_date             date not null comment '净值日期',
   nav                  double comment '净值',
   nav_acc              double comment '累计净值',
   nav_tot              double comment '资产净值',
   source_mark          int comment '数据源：0：wind；1：手动；2：导入',
   primary key (wind_code, nav_date)
) ENGINE=MyISAM;
