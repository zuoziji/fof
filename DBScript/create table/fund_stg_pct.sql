drop index uk_fund_stg_pct on fund_stg_pct;

drop table if exists fund_stg_pct;

/*==============================================================*/
/* Table: fund_stg_pct                                          */
/*==============================================================*/
create table fund_stg_pct
(
   id                   int not null AUTO_INCREMENT,
   wind_code            varchar(20) not null,
   stg_code             varchar(20) not null comment '策略代码：alpha,  long_only, ...',
   trade_date           date not null,
   stg_pct              double comment '策略比例',
   primary key (id)
) ENGINE=MyISAM;

/*==============================================================*/
/* Index: uk_fund_stg_pct                                       */
/*==============================================================*/
create unique index uk_fund_stg_pct on fund_stg_pct
(
   wind_code,
   stg_code,
   trade_date
);