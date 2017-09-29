drop table if exists wind_fund_info;

/*==============================================================*/
/* Table: wind_fund_info                                        */
/*==============================================================*/
create table wind_fund_info
(
   wind_code            varchar(20) not null,
   sec_name             varchar(100),
   strategy_type        varchar(50),
   fund_setupdate       date,
   fund_maturitydate    date,
   fund_mgrcomp         varchar(200),
   fund_existingyear    double,
   fund_ptmyear         double,
   fund_type            varchar(50),
   fund_fundmanager     varchar(200),
   primary key (wind_code)
) ENGINE=MyISAM;

alter table wind_fund_info comment 'wind原始数据表';
