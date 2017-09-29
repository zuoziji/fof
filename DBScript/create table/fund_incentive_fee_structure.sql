drop table if exists fund_incentive_fee_structure;

/*==============================================================*/
/* Table: fund_incentive_fee_structure                          */
/*==============================================================*/
create table fund_incentive_fee_structure
(
   id                   int not null AUTO_INCREMENT comment '唯一id',
   wind_code_p          varchar(20) comment '母基金代码',
   wind_code            varchar(20) comment '子基金代码',
   nav_mark             double comment '净值比较基准',
   much                 double comment '提取比例',
   primary key (id)
) ENGINE=MyISAM;

alter table fund_incentive_fee_structure comment '业绩提取 incentive fee structure';
