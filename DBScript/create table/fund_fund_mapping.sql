drop table if exists fund_fund_mapping;

/*==============================================================*/
/* Table: fund_fund_mapping                                     */
/*==============================================================*/
create table fund_fund_mapping
(
   wind_code_s          varchar(20) not null comment '已投基金代码',
   wind_code            varchar(20) not null comment '实际基金代码',
   sec_name_s           varchar(100) comment '已投资基金别名',
   date_start           date comment '投资子基金起始累计净值',
   date_end             date comment '清盘日其',
   warning_line         double comment '预警线',
   winding_line         double comment '清盘线',
   primary key (wind_code_s)
);

alter table fund_fund_mapping comment '记录已投资基金与实际基金产品之间关系';
