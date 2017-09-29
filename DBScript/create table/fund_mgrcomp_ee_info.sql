drop table if exists fund_mgrcomp_ee_info;

/*==============================================================*/
/* Table: fund_mgrcomp_ee_info                                  */
/*==============================================================*/
create table fund_mgrcomp_ee_info
(
   ee_id                int primary key auto_increment,
   name                 varchar(50),
   sex                  varchar(10),
   birthday             varchar(50),
   graduate             text,
   career_since         text comment '起始相关行业从业日期',
   post_before          text comment '过往任职单位及职位',
   post_period          text comment '过往任职起止日期',
   post_curr            text comment '目前职位',
   share_percent        text comment '所占股份（如有）',
   hist_performance     text comment '历史业绩',
   punish               text comment '行业处分',
   Interest_conflict    text comment '利益冲突'
) engine= MyISAM;
