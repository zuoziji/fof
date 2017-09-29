drop table if exists fund_nav_calc;

CREATE TABLE fund_nav_calc (
   `wind_code` varchar(20) NOT NULL,
   `nav_date` date NOT NULL,
   `share` float DEFAULT '0' COMMENT '基金份额',
   `market_value` float DEFAULT '0' COMMENT '基金市值',
   `cash_amount` float DEFAULT '0' COMMENT '银行现金余额',
   `manage_fee` float DEFAULT '0' COMMENT '管理费',
   `custodian_fee` float DEFAULT '0' COMMENT '托管费',
   `admin_fee` float DEFAULT '0' COMMENT '行政管理费',
   `storage_fee` float DEFAULT '0' COMMENT '客户专项资金保管',
   `other_fee` float DEFAULT '0' COMMENT '其他费用（用于保存其他未标明的费用，主要供手工填写使用）',
   `nav` float DEFAULT '1' COMMENT '基金净值',
   PRIMARY KEY (`wind_code`,`nav_date`)
 ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='基金净值计算表，供子基金、母基金净值计算使用';
 