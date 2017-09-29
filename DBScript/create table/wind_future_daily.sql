CREATE TABLE `fof_ams_dev`.`wind_future_daily` (
  `wind_code` VARCHAR(20) NOT NULL,
  `trade_date` DATE NOT NULL,
  `open` FLOAT NULL,
  `high` FLOAT NULL,
  `low` FLOAT NULL,
  `close` FLOAT NULL,
  `volume` FLOAT NULL,
  `amt` FLOAT NULL,
  `dealnum` FLOAT NULL COMMENT '成交笔数，统计逐笔成交的数量',
  `settle` FLOAT NULL COMMENT '结算价',
  `maxupordown` INT NULL COMMENT '标记收盘涨停或跌停状态,1表示涨停；-1则表示跌停；0表示未涨跌停。',
  `oi` FLOAT NULL COMMENT '持仓量',
  `st_stock` FLOAT NULL COMMENT '注册仓单数量',
  PRIMARY KEY (`wind_code`, `trade_date`))
ENGINE = MyISAM;
