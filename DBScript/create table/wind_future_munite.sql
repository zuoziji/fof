CREATE TABLE `wind_future_munite` (
   `wind_code` varchar(20) NOT NULL,
   `trade_date` date NOT NULL,
   `trade_datetime` datetime NOT NULL,
   `open` float DEFAULT NULL,
   `high` float DEFAULT NULL,
   `low` float DEFAULT NULL,
   `close` float DEFAULT NULL,
   `volume` float DEFAULT NULL,
   `amount` float DEFAULT NULL,
   `position` float DEFAULT NULL COMMENT '≥÷≤÷¡ø',
   PRIMARY KEY (`wind_code`,`trade_datetime`)
 ) ENGINE=MyISAM DEFAULT CHARSET=utf8;
 