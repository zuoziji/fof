drop table fund_mgrcomp_file;

CREATE TABLE `fund_mgrcomp_file` (
   `file_id` int(11) NOT NULL AUTO_INCREMENT,
   `mgrcomp_id` varchar(100) DEFAULT NULL,
   `file_type` varchar(45) DEFAULT NULL,
   `upload_user_id` int(11) DEFAULT NULL,
   `upload_datetime` datetime DEFAULT NULL,
   `file_name` varchar(500) DEFAULT NULL,
   `file_content` longblob,
   `comments` text,
   PRIMARY KEY (`file_id`)
 ) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='投顾文件信息表，存放投顾评审过程中的相关文件，文件内容存放在file_content中，文件名存放在file_name 带扩展名';
