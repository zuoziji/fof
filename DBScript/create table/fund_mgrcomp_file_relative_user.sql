drop table fund_mgrcomp_file_relative_user;

CREATE TABLE `fund_mgrcomp_file_relative_user` (
  `id` INT NOT NULL,
  `file_id` INT NULL,
  `user_id` INT NULL,
  PRIMARY KEY (`id`))
ENGINE = MyISAM
COMMENT = '存放每一个文件相关用户的信息';
