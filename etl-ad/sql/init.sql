CREATE TABLE fact_new_adv_n (
  aid VARCHAR(64),
  channel_code VARCHAR(32),
  init_area VARCHAR(16),
  area VARCHAR(16),
  init_ip VARCHAR(16),
  ip VARCHAR(16),
  create_time DATETIME,
  update_time DATETIME,
  create_date INT,
  PRIMARY KEY(aid),
  KEY idx_channel_code(channel_code),
  KEY idx_init_area(init_area),
  KEY idx_area(area),
  KEY idx_create_date(create_date)
) ENGINE=InnoDB COMMENT='新增用户';
