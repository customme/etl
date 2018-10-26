CREATE TABLE fact_new_adv_n (
  aid VARCHAR(64),
  cuscode VARCHAR(32),
  init_city VARCHAR(16),
  city VARCHAR(16),
  init_ip VARCHAR(16),
  ip VARCHAR(16),
  create_time DATETIME,
  update_time DATETIME,
  create_date INT,
  PRIMARY KEY(aid),
  KEY idx_cuscode(cuscode),
  KEY idx_init_city(init_city),
  KEY idx_city(city),
  KEY idx_create_date(create_date)
) ENGINE=MyISAM COMMENT='新增用户';
