CREATE KEYSPACE AnalyticsPoC
  WITH REPLICATION = { 
   'class' : 'SimpleStrategy', 
   'replication_factor' : 3 
  };
 
use  analyticspoc;

CREATE TABLE security_factors_long (
    security_id text,
    factor_name text,
    business_date date,
    factor_value double,
    PRIMARY KEY ((security_id, factor_name), business_date)
) WITH CLUSTERING ORDER BY (business_date DESC);

CREATE TABLE security_factors_wide (
    security_id text,
    business_date date,
    adj_close double,
    adj_high double,
    adj_low double,
    adj_open double,
    adj_volume double,
    calculated_factor1 double,
    calculated_factor10 double,
    calculated_factor2 double,
    calculated_factor3 double,
    calculated_factor4 double,
    calculated_factor5 double,
    calculated_factor6 double,
    calculated_factor7 double,
    calculated_factor8 double,
    calculated_factor9 double,
    ex_dividend double,
    factor1 double,
    factor10 double,
    factor2 double,
    factor3 double,
    factor4 double,
    factor5 double,
    factor6 double,
    factor7 double,
    factor8 double,
    factor9 double,
    org_close double,
    org_high double,
    org_low double,
    org_open double,
    org_volume double,
    split_ratio double,
    security_id_sort text,
	PRIMARY KEY (security_id, security_id_sort, business_date)
) with CLUSTERING ORDER BY (security_id_sort ASC, business_date ASC);
	
CREATE TABLE portfolio_positions (
    portfolio_id text,
    business_date date,
    security_id text,
    weight double,
    PRIMARY KEY ((portfolio_id, business_date), security_id)
);