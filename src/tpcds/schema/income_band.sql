create table income_band(
	ib_income_band_sk integer not null,
	ib_lower_bound integer,
	ib_upper_bound integer
) DISTRIBUTED BY (ib_income_band_sk);
