 CREATE TABLE transactions(
		currency CHARACTER(3) NOT NULL,
		amount BIGINT NOT NULL,
		state VARCHAR(25) NOT NULL,
		created_date TIMESTAMP NOT NULL,
		merchant_category VARCHAR(100),
		merchant_country VARCHAR(3),
		entry_method VARCHAR(4) NOT NULL,
		user_id UUID NOT NULL,
		type VARCHAR(20) NOT NULL,
		source VARCHAR(20) NOT NULL,
		id UUID PRIMARY KEY	
		);

	
CREATE TABLE users(
		id UUID PRIMARY KEY,
		has_email BOOLEAN NOT NULL,
		phone_country VARCHAR(300),
		is_fraudster BOOLEAN NOT NULL,
		terms_version DATE,
		created_date TIMESTAMP NOT NULL,
		state VARCHAR(25) NOT NULL,
		country VARCHAR(2),
		birth_year INTEGER,
		kyc VARCHAR(20),
		failed_sign_in_attempts INTEGER
		);
		
		
CREATE TABLE fx_rates(
		ts TIMESTAMP,
		base_ccy VARCHAR(3),
		ccy VARCHAR(10),
		rate DOUBLE PRECISION,
		PRIMARY KEY(ts, base_ccy, ccy)
		);


CREATE TABLE currency_details(
		ccy VARCHAR(10) PRIMARY KEY,
		iso_code INTEGER,
		exponent INTEGER,
		is_crypto BOOLEAN NOT NULL
		);
		







