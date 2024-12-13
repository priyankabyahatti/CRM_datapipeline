DROP TABLE IF EXISTS accounts CASCADE;

CREATE TABLE accounts (
    account_id bigserial NOT NULL,
    account_name varchar(30) NOT NULL,
    acc_created_at timestamp NOT NULL,
    acc_updated_at timestamp,
    industry varchar(30),
    account_value float,
    region varchar(20),
    PRIMARY KEY (account_id)
);