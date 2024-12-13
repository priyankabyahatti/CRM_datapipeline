DROP TABLE IF EXISTS deals CASCADE;

CREATE TABLE deals (
    deal_id bigserial NOT NULL,
    deal_name varchar(50) NOT NULL,
    deal_size float,
    probability_of_closure float,
    deal_Stage varchar(30),
    PRIMARY KEY (deal_id),
    account_id bigserial REFERENCES accounts (account_id)
)