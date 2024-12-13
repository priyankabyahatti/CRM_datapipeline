DROP TABLE IF EXISTS contacts CASCADE;

CREATE TABLE contacts (
    contact_id bigserial NOT NULL,
    contact_name varchar(30) NOT NULL,
    email varchar(100),
    job_title varchar(30),
    lead_source varchar(30),
    last_contacted_date timestamp,
    PRIMARY KEY (contact_id),
    account_id bigserial REFERENCES accounts (account_id)
)