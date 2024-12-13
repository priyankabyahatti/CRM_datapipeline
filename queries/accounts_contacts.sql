DROP TABLE IF EXISTS accounts_contacts CASCADE;

CREATE TABLE accounts_contacts (
    account_id bigserial NOT NULL,
    contact_id varchar(50)
)