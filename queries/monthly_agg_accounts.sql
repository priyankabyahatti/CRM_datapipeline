DROP TABLE IF EXISTS monthly_agg_accounts CASCADE;

CREATE TABLE monthly_agg_accounts (
    account_id bigserial NOT NULL,
    acc_created_at timestamp NOT NULL,
    contact_id integer,
    deal_id integer
)