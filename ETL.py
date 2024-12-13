# from freshsales.api import API
from utils import get_data, save_local, read_data, convert_datatypes,build_db_conn, read_exec_sql, post_data
import pandas as pd
from prefect import task, flow
from sqlalchemy import create_engine, text

pd.options.display.max_columns = None
pd.options.display.max_rows = None


# EXTRACT contacts, accounds and deals from Freshsales API endpoint
@task 
def extract_data() -> pd.DataFrame:
    """_summary_

    Returns:
        contacts_df (dataframe): raw contacts data 
        accounts_df (dataframe): raw accounts data 
        deals_df (dataframe): raw deals data
    """
    contacts_df = get_data("https://sbti-785490157659151199.myfreshworks.com/crm/sales/api/contacts/view/402011015689?include=sales_accounts",'contacts')
    accounts_df = get_data("https://sbti-785490157659151199.myfreshworks.com/crm/sales/api/sales_accounts/view/402010572406?include=deals", 'sales_accounts')
    deals_df = get_data("https://sbti-785490157659151199.myfreshworks.com/crm/sales/api/deals/view/402011015684?include=sales_account",'deals')
    save_local(contacts_df, 'contacts')
    save_local(accounts_df, 'accounts')
    save_local(deals_df, 'deals')

    return contacts_df, accounts_df, deals_df

@task 
def data_preprocess() -> pd.DataFrame:
    """_summary_

    Returns:
        contacts_df (dataframe): preprocessed contacts data 
        accounts_df (dataframe): preprocessed accounts data 
        deals_df (dataframe): preprocessed deals data
    """
    contacts_df, accounts_df, deals_df = read_data('contacts.csv', 'accounts.csv', 'deals.csv')

    contacts_df = convert_datatypes(contacts_df, 'custom_field')
    contacts_df = convert_datatypes(contacts_df, 'sales_accounts')
    accounts_df = convert_datatypes(accounts_df, 'custom_field')
    deals_df = convert_datatypes(deals_df, 'custom_field')

    contacts_df = contacts_df[['id', 'display_name', 'email', 'job_title', 'custom_field','sales_accounts']]
    contacts_df = contacts_df.rename(columns={'id': 'contact_id', 'display_name': 'contact_name'})
    contacts_df['contact_name'] = contacts_df['contact_name'].replace('(sample)','')
    contacts_df['account_id'] = contacts_df['sales_accounts'].apply(lambda x: x[0]['id'])
    contacts_df[['lead_source', 'last_contacted_date']] = pd.json_normalize(contacts_df['custom_field'])
    contacts_df.drop('sales_accounts', axis=1, inplace=True)

    # Accounts
    accounts_df = accounts_df[['id', 'name', 'custom_field', 'created_at', 'updated_at']]
    accounts_df = accounts_df.rename(columns={'id': 'account_id', 'name': 'account_name', 'created_at': 'acc_created_at', 'updated_at': 'acc_updated_at'})
    accounts_df['account_name'] = accounts_df['account_name'].replace('(sample)','')
    accounts_df['acc_created_at'] = pd.to_datetime(pd.to_datetime(accounts_df['acc_created_at']).dt.date)
    accounts_df[['industry', 'account_value', 'region']] = pd.json_normalize(accounts_df['custom_field'])

    # Deals
    deals_df = deals_df[['id', 'name', 'amount', 'custom_field','sales_account_id']]
    deals_df = deals_df.rename(columns={'id': 'deal_id', 'name': 'deal_name','sales_account_id':'account_id','amount':'deal_size'})
    deals_df[['probability_of_closure', 'deal_stage']] = pd.json_normalize(deals_df['custom_field'])

    return contacts_df, accounts_df, deals_df

@task 
def transform_data(contacts_df, accounts_df, deals_df) -> pd.DataFrame:
    """_summary_

    Args:
        contacts_df (dataframe): preprocessed contacts data
        accounts_df (dataframe): preprocessed contacts data
        deals_df (dataframe): preprocessed contacts data

    Returns:
        contacts_df (dataframe): transformed contacts data
        accounts_df (dataframe): transformed contacts data
        deals_df (dataframe): transformed contacts data
    """
    contacts_df, accounts_df, deals_df = data_preprocess()
    dfs = [contacts_df, accounts_df, deals_df]

    [df.drop('custom_field', axis=1, inplace=True) for df in dfs]
    [df.drop_duplicates(inplace=True) for df in dfs]

    # Update amount for specific deal id 
    deals_df.loc[deals_df['deal_id'] == 402002591851, 'deal_size'] = 24456  

    merge_df1 = pd.merge(accounts_df, contacts_df, how='left', on='account_id')
    merge_df2 = pd.merge(merge_df1, deals_df, how='left', on='account_id')
    # Aggregates of contacts and deals per account
    # Weekly
    weekly_agg = merge_df2.groupby(['account_id']).resample('W', on='acc_created_at').agg({'contact_id':'count', 'deal_id': 'count'}).reset_index()
    # Daily
    daily_agg = merge_df2.groupby(['account_id']).resample('D', on='acc_created_at').agg({'contact_id':'count', 'deal_id': 'count'}).reset_index()
    # Monthly
    monthly_agg = merge_df2.groupby(['account_id']).resample('ME', on='acc_created_at').agg({'contact_id':'count', 'deal_id': 'count'}).reset_index()
    # List of contacts per account
    accounts_contacts = merge_df1.groupby('account_id')['contact_id'].apply(list).reset_index()
    
    tables = (contacts_df, accounts_df, deals_df, accounts_contacts, weekly_agg, daily_agg, monthly_agg)

    # Push updated deals data to Freshsales
    data = '{"unique_identifier":{"id": "402002591851"}, "deal":{"amount":"24456.0"}}'
    post_data("https://sbti-785490157659151199.myfreshworks.com/crm/sales/api/deals/upsert", data)

    return tables

@task 
def load_data(contacts_df, accounts_df, deals_df, accounts_contacts, weekly_agg, daily_agg, monthly_agg):
    """_summary_

    Args:
        contacts_df (dataframe): transformed contacts data
        accounts_df (dataframe): transformed accounts data
        deals_df (dataframe): transformed deals data
        accounts_contacts (dataframe): transformed aggregated data
        weekly_agg (dataframe): transformed aggregated data
        daily_agg (dataframe): transformed aggregated data
        monthly_agg (dataframe): transformed aggregated data
    """
    engine = build_db_conn()
    conn = engine.connect()
    dir = 'queries/'
    files_dfs = [('accounts.sql', accounts_df), ('contacts.sql', contacts_df), ('deals.sql', deals_df), 
              ('accounts_contacts.sql', accounts_contacts), ('weekly_agg_accounts.sql', weekly_agg), 
             ('monthly_agg_accounts.sql', monthly_agg), ('daily_agg_accounts.sql', daily_agg)]
    [read_exec_sql(dir, file[0], conn) for file in files_dfs]
    [df[1].to_sql(df[0].replace('.sql',''), engine, index=False, if_exists="append") for df in files_dfs]
    conn.commit()
    conn.close()


@flow(name="SBTi Pipeline")
def data_pipeline():
    """ end to end pipeline """
    contacts_df, accounts_df, deals_df = extract_data()
    contacts_df, accounts_df, deals_df = data_preprocess()
    contacts_df, accounts_df, deals_df, accounts_contacts, weekly_agg, daily_agg, monthly_agg = transform_data(contacts_df, accounts_df, deals_df)
    load_data(contacts_df, accounts_df, deals_df, accounts_contacts, weekly_agg, daily_agg, monthly_agg)
    print("tables created in freshsales database")

# Run the flow!
if __name__ == "__main__":
    # Run pipeline
    data_pipeline.serve('sbti-deployment', cron="0 0 * * *" )

  


