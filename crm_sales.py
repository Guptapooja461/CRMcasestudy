import pandas as pd


# Load datasets
accounts = pd.read_csv("accounts.csv")
products = pd.read_csv("products.csv")
sales_teams = pd.read_csv("sales_teams.csv")
pipeline = pd.read_csv("sales_pipeline.csv")


# Initial data inspection
print(accounts.info())
print(products.info())
print(sales_teams.info())
print(pipeline.info())

# Check for missing values in subsidiary_of column
print(accounts['subsidiary_of'].isna().sum())

# Fill missing values in subsidiary_of with 'Independent'
accounts['subsidiary_of'] = accounts['subsidiary_of'].fillna('Independent')


#validate the changes
print(accounts['subsidiary_of'].value_counts().head())

#identify missing values in year_established, revenue, and employees columns
print(accounts[['year_established', 'revenue', 'employees']].isna().sum())


# Check for missing values in close_value column
print(pipeline['close_value'].isna().sum())

#checking why close_value is missing 
#if the deal_stage is 'Won' but close_value is missing, need to investigate
print(pipeline.loc[
    (pipeline['deal_stage'] == 'Won') & (pipeline['close_value'].isna())
])

# replace missing close_value with 0 for 'Lost' deals
pipeline.loc[
    pipeline['deal_stage'] == 'Lost',
    'close_value'
] = 0

# Convert engage_date and close_date to datetime format
pipeline['engage_date'] = pd.to_datetime(pipeline['engage_date'], errors='coerce')
pipeline['close_date'] = pd.to_datetime(pipeline['close_date'], errors='coerce')


# Validate date conversion
print(pipeline[['engage_date', 'close_date']].info())


#creating time based metrics
pipeline['sales_cycle_days'] = (
    pipeline['close_date'] - pipeline['engage_date']
).dt.days

# Handle negative sales_cycle_days by setting them to None
pipeline.loc[pipeline['sales_cycle_days'] < 0, 'sales_cycle_days'] = None

# Merge pipeline with accounts to create fact_sales table
fact_sales = pipeline.merge(
    accounts,
    on='account',
    how='left'
)

# Merge fact_sales with products to include product details
fact_sales = fact_sales.merge(
    products,
    on='product',
    how='left'
)




# Final inspection of fact_sales table
print(fact_sales.head())
print(fact_sales.info())


#revenue only matters in won deals,so filtering only won deals over here
won_deals = fact_sales[fact_sales['deal_stage'] == 'Won']

#customer level revenue analysis(summary)
customer_summary = won_deals.groupby('account').agg(
    total_revenue=('close_value', 'sum'),
    deal_count=('opportunity_id', 'count')
).reset_index()

#segmenting customers based on revenue
revenue_threshold = customer_summary['total_revenue'].quantile(0.75)

customer_summary['customer_segment'] = 'Standard'
customer_summary.loc[
    customer_summary['total_revenue'] >= revenue_threshold,
    'customer_segment'
] = 'High Value'

#sanity check
print(customer_summary['customer_segment'].value_counts())
print(customer_summary.sort_values('total_revenue', ascending=False).head())




#product level performance analysis for revenue
#each prouct generate how much revenue 
product_summary = won_deals.groupby('product').agg(
    total_revenue=('close_value', 'sum'),
    deals_won=('opportunity_id', 'count')
).reset_index()


product_summary = product_summary.sort_values(
    by='total_revenue',
    ascending=False
)

print(product_summary.head())
print(product_summary.describe())



#pipeline summary
#helps to understand where deals are getting stuck

pipeline_summary = fact_sales.groupby('deal_stage').agg(
    deal_count=('opportunity_id', 'count'),
    total_pipeline_value=('close_value', 'sum')
).reset_index()

pipeline_summary = pipeline_summary.sort_values(
    by='total_pipeline_value',  
    ascending=False
)

print(pipeline_summary.head())



#territory performance analysis
#region wise performance

territory_data = fact_sales.merge(
    sales_teams,
    on='sales_agent',
    how='left'
)

print(territory_data.columns)
territory_summary = territory_data.groupby('regional_office').agg(
    total_deals=('opportunity_id', 'count'),
    deals_won=('deal_stage', lambda x: (x == 'Won').sum()),
    total_revenue=('close_value', 'sum')
).reset_index()

territory_summary['conversion_rate'] = (
    territory_summary['deals_won'] / territory_summary['total_deals']
)

territory_summary = territory_summary.sort_values('total_deals', ascending=False)
print(territory_summary.head())



customer_summary.to_csv('customer_summary.csv', index=False)
product_summary.to_csv('product_summary.csv', index=False)
pipeline_summary.to_csv('pipeline_summary.csv', index=False)
territory_summary.to_csv('territory_summary.csv', index=False)


print("Summary tables have been exported as CSV files.")
