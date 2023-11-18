###### This file is for dagBookstore.py file ######
### This file has additional methods for completing the tasks functionalitues ###



# Completed Sub task 4 : cleaning data frame

def clean_dataframe(df):
    # Clean the first DataFrame
    cleaned_df = df.drop_duplicates().dropna()

    # Remove leading and trailing whitespaces in string columns
    cleaned_df = cleaned_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Convert string columns to lowercase
    string_columns1 = ['product_type', 'url', "upc", "title", "description", "category"]  # Replace with your actual string columns
    for col in string_columns1:
        cleaned_df[col] = cleaned_df[col].str.lower()

    # Fill missing values with a specific value
    cleaned_df['availability'] = cleaned_df['availability'].fillna(0)  # Replace with your actual numeric column
    cleaned_df['stars'] = cleaned_df['stars'].fillna(0)  # Replace with your actual numeric column
    cleaned_df['price_incl_tax'] = cleaned_df['price_incl_tax'].fillna(0)  # Replace with your actual numeric column
    cleaned_df['price'] = cleaned_df['price'].fillna(0)  # Replace with your actual numeric column
    cleaned_df['price_excl_tax'] = cleaned_df['price_excl_tax'].fillna(0)  # Replace with your actual numeric column

    # Reset index after cleaning
    cleaned_df = cleaned_df.reset_index(drop=True)

    return cleaned_df



# Completed Sub task 5 : transforming data - remove useless columns

def transform_dataframe(df):

    # Extract the year from the 'title' and create a new column 'publication_year'
    df['publication_year'] = df['title'].str.extract(r'(\d{4})', expand=False)
    df['publication_year'] = df['publication_year'].astype(float)

    # Extract the domain from the 'url' column and create a new column 'domain'
    df['domain'] = df['url'].str.extract(r'://(.[^/]+)', expand=False)

    # Calculate the 'discount' as the difference between 'price_incl_tax' and 'price_excl_tax'
    df['discount'] = df['price_incl_tax'] - df['price_excl_tax']

    # Drop non-using columns like URL, description, etc.
    columns_to_drop = ['url', 'description', 'num_reviews', 'price_excl_tax', 'price_excl_tax']  
    df = df.drop(columns=columns_to_drop, errors='ignore')

    print(f'transformaing data... columns: {df.columns}')
    
    return df




# SQL Queries for some tasks


get_tables_sql = "SELECT * FROM books;"




create_table_destination_sql = """
    CREATE TABLE IF NOT EXISTS books_destination (
    availability TEXT,
    category TEXT,
    price TEXT,
    product_type TEXT,
    stars TEXT,
    tax TEXT,
    title TEXT,
    upc TEXT,
    publication_year TEXT,
    domain TEXT, 
    discount TEXT);
"""


