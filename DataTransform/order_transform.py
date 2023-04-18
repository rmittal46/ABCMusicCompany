import pandas as pd


def orders_transform(clean_df):
    # Convert price and total columns to float
    transform_df = convert_to_float(clean_df)

    transform_df = convert_to_date(transform_df)

    # Add a revenue column
    # transform_df['revenue'] = get_revenue(transform_df['ProductQuantity'],
    #                                                 transform_df['UnitPrice'])

    return transform_df


# Convert price and total columns to float
def convert_to_float(df):
    print(list(df.columns.values))
    df['UnitPrice'] = df['UnitPrice'].astype(float)
    df['TotalPrice'] = df['TotalPrice'].astype(float)
    return df


# Calculate revenue
def get_revenue(product_quantity, price):
    return product_quantity * price


# Convert dates to datetime format
def convert_to_date(df):
    df['PaymentDate'] = pd.to_datetime(df['PaymentDate'])
    return df