def clean_data(model):
    clean_df = remove_special_char(model)
    split_name_df = split_customer_name(clean_df)
    lower_cased_df = to_lower_case(split_name_df)
    modified_df = replace_country_code(lower_cased_df)
    return modified_df


def dropNull(model):
    # Drop rows with missing values
    model.dropna()
    return model


def dropDuplicates(model):
    # Drop rows with Duplicate values
    model.drop_duplicates()
    return model


def split_customer_name(dataframe):
    dataframe[['First_name', 'Last_name']] = dataframe['ClientName'].str.split(n=1, expand=True)
    return dataframe


def remove_special_char(dataframe):
    dataframe['ClientName'] = dataframe['ClientName'].str.replace('[^a-zA-Z0-9 ]+', '', regex=True)
    return dataframe


def to_lower_case(dataframe):
    # Convert all columns to lowercase and capitalize first letter
    dataframe = dataframe.applymap(lambda x: x.lower().capitalize() if isinstance(x, str) else x)
    return dataframe

def replace_country_code(dataframe):
    dataframe['DeliveryCountry'] = dataframe['DeliveryCountry'].replace(['Uk'],'United kingdom')
    return dataframe