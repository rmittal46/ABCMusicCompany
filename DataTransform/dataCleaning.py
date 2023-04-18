
def clean_data(model):
    dropNull(model)
    return model


def dropNull(model):
    # Drop rows with missing values
    model.dropna()
    return model


def dropDuplicates(model):
    # Drop rows with Duplicate values
    model.drop_duplicates()
    return model
