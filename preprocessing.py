from pandas import DataFrame
from config import NAN

def replace_nan(dataset: DataFrame):
    return dataset.replace(["NaN", "nan"], NAN)

def check_if_number(x):
    try:
        int(float(x))
        return True
    except:
        return False

#Making every cell str and checking if a number value is in the field, else make it nan
def process_phone_numbers(dataset: DataFrame):
    new_dataset = dataset.copy(deep=True)

    new_dataset["phone"] = [str(int(float(elem))) if check_if_number(elem) else NAN for elem in dataset["phone"]]

    return new_dataset

def convert_types(dataset: DataFrame):
    new_dataset = dataset.copy(deep=True)
    for column in dataset.columns:
        new_dataset[column] = [str(elem).strip() if elem != NAN and str(elem).strip() != "" else NAN for elem in dataset[column]]
    
    return new_dataset

def preprocess(dataset: DataFrame):
    return replace_nan(process_phone_numbers(convert_types(dataset.copy(deep=True))))
