import pandas as pd
from config import NAN
from fuzzywuzzy import fuzz

from typing import List

def fuzzywuzzy_tournament(array: List[str]):
    scores = {elem : 0 for elem in array}

    for elem in array:
        for elem_2 in array:
            #Should be already strings and strip. Lowercased for a better matching
            score = fuzz.token_sort_ratio(str(elem).strip().lower(), elem_2.strip().lower())
            scores[elem_2] += score
    
    return list({key: value for key, value in sorted(scores.items(), key=lambda x: x[1], reverse=True)}.keys())[0]


def choose_candidate(candidates):
    candidates = candidates.values

    #Remove NaN values
    candidates = [elem for elem in candidates if not pd.isna(elem)]
    if len(candidates) == 0:
        return NAN
    
    #Every candidate will vote for similarity for the other candidates and himself
    #The candidate with the most votes will be the chosen one
    #If tie, then the first candidate will be chosen
    return fuzzywuzzy_tournament(candidates)


def reduce_columns(dataset: pd.DataFrame, all_columns, suffixes: tuple)-> pd.DataFrame:
    dataset = dataset.copy(deep=True)

    suff = [elem for elem in suffixes]

    aux = pd.DataFrame()

    for column in all_columns:
        columns = [f"{column}{elem}" for elem in suff if f"{column}{elem}" in dataset.columns]
        if column in dataset.columns:
            columns.append(column)
        
        if len(columns) == 0:
            continue

        if len(columns) == 1:
            aux[column] = dataset[columns[0]]
            continue

        df = pd.DataFrame(dataset[[elem for elem in columns]])

        result_column = []
        for _, candidates in df.iterrows():
            result_column.append(choose_candidate(candidates))
        
        aux[column] = result_column
    
    return aux
    
