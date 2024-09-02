from pandas import DataFrame
from column_reducer import reduce_columns

from typing import List, Tuple

import uuid

def anti_join(dataset_1: DataFrame, dataset_2: DataFrame, key, suffixes, how_was_joined)-> DataFrame:
    outer_join = dataset_1.merge(dataset_2, on=[key], how="outer", suffixes=suffixes, indicator=True)

    if how_was_joined == "left":
        return outer_join[(outer_join._merge == "right_only")].drop("_merge", axis=1)
    
    if how_was_joined == "right":
        return outer_join[(outer_join._merge == "left_only")].drop("_merge", axis=1)
    
    return outer_join[~(outer_join._merge == "both")].drop("_merge", axis=1)

def join_two(dataset_1: DataFrame, dataset_2: DataFrame, key, sufixes, how)-> Tuple[DataFrame, List[DataFrame]]:

    all_columns = sorted(set(dataset_1.columns).union(set(dataset_2.columns)))

    dataset_1_without_duplicate_key = dataset_1.drop_duplicates(subset=[key], keep=False)
    dataset_1_only_duplicate_key = dataset_1[dataset_1.duplicated([key], keep=False)] #Includes nan s and empty column domain not usefull

    dataset_2_without_duplicate_key = dataset_2.drop_duplicates(subset=[key], keep=False)
    dataset_2_only_duplicate_key = dataset_2[dataset_2.duplicated([key], keep=False)]

    AB = dataset_1_without_duplicate_key.merge(dataset_2_without_duplicate_key, suffixes=sufixes, on=[key], how=how)

    anti_join_AB = anti_join(dataset_1_without_duplicate_key, dataset_2_without_duplicate_key, key=key, suffixes=("_TEMP1", "_TEMP2"), how_was_joined=how)
    anti_join_AB = reduce_columns(anti_join_AB, all_columns, suffixes=("_TEMP1", "_TEMP2"))#We don't loose data, we just prune NaN columns

    rest = []
    if dataset_1_only_duplicate_key.shape[0] > 0:
        rest.append(dataset_1_only_duplicate_key)
    if dataset_2_only_duplicate_key.shape[0] > 0:
        rest.append(dataset_2_only_duplicate_key)
    if anti_join_AB.shape[0] > 0:
        rest.append(anti_join_AB)

    return AB, rest


def join_many_one_key(datasets: List[DataFrame], key)-> Tuple[DataFrame, List[DataFrame]]:
    sufixes = []
    
    final = datasets.pop(0).copy(deep=True)
    rest = []

    while datasets:

        sufix_1 = None
        sufix_2 = f"_{uuid.uuid4()}"
        sufixes.append(sufix_2)

        final, not_merged = join_two(final, datasets.pop(0).copy(deep=True), key, sufixes=(sufix_1, sufix_2), how="inner")
        rest.extend(not_merged)

    return final, rest, sufixes

    


