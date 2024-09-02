import pandas as pd
import os

from config import GOOGLE_PATH, FACEBOOK_PATH, WEBSITE_PATH, OUTPUT_PATH

from join import join_many_one_key, join_two
from column_reducer import reduce_columns
from preprocessing import preprocess
from config import SEP


def main_v1():

    facebook_dataset = pd.read_csv(FACEBOOK_PATH, encoding="utf-8", on_bad_lines="skip", low_memory=False)
    google_dataset = pd.read_csv(GOOGLE_PATH, encoding="utf-8", on_bad_lines="skip", low_memory=False)
    website_dataset = pd.read_csv(WEBSITE_PATH, sep=";", on_bad_lines="skip", low_memory=False)

    facebook_dataset = preprocess(facebook_dataset)
    google_dataset = preprocess(google_dataset)
    website_dataset = preprocess(website_dataset)
    
    google_dataset = google_dataset.rename(columns={"category" : "categories"})
    website_dataset = website_dataset.rename(columns={
        "root_domain" : "domain" ,
        "s_category" : "categories",
        "main_city" : "city",
        "main_country" : "country_name",
        "legal_name" : "name",
        "main_region" : "region_name",
        }, errors="raise")

    all_columns = sorted(set(facebook_dataset.columns).union(set(google_dataset.columns)).union(set(website_dataset.columns)))

    suffixes = []
    final, garbage, suff = join_many_one_key([facebook_dataset, google_dataset, website_dataset], "domain")
    suffixes.extend(suff)

    good_garbage, bad_garbage, suff_2 = join_many_one_key(garbage, "phone")
    suffixes.extend(suff_2)

    reduce_columns(good_garbage, all_columns, suffixes=suffixes).to_csv(os.path.join(OUTPUT_PATH, "main_v1_rest_that_couldnt_be_joined_on_domain_so_it_was_joined_on_phone.csv"), sep=SEP, index=False)

    good_garbage["phone"] = reduce_columns(good_garbage, ["phone"], suffixes=suffixes)["phone"]
    good_garbage = good_garbage.drop([f"phone{elem}" for elem in suffixes if f"phone{elem}" in good_garbage.columns], axis=1)

    final["phone"] = reduce_columns(final, ["phone"], suffixes=suffixes)["phone"]
    final = final.drop([f"phone{elem}" for elem in suffixes if f"phone{elem}" in final.columns], axis=1)


    suffixes.append("_garbage")
    final, discarded = join_two(final, good_garbage, "phone", sufixes=(None, "_garbage"), how="left")
    final = reduce_columns(final, all_columns, suffixes=suffixes)
    
    final_with_duplicated_phone = discarded[0]
    final_with_duplicated_phone = reduce_columns(final_with_duplicated_phone, all_columns, suffixes=suffixes)
    final = pd.concat([final, final_with_duplicated_phone], ignore_index=True)

    final.to_csv(os.path.join(OUTPUT_PATH, "main_v1_final.csv"), sep=SEP, index=False)


def main_v2():
    facebook_dataset = pd.read_csv(FACEBOOK_PATH, encoding="utf-8", on_bad_lines="skip", low_memory=False)
    google_dataset = pd.read_csv(GOOGLE_PATH, encoding="utf-8", on_bad_lines="skip", low_memory=False)
    website_dataset = pd.read_csv(WEBSITE_PATH, sep=";", on_bad_lines="skip", low_memory=False)

    facebook_dataset = preprocess(facebook_dataset)
    google_dataset = preprocess(google_dataset)
    website_dataset = preprocess(website_dataset)
    
    google_dataset = google_dataset.rename(columns={"category" : "categories"})
    website_dataset = website_dataset.rename(columns={
        "root_domain" : "domain" ,
        "s_category" : "categories",
        "main_city" : "city",
        "main_country" : "country_name",
        "legal_name" : "name",
        "main_region" : "region_name",
        }, errors="raise")

    all_columns = sorted(set(facebook_dataset.columns).union(set(google_dataset.columns)).union(set(website_dataset.columns)))


    facebook_without_duplicate_domains = facebook_dataset.drop_duplicates(subset=["domain"], keep=False)

    google_without_duplicate_domains = google_dataset.drop_duplicates(subset=["domain"], keep=False)

    website_without_duplicate_domains = website_dataset.drop_duplicates(subset=["domain"], keep=False)

    AB = facebook_without_duplicate_domains.merge(google_without_duplicate_domains, suffixes=("_facebook", "_google"), on=["domain"], how="inner")
    ABC = AB.merge(website_without_duplicate_domains, on=["domain"], how="inner")
    
    #Preference: facebook -> google -> website 
    ABC = reduce_columns(ABC, all_columns, suffixes=("_facebook", "_google"))

    ABC.to_csv(os.path.join(OUTPUT_PATH, "main_v2_final.csv"), sep=SEP, index=False)


def main():
    #Tried adding more info to the columns by joining domain then joining phone column with the rest.
    #Too much complexity for almost no gain compared to simply joining on domain.
    #Very slow compared to v2.
    #Offers an aditional table that tries to join on phone the rows that are unfit to be joined on domain.
    #The aditional table may contain conflicts when compared to the final.csv
    main_v1()

    #Just better and simpler
    main_v2()

if __name__ == "__main__":
    main()
