"""
Script for matching the spare parts lists

Parameters to Adjust:
- entity: The entity identifier
- n_chunk: Number of chunks to avoid memory issues
- file_to_match 
- full_invetory

Usage:
1. Set the parameters to adjust according to your needs.
2. Run the script to match the data.
"""

import pandas as pd
import numpy as np
import re
from concurrent.futures import ThreadPoolExecutor
import os
import Levenshtein
import argparse
import sys
import warnings
import shutil
import time


def clean_str(string):
    # Clean a string by removing non-alphanumeric characters and leading zeros
    new_string = re.sub(r"[^a-zA-Z0-9\s]|[\W_]+", "", string)
    new_string = re.sub(r"[-.,/\\]", "", new_string)
    new_string = re.sub(" ", "", new_string)
    new_string = new_string.lstrip("0")
    new_string = new_string.strip("0")
    return new_string


def rm_initial_final_letters(string):
    # Remove final non-numeric characters from a string
    if string.isalpha():
        return string
    new_string = re.sub(r"^[a-zA-Z]+", "", string)
    new_string = re.sub(r"[a-zA-Z]+$", "", new_string)
    return new_string


def find_match(df):
    # find match if ID_partial is icluded in ID_full
    for index, row in df.iterrows():
        if str(row["ID_partial"]) in str(row["ID_full"]):
            df.at[index, "IND"] = 1
        else:
            df.at[index, "IND"] = 0
    return df


def matching_percentage(string1, string2):
    # Preprocessing the strings
    string1 = str(string1).lower()
    string1 = re.sub(r"[^\w\s]", "", string1)
    string1 = string1.split()
    string1 = sorted(string1)

    string2 = str(string2).lower()
    string2 = re.sub(r"[^\w\s]", "", string2)
    string2 = string2.split()
    string2 = sorted(string2)
    
    # Creating the result matrix
    result = [[0 for _ in range(len(string2))] for _ in range(len(string1))]
    for i in range(len(string1)):
        for j in range(len(string2)):
            long, short = (
                (string1[i], string2[j])
                if len(string1[i]) >= len(string2[j])
                else (string2[j], string1[i])
            )

            if short == long:
                result[i][j] = 1
            elif short in long:
                result[i][j] = 0.5

    # Calculating the total sum of matches
    total_sum = sum(sum(row) for row in result)

    # Normalizing the total sum by the maximum possible matches
    max_dim = max(len(string1), len(string2))
    matching_perc = total_sum / max_dim * 100
    if matching_perc > 100:
        matching_perc = 100
    return matching_perc


def levenshtein_distance(row):
    # Calculate Levenshtein distance between two strings
    dist = Levenshtein.distance(str(row["ID_partial"]), str(row["ID_full"]))
    return dist


def inventory_preparation(df):
    df = df[
        [
            "Mfr Part Number",
            "Material",
            "Material Description",
            "Supplier Mat. No.",
            "Base Unit of Measure",
        ]
    ]

    # Data for match on Mfr Part Number
    list_MPN = df[
        ["Mfr Part Number", "Material", "Material Description", "Base Unit of Measure"]
    ]
    list_MPN.columns = [
        "Mfr Part Number",
        "Material",
        "Description_full",
        "Base Unit of Measure",
    ]
    list_MPN = list_MPN.dropna(subset=["Mfr Part Number"])
    # Prepare the ID for the matching
    list_MPN["Mfr Part Number"] = list_MPN["Mfr Part Number"].astype(str)
    list_MPN["ID_full"] = list_MPN["Mfr Part Number"].apply(clean_str)
    list_MPN = list_MPN.drop_duplicates()

    # Data for match on Supplier Mat. No.
    list_SMN = df[
        [
            "Supplier Mat. No.",
            "Material",
            "Material Description",
            "Base Unit of Measure",
        ]
    ]
    list_SMN.columns = [
        "Supplier Mat. No.",
        "Material",
        "Description_full",
        "Base Unit of Measure",
    ]
    list_SMN = list_SMN.dropna(subset=["Supplier Mat. No."])
    # Prepare the ID for the matching
    list_SMN["Supplier Mat. No."] = list_SMN["Supplier Mat. No."].astype(str)
    list_SMN["ID_full"] = list_SMN["Supplier Mat. No."].apply(clean_str)
    list_SMN = list_SMN.drop_duplicates()
    return list_MPN, list_SMN, df


def to_match_preparation(df):
    df = df[["ID", "product_identifier", "item_description"]]
    df.columns = ["ID", "Product_identifier", "Description_partial"]
    df = df.dropna(subset=["Product_identifier"])
    mask = df["Product_identifier"].str.contains(";")
    # split rows with ; in Product_identifier
    split_df = df[mask].copy()
    split_df = split_df.assign(
        Product_identifier=split_df["Product_identifier"].str.split(";")
    ).explode("Product_identifier")
    result_df = pd.concat([df[~mask], split_df], ignore_index=True)
    # Prepare the ID for the matching
    result_df["Product_identifier"] = result_df["Product_identifier"].astype(str)
    result_df["ID_partial"] = result_df["Product_identifier"].apply(clean_str)
    result_df["ID_partial"] = result_df["ID_partial"].apply(rm_initial_final_letters)
    result_df = result_df.drop_duplicates()
    return result_df


def matching_procedure(
    to_match, full_inventory, inventory_name, n_chunk, folder, country
):
    data_chunks = np.array_split(full_inventory, n_chunk)
    # Create the folder to save the files
    if os.path.exists(os.path.join(folder, country, inventory_name)):
        shutil.rmtree(os.path.join(folder, country, inventory_name))
    os.makedirs(os.path.join(folder, country, inventory_name))

    for i, chunk in enumerate(data_chunks, start=1):
        merged_data = pd.merge(chunk, to_match, how="cross")
        merged_data["IND"] = 0

        # Find matches in ID using ThreadPoolExecutor
        with ThreadPoolExecutor() as executor:
            results1 = list(executor.map(find_match, [merged_data]))

        match = results1[0]
        match = match[match["IND"] == 1]
        match = match.drop(columns="IND")

        match["Description_full"] = np.where(
            match["Description_full"].isna(), "", match["Description_full"]
        )
        # Compute the matching percentage
        match["matching_percentage"] = match.apply(
            lambda row: matching_percentage(
                row["Description_partial"], row["Description_full"]
            ),
            axis=1,
        )
        match["Matching_based_on"] = np.where(
            match["matching_percentage"] != 0, inventory_name, ""
        )
        # Compute the measure of distance between IDs
        match["ID_similarity"] = match.apply(levenshtein_distance, axis=1)
        # Save partial match
        match_name_file = "match_on_" + inventory_name + f"_chunk_{i}.csv"
        match.to_csv(
            os.path.join(folder, country, inventory_name, match_name_file), index=False
        )
        print(i)
    return


def final_file_preparation(folder, country, to_match):
    # Collect all the matching dataframe previously saved
    MPN_files = []
    for files_int in os.listdir(os.path.join(folder, country, "MPN/")):
        MPN_files.append(os.path.join(folder, country, "MPN/", files_int))

    MPN_Match = pd.DataFrame()
    for file_path in MPN_files:
        tmp = pd.read_csv(file_path, sep=",", low_memory=False)
        MPN_Match = pd.concat([MPN_Match, tmp], ignore_index=True)

    MPN_Match.rename(columns={"Mfr Part Number": "PartNumber"}, inplace=True)

    SMN_files = []
    for files_int in os.listdir(os.path.join(folder, country, "SMN/")):
        SMN_files.append(os.path.join(folder, country, "SMN/", files_int))

    SMN_Match = pd.DataFrame()
    for file_path in SMN_files:
        tmp = pd.read_csv(file_path, sep=",", low_memory=False)
        SMN_Match = pd.concat([SMN_Match, tmp], ignore_index=True)

    SMN_Match.rename(columns={"Supplier Mat. No.": "PartNumber"}, inplace=True)

    # Concatenate  MPN and SMN data
    df_merged_final = pd.concat([MPN_Match, SMN_Match], ignore_index=True)
    df_merged_final = df_merged_final.drop(columns=["ID_partial", "ID_full"])
    df_merged_final = df_merged_final.drop_duplicates()

    # Select the 7 top matches accordingly to the matching percentage and the ID similarity
    df_top7_per_ID = (
        df_merged_final.groupby("Product_identifier")
        .apply(
            lambda x: x.sort_values(
                by=["matching_percentage", "ID_similarity"], ascending=[False, True]
            ).head(7)
        )
        .reset_index(drop=True)
    )
    df_top7_per_ID = df_top7_per_ID.drop(columns="ID_similarity")
    df_top7_per_ID["Product_identifier"] = df_top7_per_ID["Product_identifier"].astype(
        str
    )
    df_top7_per_ID = pd.merge(
        df_top7_per_ID,
        to_match[["ID", "Product_identifier", "Description_partial"]],
        on="Product_identifier",
        how="outer",
    )
    df_top7_per_ID["Description_partial"] = np.where(
        df_top7_per_ID["Description_partial_x"].isna(),
        df_top7_per_ID["Description_partial_y"],
        df_top7_per_ID["Description_partial_x"],
    )
    df_top7_per_ID["Description_partial"] = np.where(
        df_top7_per_ID["Description_partial_y"].isna(),
        df_top7_per_ID["Description_partial_x"],
        df_top7_per_ID["Description_partial_y"],
    )
    df_top7_per_ID = df_top7_per_ID.drop(
        columns=["Description_partial_x", "Description_partial_y", "ID_x"]
    )

    df_top7_per_ID = df_top7_per_ID[
        [
            "ID_y",
            "Product_identifier",
            "Description_partial",
            "PartNumber",
            "Description_full",
            "Material",
            "Matching_based_on",
            "matching_percentage",
            "Base Unit of Measure",
        ]
    ]
    df_top7_per_ID.columns = [
        "ID",
        "Product Identifier",
        "Product Identifier Description",
        "Part Number",
        "Part Number Description",
        "Material",
        "Matching Based On",
        "Matchingc Percentage",
        "Base Unit of Measure",
    ]
    df_top7_per_ID["Material"] = df_top7_per_ID["Material"].astype(str)
    df_top7_per_ID["Matchingc Percentage"] = df_top7_per_ID[
        "Matchingc Percentage"
    ].round(2)
    return df_top7_per_ID


def main(country, n_chunk, file_to_match, full_inventory):
    start_time = time.time()
    folder = os.getcwd()
    full_inventory = pd.read_csv(os.path.join(folder, country, full_inventory))
    list_MPN, list_SMN, full_inventory = inventory_preparation(full_inventory)
    
    to_match = pd.read_csv(os.path.join(folder, country, file_to_match)) #, encoding='ISO-8859-1'
    to_match = to_match_preparation(to_match)
    print("Dataframes prepared!")

    matching_procedure(to_match, list_MPN, "MPN", n_chunk, folder, country)
    matching_procedure(to_match, list_SMN, "SMN", n_chunk, folder, country)
    final_match = final_file_preparation(folder, country, to_match)
    final_match.to_csv(
        os.path.join(folder, country, "final_match.csv"), index=False, decimal=","
    )
    end_time = time.time()
    print("used time")
    print(end_time - start_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--country", type=str, help="Entity identifier")
    parser.add_argument("-n", "--n_chunk", type=int, help="Number of chunk")
    parser.add_argument("-f", "--file_to_match", type=str, help="File to match")
    parser.add_argument("-i", "--full_invetory", type=str, help="Full invetory")

    args = parser.parse_args()

    if not all([args.country, args.n_chunk, args.file_to_match, args.full_invetory]):
        print("All parameters must be passed")
        sys.exit(1)

    warnings.filterwarnings("ignore")
    main(args.country, args.n_chunk, args.file_to_match, args.full_invetory)
