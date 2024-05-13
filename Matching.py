"""
Script for matching the spare parts lists

Parameters to Adjust:
- entity: The entity identifier
- n_chunk: Number of chunks to avoid memory issues
- folder_path: The path to the folder containing the folder named as the entity
- file_to_match: File with columns: 'ID', 'Productidentifier', 'Item Description'
- full_invetory: File with columns: 'Mfr Part Number', 'Supplier Mat. No.', 'Material Description', 'Material', 'Base Unit of Measure'
- match_on_Mfr_Part_Number: Filename for match on Mfr Part Number
- match_on_Supplier_Mat_No: Filename for match on Supplier Mat. No.
- final_match: Filename for the final merged match

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


entity = ''
n_chunk = 1                                                         

# File to read
folder_path = ''                        # Update this path to your folder
file_to_match = ''                      # File with columns: 'ID', 'Productidentifier', 'Item Description'
full_invetory = ''                      # File with columns: 'Mfr Part Number', 'Supplier Mat. No.', 'Material Description', 'Material', 'Base Unit of Measure'                              

# File to write
match_on_Mfr_Part_Number = 'match_on_Mfr_Part_No'
match_on_Supplier_Mat_No = 'match_on_Supplier_Mat_No'
final_match = 'FINAL_MATCH_' + str(entity).replace('/', '') + '.csv'


###########################################################################################################################

def clean_str(stringa):
    # Clean a string by removing non-alphanumeric characters and leading zeros
    nuova_stringa = re.sub(r'[^a-zA-Z0-9\s]|[\W_]+', '', stringa)
    nuova_stringa = re.sub(r'[-.,/\\]', '', nuova_stringa)
    nuova_stringa = nuova_stringa.lstrip('0')
    return nuova_stringa


def rm_final_letters(stringa):
    # Remove final non-numeric characters from a string
    if stringa.isalpha():  
        return stringa  
    nuova_stringa = re.sub(r'[^0-9]+$', '', stringa)
    return nuova_stringa


def find_match(df):
    #find match if ID_partial is icluded in ID_full
    for index, row in df.iterrows():
        if str(row['ID_partial']) in str(row['ID_full']):
            df.at[index, 'IND'] = 1
        else:
            df.at[index, 'IND'] = 0
    return df


def matching_percentage(string1, string2):
    # Calculate matching percentage between two descriptions
    string1 = str(string1)
    string1 = string1.lower()
    string1 = re.sub(r'[^\w\s]', '', string1)
    string1 = string1.split()
    string1 = sorted(string1)

    string2 = str(string2)
    string2 = string2.lower()
    string2 = re.sub(r'[^\w\s]', '', string2)
    string2 = string2.split()
    string2 = sorted(string2)



    result = [[0 for _ in range(len(string2))] for _ in range(len(string1))]
    for i in range(len(string1)):
            for j in range(len(string2)):
                if len(string1[i]) >= len(string2[j]):
                    long = string1[i]
                    short = string2[j]
                else:
                    short = string1[i]
                    long = string2[j]
                
                if short == long:
                    result[i][j] = 1
                elif short in long:
                    result[i][j] = 0.5
                    
    results = pd.DataFrame(result)
    total_sum = results.values.sum()
    if results.shape[0] >= results.shape[1]:
        dim = results.shape[0]
    else:
        dim = results.shape[1]
    matching_perc = total_sum/dim*100
    return matching_perc


def levenshtein_distance(row):
    # Calculate Levenshtein distance between two strings
    dist = Levenshtein.distance(str(row['ID_partial']), str(row['ID_full']))
    return dist

###############################################################################################################################


# Data Preparation
full_list_inventory = pd.read_csv(os.path.join(folder_path, full_invetory))
full_list_inventory = full_list_inventory[['Mfr Part Number', 'Material', 'Material Description', 'Supplier Mat. No.', 'Base Unit of Measure']]

# Data for match on Mfr Part Number
list_MPN = full_list_inventory[['Mfr Part Number', 'Material', 'Material Description', 'Base Unit of Measure']]
list_MPN.columns = ['Mfr Part Number', 'Material', 'Description_full', 'Base Unit of Measure']
list_MPN = list_MPN.dropna(subset=['Mfr Part Number'])
# Prepare the ID for the matching
list_MPN['Mfr Part Number'] = list_MPN['Mfr Part Number'].astype(str)
list_MPN['ID_full'] = list_MPN['Mfr Part Number'].apply(clean_str)
list_MPN = list_MPN.drop_duplicates()

# Data for match on Supplier Mat. No.
list_SMN = full_list_inventory[['Supplier Mat. No.', 'Material', 'Material Description', 'Base Unit of Measure']]
list_SMN.columns = ['Supplier Mat. No.', 'Material', 'Description_full', 'Base Unit of Measure']
list_SMN = list_SMN.dropna(subset=['Supplier Mat. No.'])
# Prepare the ID for the matching
list_SMN['Supplier Mat. No.'] = list_SMN['Supplier Mat. No.'].astype(str)
list_SMN['ID_full'] = list_SMN['Supplier Mat. No.'].apply(clean_str)
list_SMN = list_SMN.drop_duplicates()

# Data to match
data = pd.read_csv(os.path.join(folder_path, entity, file_to_match))
data = data[['ID', 'Productidentifier', 'Item Description']]
data.columns = ['ID', 'Product_identifier', 'Description_partial']
data = data.dropna(subset=['Product_identifier'])
# Prepare the ID for the matching
data['Product_identifier'] = data['Product_identifier'].astype(str)
data['ID_partial'] = data['Product_identifier'].apply(clean_str)
data['ID_partial'] = data['ID_partial'].apply(rm_final_letters)
data = data.drop_duplicates()



# FIRST MATCHING on Mfr Part Number

data_chunks = np.array_split(data, n_chunk)
# Create the folder to save the files
os.makedirs(os.path.join(folder_path, entity, 'MPN')) 

for i, chunk in enumerate(data_chunks, start=1):
    merged_data = pd.merge(chunk, list_MPN, how='cross')
    merged_data['IND'] = 0

    # Find matches in ID using ThreadPoolExecutor 
    with ThreadPoolExecutor() as executor:
       results1 = list(executor.map(find_match, [merged_data]))

    MPN_Match = results1[0]
    MPN_Match = MPN_Match[MPN_Match['IND']==1]
    MPN_Match = MPN_Match.drop(columns = 'IND')

    MPN_Match['Description_full'] = np.where(MPN_Match['Description_full'].isna(), '', MPN_Match['Description_full'])
    # Compute the matching percentage
    MPN_Match['matching_percentage'] = MPN_Match.apply(lambda row: matching_percentage(row['Description_partial'], row['Description_full']), axis=1)
    MPN_Match['Matching_based_on'] = np.where(MPN_Match['matching_percentage'] != 0, 'Mfr Part Number', '')
    # Compute the measure of distance between IDs
    MPN_Match['ID_similarity'] = MPN_Match.apply(levenshtein_distance, axis=1)
    # Save partial match
    match_on_Mfr_Part_Number_chunck = match_on_Mfr_Part_Number + f'chunk_{i}.csv'
    MPN_Match.to_csv(os.path.join(folder_path, entity, 'MPN/', match_on_Mfr_Part_Number_chunck), index = False)
    print(i)



# SECOND MATCHING on Supplier Mat. No.

# Create the folder to save the files
os.makedirs(os.path.join(folder_path, entity, 'SMN')) 

for i, chunk in enumerate(data_chunks, start=1):
    merged_data = pd.merge(chunk, list_SMN, how='cross')
    merged_data['IND'] = 0

    # Find matches in ID using ThreadPoolExecutor 
    with ThreadPoolExecutor() as executor:
       results1 = list(executor.map(find_match, [merged_data]))

    SMN_Match = results1[0]
    SMN_Match = SMN_Match[SMN_Match['IND']==1]
    SMN_Match = SMN_Match.drop(columns = 'IND')

    SMN_Match['Description_full'] = np.where(SMN_Match['Description_full'].isna(), '', SMN_Match['Description_full'])
    # Compute the matching percentage
    SMN_Match['matching_percentage'] = SMN_Match.apply(lambda row: matching_percentage(row['Description_partial'], row['Description_full']), axis=1)
    SMN_Match['Matching_based_on'] = np.where(SMN_Match['matching_percentage'] != 0, 'Mfr Part Number', '')
    # Compute the measure of distance between IDs
    SMN_Match['ID_similarity'] = SMN_Match.apply(levenshtein_distance, axis=1)
    # Save partial match
    match_on_Supplier_Mat_No_chunck = match_on_Supplier_Mat_No + f'chunk_{i}.csv'
    SMN_Match.to_csv(os.path.join(folder_path, entity, 'SMN/', match_on_Supplier_Mat_No_chunck), index = False)
    print(i)




# FINAL FILE PREPARATION

# Collect all the matching dataframe previously saved
MPN_files = []
for files_int in os.listdir(os.path.join(folder_path, entity, 'MPN/')):
    print(files_int)
    MPN_files.append(os.path.join(folder_path, entity, 'MPN/', files_int))

MPN_Match = pd.DataFrame()
for file_path in MPN_files:
    tmp = pd.read_csv(file_path, sep = ',', low_memory=False)
    MPN_Match = pd.concat([MPN_Match, tmp], ignore_index=True)

MPN_Match.rename(columns={'Mfr Part Number': 'PartNumber'}, inplace=True)


SMN_files = []
for files_int in os.listdir(os.path.join(folder_path, entity, 'SMN/')):
    print(files_int)
    SMN_files.append(os.path.join(folder_path, entity, 'SMN/', files_int))

SMN_Match = pd.DataFrame()
for file_path in SMN_files:
    tmp = pd.read_csv(file_path, sep = ',', low_memory=False)
    SMN_Match = pd.concat([SMN_Match, tmp], ignore_index=True)
    
SMN_Match.rename(columns={'Supplier Mat. No.': 'PartNumber'}, inplace=True)


# Concatenate  MPN and SMN data
df_merged_final = pd.concat([MPN_Match, SMN_Match], ignore_index=True)
df_merged_final = df_merged_final.drop(columns = ['ID_partial', 'ID_full'])
df_merged_final = df_merged_final.drop_duplicates()

# Select the 7 top matches accordingly to the matching percentage and the ID similarity
df_top7_per_ID = df_merged_final.groupby('Product_identifier').apply(lambda x: x.sort_values(by=['matching_percentage', 'ID_similarity'], ascending=[False, True]).head(7)).reset_index(drop=True)
df_top7_per_ID = df_top7_per_ID.drop(columns = 'ID_similarity')
df_top7_per_ID['Product_identifier'] = df_top7_per_ID['Product_identifier'].astype(str)
df_top7_per_ID = pd.merge(df_top7_per_ID, data[['ID', 'Product_identifier','Description_partial' ]], on = 'Product_identifier', how = 'outer')
df_top7_per_ID['Description_partial'] = np.where(df_top7_per_ID['Description_partial_x'].isna(), df_top7_per_ID['Description_partial_y'], df_top7_per_ID['Description_partial_x'])
df_top7_per_ID['Description_partial'] = np.where(df_top7_per_ID['Description_partial_y'].isna(), df_top7_per_ID['Description_partial_x'], df_top7_per_ID['Description_partial_y'])
df_top7_per_ID = df_top7_per_ID.drop(columns=['Description_partial_x', 'Description_partial_y', 'ID_x'])

df_top7_per_ID = df_top7_per_ID[['ID_y', 'Product_identifier', 'Description_partial', 'PartNumber', 'Description_full',
                                 'Material', 'Matching_based_on', 'matching_percentage', 'Base Unit of Measure']]
df_top7_per_ID.columns = ['ID', 'Product Identifier', 'Product Identifier Description', 'Part Number', 'Part Number Description', 'Material',
                            'Matching Based On', 'Matchingc Percentage', 'Base Unit of Measure']
df_top7_per_ID['Material'] = df_top7_per_ID['Material'].astype(str)
df_top7_per_ID['Matchingc Percentage'] = df_top7_per_ID['Matchingc Percentage'].round(2)

# Save final file
df_top7_per_ID.to_csv(os.path.join(folder_path, entity, final_match, ), index = False, decimal=',')


