"""
 SCRIPT  FOR TRANSLATING TEXT DATA USING THE DEEPL API

Parameters to Adjust:
- Deepl_Key: Your DeepL API key
- entity: The entity identifier
- input_lang: The language of the input text
- folder_path: The path to the folder containing the folder named as the entity
- file_to_translate: The name of the input file with text to be translated
- file_translated: The name of the output file with translated text

Usage:
1. Set the parameters to adjust according to your needs.
2. Run the script to translate text data using the DeepL API.
"""

import pandas as pd 
from concurrent.futures import ThreadPoolExecutor
import requests
import os


Deepl_Key = ''
entity = ''
input_lang = ''

# File to read
folder_path = 'C:/Users/mgallo/PythonScript/DescriptionMatching/'     # Update this path to your folder
file_to_translate = 'KES Parts Inventory.csv'                         # File with columns: 'ID', 'Material', 'Material Description'  

# File to write 
file_translated = 'KES Parts Inventory_EN.csv'


###############################################################################################################################

def translate_deepl(text):
    url = 'https://api.deepl.com/v2/translate'

    params = {
        'auth_key': Deepl_Key,
        'text': text,
        'target_lang': "EN",
        'source_lang': input_lang
    }

    response = requests.post(url, data=params) 
    if response.status_code == 200:
        traduzione = response.json()['translations'][0]['text']
        return traduzione
    else:
        print("ERROR")
        return ''

################################################################################################################################
    
    
# Read file to translate
to_translate = pd.read_csv(os.path.join(folder_path, entity, file_to_translate))
to_translate.columns = ['ID', 'Material', 'Material Description']

# Translate text using ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
    results = list(executor.map(translate_deepl, to_translate['Material Description']))

# Add translated text to DataFrame
to_translate['Translation'] = results
to_translate = to_translate[['ID', 'Material', 'Translation']]
to_translate.columns = ['ID', 'Productidentifier', 'Item Description']
# Save translated file
to_translate.to_csv(os.path.join(folder_path, entity, file_translated))



