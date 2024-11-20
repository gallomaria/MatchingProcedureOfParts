"""
 SCRIPT  FOR TRANSLATING TEXT DATA USING THE DEEPL API

Parameters to Adjust:
- Deepl_Key: Your DeepL API key
- country: The entity identifier
- input_lang: The language of the input text
- file_to_translate: The name of the input file with text to be translated

Usage:
1. Set the parameters to adjust according to your needs.
2. Run the script to translate text data using the DeepL API.
"""

import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import requests
import os
import argparse
import sys
import warnings


def translate_deepl(text, input_lang, deepl_key):
    url = "https://api.deepl.com/v2/translate"

    params = {
        "auth_key": deepl_key,
        "text": text,
        "target_lang": "EN",
        "source_lang": input_lang,
    }

    response = requests.post(url, data=params)
    if response.status_code == 200:
        traduzione = response.json()["translations"][0]["text"]
        return traduzione
    else:
        print("ERROR")
        return ""


def main(country, input_lang, deepl_key, file_to_translate):
    folder = os.getcwd()
    to_translate = pd.read_csv(os.path.join(folder, country, file_to_translate))
    to_translate.columns = ["ID", "Material", "Material Description"]

    with ThreadPoolExecutor() as executor:
        results = list(
            executor.map(
                lambda desc: translate_deepl(desc, input_lang, deepl_key),
                to_translate["Material Description"],
            )
        )

    to_translate["Translation"] = results
    to_translate = to_translate[["ID", "Material", "Translation"]]
    to_translate.columns = ["ID", "product_identifier", "item_description"]
    # Save translated file
    print("File translated")
    to_translate.to_csv(os.path.join(folder, country, "to_match.csv"), index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--country", type=str, help="Entity identifier")
    parser.add_argument("-i", "--input_lang", type=str, help="Input language")
    parser.add_argument("-d", "--deepl_key", type=str, help="Deepl key")
    parser.add_argument(
        "-f", "--file_to_translate", type=str, help="File to translate"
    )

    args = parser.parse_args()

    if not all([args.country, args.input_lang, args.deepl_key, args.file_to_translate]):
        print("All parameters must be passed")
        sys.exit(1)

    warnings.filterwarnings("ignore")
    main(args.country, args.input_lang, args.deepl_key, args.file_to_translate)

