# PartsMatchingProcedure

The project aims to find out whether in a given list of spare parts, those parts are already registered in the system, the match is performed based on a transformed identifier and on the description of the part.

## Table of Contents
- [The Data](#The-Data)
- [Implementation](#Implementation)
    - [Translation](#Translation)
    - [Matching](#Matching)
    - [Requirements](#Requirements)

## The Data
A comprehensive list of all spare parts registered in the system is provided, containing the following details for each part:
- Two different IDs: _Mfr Part Number_ and _Supplier Mat. No_
- English description of the part
- Material Code
- Base Unit of Measure

The inventory of spare parts is received from an external entity, which includes:
- ID
- Product identifier
- Description in the local language

## Implementation
### Translation
When the description of the parts is not in English, a translation procedure is required. This is accomplished through an API connection to DeepL.

[Python script link](Inventory_Translation.py)

### Matching
The matching procedure begins with an initial comparison between the Product Identifier and the Mfr Part Number. For each partial match, a matching percentage is calculated based on the material description. The matched are then grouped by Product Identifier and arranged in order of similarity between the Product Identifier and the Mfr Part Number and percentage of description matching. 
The same procedure is then executed for the Supplier Mat. No. 

[Python script link](Matching.py)

### Requirements
- Python 3.8+
- [pandas](https://pandas.pydata.org/docs/)
- [numpy](https://github.com/numpy/numpy)
- [os](https://docs.python.org/3/library/os.html)
- [concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html)
- [requests](https://pypi.org/project/requests/)
- [Levenshtein](https://pypi.org/project/python-Levenshtein/)
