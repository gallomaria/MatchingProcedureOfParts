# Mapping
## AUTOMATED PROCESS
The project aims to find out whether in a given list of spare parts, those parts are already registered in the system.

## Table of Contents
- [The Data](#The-Data)
- [Implementation](#Implementation)
    - [Translation](#Translation)
    - [Matching](#Matching)
    - [Requirements](#Requirements)

## The Data
A complete list of all pieces registered in the system is given. The following details are provided for each part:
- Two different IDs: _Mfr Part Number_ and _Supplier Mat. No_
- English description of the part
- Material Code
- Base Unit

The list of spare parts is recived from an entity. The list includes:
- Product identifier
- Description in the local language

## Implementation
### Translation
When the Description of the parts is not in English a translation procedure is needed:
This is done through an API connection to DeepL, after a preparation of the data. 

### Matching


### Requirements
- Python 3+
- [requests](https://pypi.org/project/requests/)
- 
