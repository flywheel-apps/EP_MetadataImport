import pandas as pd
from pathlib import Path
import xlrd
import yaml
import mapping_class as mc



# xlrd use to verify that sheets provided exist in the excel file.

df_path = '/Users/davidparker/Documents/Flywheel/SSE/MyWork/Gears/UM_MetadataImport/MF_PreClin_Tracker_20201201.xlsx'
firstrow_spec = 14
sheets_spec = "MRIDataTracker"


def load_excel_dataframe(excel_path, firstrow_spec, sheets_spec=0):
    
    # First iteration only supports single sheet import, but sheet can be specified.
    
    
    df = pd.read_excel(excel_path, header=firstrow_spec-1, sheet_name=sheets_spec)
    return(df)

def load_text_dataframe(df_path, firstrow_spec, delimiter_spec):
    
    df = pd.read_table(df_path, delimiter=delimiter_spec, header=firstrow_spec-1)

    return(df)

def load_yaml(yaml_path):
    with open(yaml_path) as file:
        import_dict = yaml.load(file, Loader=yaml.FullLoader)
        
    for key in import_dict:
        import_dict[key] = mc.mapping_object(import_dict[key])
        
    return import_dict