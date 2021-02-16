import os
import pandas as pd
from pathlib import Path

import flywheel

import import_data as id
import load_data as ld
import mapping_class as mc
import logging

logging.basicConfig(level=logging.DEBUG)

def main(project, yaml_file, excel_file):
    
    mapping = ld.load_yaml(yaml_file)
    starting_row = 14
    
    df = ld.load_excel_dataframe(excel_file,starting_row, "MRIDataTracker")
    id.import_data(project, df, mapping)
    


if __name__ == "__main__":
    api_key = os.environ['API_KEY']
    
    fw = flywheel.Client(api_key)
    
    project_id = '5daa044a69d4f3002a16ea93'
    project = fw.get_project(project_id)
    
    yaml_file = "/Users/davidparker/Documents/Flywheel/SSE/MyWork/Gears/UM_MetadataImport/demo_yaml.yaml"
    excel_file = "/Users/davidparker/Documents/Flywheel/SSE/MyWork/Gears/UM_MetadataImport/PreClinical_UploadExample.xlsx"
    
    main(project, yaml_file, excel_file)