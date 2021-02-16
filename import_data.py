import pandas as pd
from pathlib import Path
import logging


df_path = '/Users/davidparker/Documents/Flywheel/SSE/MyWork/Gears/UM_MetadataImport/MF_PreClin_Tracker_20201201.xlsx'
firstrow_spec = 14
sheets_spec = "MRIDataTracker"

mapping_levels = ['Subject', 'Session', 'Acquisition']

log = logging.getLogger("__main__")



def import_data(project, data, mapping):
    
    # check to see if each level has a map in the mapping object
    
    # While this could be done iteratively, I will take advantage of the hierarchy structure 
    # To shorten processing time
    
    nrows, ncols = data.shape
    log.info("Starting Mapping")
    if "Subject" in mapping:
        
        cmap = mapping["Subject"]
        label_col = cmap.label_column
        import_cols = cmap.import_columns
        
        if label_col not in data.keys():
            log.error(f"No Column {label_col} in data frame")
            raise Exception("Subject ID Column not found")

    if "Session" in mapping:
        ses_cmap = mapping["Session"]
        ses_label_col = ses_cmap.label_column
        ses_import_cols = ses_cmap.import_columns
        
        if ses_label_col not in data.keys():
            log.error(f"No Column {ses_label_col} in data frame")
            raise Exception("Subject ID Column not found")

    if "Acquisition" in mapping:
        acq_cmap = mapping["Acquisition"]
        acq_label_col = acq_cmap.label_column
        print(acq_label_col)
        acq_import_cols = acq_cmap.import_columns

        if acq_label_col not in data.keys():
            log.error(f"No Column {acq_label_col} in data frame")
            raise Exception("Subject ID Column not found")
        
        
    for nr in range(nrows):
        
        subject = upload_columns_from_dataframe_to_container_child(project,
                                                                   label_col,
                                                                   import_cols,
                                                                   data,
                                                                   nr)
        
        if subject is not None and "Session" in mapping:
            session = upload_columns_from_dataframe_to_container_child(subject,
                                                                       ses_label_col,
                                                                       ses_import_cols,
                                                                       data,
                                                                       nr)
            
            if session is not None and "Acquisition" in mapping:
                acq = upload_columns_from_dataframe_to_container_child(session,
                                                                       acq_label_col,
                                                                       acq_import_cols,
                                                                       data,
                                                                       nr)
    

def upload_columns_from_dataframe_to_container_child(container, label_col, columns, df, row):
        
        #print(df)
        map_data = df[columns].iloc[row].to_dict()
        
        map_data = {i: str(v) for i, v in map_data.items() if v}
        child_label = df[label_col][row]
        query = f"label=\"{child_label}\""
        update_info = {}
        child = get_child(container, query)
        
        if len(child) < 1:
            log.error(f"Container Type {container.container_type} returned no children")
            child = None
            
        else:
            child = child[0].reload()
            print(map_data)
            log.debug(f"Updating container {container.label}")
            child.update_info(map_data)
        
        return child
        


    
        
def get_child(container, query=None):
    
    ct = container.container_type
    log.info(query)
    if ct == "project":
        if query:

            return container.subjects.find(query)
        else:
            return container.subjects()
    
    elif ct == "subject":
        if query:
            return container.sessions.find(query)
        else:
            return container.sessions()
        
    elif ct == "session":
        if query:
            return container.acquisitions.find(query)
        else:
            return container.acquisions()
            
