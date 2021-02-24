import collections.abc

import pandas as pd
from pathlib import Path
import logging

import flywheel_helpers as fh

df_path = '/Users/davidparker/Documents/Flywheel/SSE/MyWork/Gears/Metadata_import_Errorprone/Data_Entry_2017_test.csv'
firstrow_spec = 1

sheets_spec = "MRIDataTracker"

mapping_levels = ['Subject', 'Session', 'Acquisition']

log = logging.getLogger("__main__")


def expand_metadata(meta_string, container):
    metas = meta_string.split('.')
    ct = container.container_type
    name = fh.get_name(container)

    first = metas.pop(0)
    val = getattr(container, first)
    temp_container = val
    for meta in metas:
        val = temp_container.get(meta)
        if val:
            temp_container = val
        else:
            log.warning(f'No metadata value {meta_string} found for {ct} {name}')
            return (None)
    return (val)
    
    


def get_objects_for_processing(fw, destination_container, level, get_files):
    
    log.debug(f"looking for {level} on container {destination_container.label}.  Files: {get_files}")
    
    project = destination_container.parents.project
    project = fw.get(project).reload()
    child_containers = fh.get_containers_at_level(fw, project, level)
    if get_files:
        resulting_containers = []
        for cc in child_containers:
            resulting_containers.extend(fh.get_containers_at_level(fw, cc, "file"))
        
    else:
        resulting_containers = child_containers
    
    return resulting_containers
    
    


def import_data(fw,
                df,
                mapping_column,
                objects_for_processing,
                get_files=False,
                metadata_destination="info",
                overwrite=False,
                dry_run=False):
    
    status_log = []
    
    if get_files:
        name = 'name'
    else:
        name = 'label'
        
    nrows, ncols = df.shape
    log.info("Starting Mapping")
    # object_names = [o.get(name) for o in objects_for_processing]
    
    df['Gear_Status'] = 'Failed'
    df['Gear_FW_Location'] = None
    
    for row in range(nrows):
        
        try:
            upload_obj = df.iloc[row]
            object_name = upload_obj.get(mapping_column)
            matches = [m for m in objects_for_processing if m.get(name) == object_name]
            
            if len(matches) > 1:
                log.warning(f"Multiple matches for for object name '{object_name}'. "
                            f"please get better at specifying flywheel objects.")
                continue
                
            elif len(matches) == 0:
                log.warning(f"No match for object name '{object_name}'.")
                continue
            
            match = matches[0]
            current_info = match.info
            
            address = fh.generate_path_to_container(fw, match)
            df.loc[df.index == row, 'Gear_FW_Location'] = address
            
            data = upload_obj.to_dict()
            data.pop(mapping_column)
            levels = metadata_destination.split('.')
            
            if levels[0] == "info":
                levels.pop(0)
            
            while levels:
                info = dict()
                info[levels.pop(-1)] = data
                data = info
            
            if dry_run:
                log.info(f"Would modify info on {address}")
                df.loc[df.index == row, 'Gear_Status'] = 'Dry-Run Success'
            else:
                print(current_info)
                update_data = update(current_info, data, overwrite)
                print(update_data)
                print(type(update_data))
                print(match)
                match.update_info(update_data)
                df.loc[df.index == row, 'Gear_Status'] = 'Success'
        
        except Exception as e:
            log.warning(f"row {row} unable to process for reason: {e}")
            log.exception(e)
    
    return df
        
        
        
        
        
        
        
        
            
        
    
    
    
    




def import_data_old(project, data, mapping):
    
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


def upload_columns_from_dataframe_to_container_child_old(container, label_col, columns, df, row):
    # print(df)
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
            


def save_df_to_csv(df, output_dir):
    output_path = output_dir/'Data_Import_Status_report.csv'
    df.to_csv(output_path, index=False)


def update(d, u, overwrite):
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = update(d.get(k, {}), v, overwrite)
        else:
            if k in d:
                if overwrite:
                    d[k] = v
            else:
                d[k] = v
        
    return d