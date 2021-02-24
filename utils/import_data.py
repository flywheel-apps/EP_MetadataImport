import collections.abc
import numpy as np

import logging

from utils import flywheel_helpers as fh

# df_path = '/Users/davidparker/Documents/Flywheel/SSE/MyWork/Gears/Metadata_import_Errorprone/Data_Entry_2017_test.csv'
# firstrow_spec = 1
# 
# sheets_spec = "MRIDataTracker"

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
                update_data = update(current_info, data, overwrite)
                match.update_info(update_data)
                df.loc[df.index == row, 'Gear_Status'] = 'Success'
        
        except Exception as e:
            log.warning(f"row {row} unable to process for reason: {e}")
            log.exception(e)
    
    return df
        
        

def save_df_to_csv(df, output_dir):
    output_path = output_dir/'Data_Import_Status_report.csv'
    df.to_csv(output_path, index=False)


def update(d, u, overwrite):
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = update(d.get(k, {}), v, overwrite)
        else:
            # Flywheel doesn't like np.bool_ type.  
            if isinstance(v, np.bool_):
                v = bool(v)
                
            if k in d:
                if overwrite:
                    
                    d[k] = v
            else:
                d[k] = v
        
    return d