from pathlib import Path
import sys

import flywheel
import flywheel_gear_toolkit as gt

from utils import load_data as ld, import_data as id


def main(context):
    
    #fw = flywheel.Client()
    config = context.config
    for inp in context.config_json["inputs"].values():
        if inp["base"] == "api-key" and inp["key"]:
            api_key = inp["key"]

    fw = flywheel.Client(api_key)
    
    # Setup basic logging and log the configuration for this job
    if config["gear_log_level"] == "INFO":
        context.init_logging("info")
    else:
        context.init_logging("debug")
    context.log_config()
    log = context.log
    
    csv_file = context.get_input_path('csv_file')
    
    if csv_file is None or not Path(csv_file).exists():
        log.error('No file provided or file does not exist')
        return 1
    csv_file = Path(csv_file)
    
    dry_run = config.get('dry-run', False)
    log.debug(f"dry_run is {dry_run}")
    
    first_row = config.get('first_row', 0)
    log.debug(f"Data starting on row{first_row}")
    
    metadata_destination = config.get("metadata_destination", "info")
    log.debug(f"Saving metadata to {metadata_destination}")
    
    mapping_column = config.get("mapping_column", 0)
    log.debug(f"Using column {mapping_column} to identify objects")
    
    overwrite = config.get("overwrite", False)
    log.debug(f"Overwrite set to {overwrite}")
    
    delimiter = config.get("delimiter", ",")
    log.debug(f"Using Delimiter: {delimiter}")
    
    object_type = config.get("object_type")
    log.debug(f"Looking for matching labels for container type {object_type}")
    
    attached_files = config.get('attached_files')
    log.debug(f"looking for files attached to container type {attached_files}")
    
    destination_level = context.destination.get('type')
    if destination_level is None:
        log.error(f"invalid destination {destination_level}")
        return 1
    
    destination_id = context.destination.get('id')
    dest_container = fw.get(destination_id)
    
    df = ld.load_text_dataframe(csv_file, first_row, delimiter)
    mapping_column = ld.validate_df(df, mapping_column)
    
    objects_for_processing = id.get_objects_for_processing(fw,
                                                           dest_container,
                                                           object_type,
                                                           attached_files)
    
    df = id.import_data(fw,
                        df,
                   mapping_column,
                   objects_for_processing,
                   attached_files,
                   metadata_destination,
                   overwrite,
                   dry_run)
    
    report_output = context.output_dir
    id.save_df_to_csv(df, report_output)
    


if __name__ == "__main__":
    
    result = main(gt.GearToolkitContext())
    sys.exit(result)

    # project_id = '5daa044a69d4f3002a16ea93'
    # project = fw.get_project(project_id)
    # 
    # yaml_file = "/Users/davidparker/Documents/Flywheel/SSE/MyWork/Gears/UM_MetadataImport/demo_yaml.yaml"
    # excel_file = "/Users/davidparker/Documents/Flywheel/SSE/MyWork/Gears/UM_MetadataImport/PreClinical_UploadExample.xlsx"
    # 
    # main(project, yaml_file, excel_file)