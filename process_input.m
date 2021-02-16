function [input_file, dest_id, info_category, sheets, id_col, delimiter, first_row] = process_input(config_file_path)


config_file_path = '/flywheel/v0/config.json'
%config_file_path = '/home/davidparker/Documents/Gears/BCH_Wrap/GGR-recon-main/config.json'

% Load configuration file json
if exist(config_file_path, 'file')
    raw_config = jsondecode(fileread(config_file_path));
else
    fprintf('No Config File Found.  Exiting \n')
    exit()
end

%% Get Inputs

% Check for inputs:
input_file = raw_config.inputs.input_file.location.path
dest_id = raw_config.destination.id
   

%% Get sheets
if isfield(raw_config.config,'sheets')
    val = raw_config.config.sheets
    sheets = split(val,",")
    for n = 1:numel(val)
        sheets{n} = strtrim(sheets{n})
    end
end

%% Get subject ID column
if isfield(raw_config.config,'subjectID_Column')
    id_col = raw_config.config.subjectID_Column
    
else
    fprintf("Missing required input subjectID_Column")
    exit()    
end

%% Get first row
if isfield(raw_config.config,'first_row')
    first_row = raw_config.config.first_row
    
else
    fprintf("Missing required input first_row")
    exit()    
end

%% Get delimiter
if isfield(raw_config.config,'delimiter')
    delimiter = raw_config.config.first_row
    
else
    fprintf("Missing required input delimiter")
    exit()    
end

%% Get delimiter
if isfield(raw_config.config,'info_category')
    info_category = raw_config.config.first_row
    
else
    fprintf("Missing required input delimiter")
    exit()    
end

end

