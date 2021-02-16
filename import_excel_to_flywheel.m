javaaddpath('rest-client.jar')
fw = flywheel.Client()

%%
config_file_path = '/flywheel/v0/config.json'
input_file, dest_id, info_category, sheets, id_col, delimiter, first_row = process_input(config_file_path)

data = load_data(input_file, first_row, delimiter, sheets)

input_file = 'MF_PreClin_Tracker_20201201.xlsx';
subject_ID_column = 'MoustID';
[filepath,filename,fileext] = fileparts(input_file)
data_start = 1;
delimiter_spec = ',';
sheets_spec = ["sheet1", "sheet2", "sheet3"];


dest = fw.get(dest_id)
if strcmp(dest.containerType,'project')
    parent_project = dest
else
    parent_project = fw.getProject(dest.parents.project)
end

for i=1:numel(data)
    import_data(parent_project, data{i}, id_col, info_category)
end


    