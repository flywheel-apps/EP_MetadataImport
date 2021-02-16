function [data] = load_data(input_file, first_row, delimiter, sheets_spec)

input_file = 'MF_PreClin_Tracker_20201201.xlsx';
subject_ID_column = 'MoustID';
[filepath,filename,fileext] = fileparts(input_file)
data_start = 1;
delimiter_spec = ',';
sheets_spec = ["sheet1", "sheet2", "sheet3"];


if strcmp(fileext,'.csv')
    delimiter = ',';
    opts = detectImportOptions(input_file);
    opts.VariableNamesLine = first_row;
    opts.DataLines = first_row + 1;
    opts.Delimiter = delimiter;
    
    data{1} = readtable(input_file,opts,'ReadVariableNames',true);
    
                          
    
elseif strcmp(fileext,'.txt')
    delimiter = delimiter_spec;
    opts = detectImportOptions(input_file);
    opts.VariableNamesLine = first_row;
    opts.DataLines = first_row + 1;
    opts.Delimiter = delimiter;
    
    data{1} = readtable(input_file,opts,'ReadVariableNames',true);
                                  
elseif strcmp(fileext, '.xls') || strcmp(fileext, '.xlsx')
    
    [~,sheets] = xlsfinfo(input_file);
    
    if isempty(sheets) == 0
        sheets_spec = sheets
    end
        
    for i=1:length(sheets_spec)
        if ~any(strcmp(sheets,sheets_spec(i)))
            fprintf('Error, sheet name '+sheets_spec(i)+' Is not present in file \n')
            exit()
        end
    end
    
    
    
    for i=1:numel(sheets_spec)
        opts = spreadsheetImportOptions('DataRange', first_row,...
                                        'Sheet',sheets_spec(i))
        data{i} = readtable(input_file,opts)
    end
    
    
        
    
else
    fprintf('Error, invalid file type \n')
    fprintf('Valid types are ".txt", ".csv", ".xls", and ".xlsx" \n')
    exit()
end



end

