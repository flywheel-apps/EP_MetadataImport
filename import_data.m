function import_data(parent_project, data, id_col, info_category)


variables = data.Properties.VariableNames
variables(ismember(variables,id_col))=[]

% Build metadata object for subject.

for i=1:size(data,1)
    subject_id = data.(id_col)(i)
    
    subject_info = struct()
    
    for v=1:numel(variables)
        subject_info.(variables{v}) = data.(variables{v})(i)
    end
    
    if ~strcmp(info_category,'')
        subject_info = struct(info_category,subject_info)
    end
    
    try
        subject = parent_project.subjects.find(['label=',subject_id])
        subject = subject{1}
        subject.updateInfo(subject_info)
    catch
        warning("Error updating subject "+subject_id)
    end
    
    

end

