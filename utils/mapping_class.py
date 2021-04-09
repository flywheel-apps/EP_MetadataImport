import re
import utils.flywheel_helpers as fh
import logging
import collections
import flywheel
import utils.import_data as id
import numpy as np
import pandas as pd

log = logging.getLogger()



class FlywheelObjectFinder:
    def __init__(self, fw, group=None, project=None, subject=None, session=None,
                 acquisition=None, analysis=None,
                 file=None, level=None):
        
        self.client = fw

        
        self.level = None
        self.highest_level = None
        
        self.group = {"id": group,
                        "obj": None,
                        "parent": None,
                        "type": "group"}

        self.project = {"id": project,
                        "obj": None,
                        "parent": self.group,
                        "type": "project"}
        
        self.subject = {"id": subject,
                        "obj": None,
                        "parent": self.project,
                        "type": "subject"}
        
        self.session = {"id": session,
                        "obj": None,
                        "parent": self.subject,
                        "type": "session"}
        
        self.acquisition = {"id": acquisition,
                        "obj": None,
                        "parent": self.session,
                        "type": "acquisition"}
        
        self.file = {"id": file,
                        "obj": None,
                        "parent": self.highest_level,
                        "type": "file"}
        
        self.analysis = {"id": analysis,
                        "obj": None,
                        "parent": self.highest_level,
                        "type": "analysis"}
        
        
        self.level = level
        self.object = None
        self.highest_container = None
    
    
    def check_for_file(self, container):
        
        if self.file is not None:
            ct = container.get('container_type', 'analysis')
            
            if self.level is not None and self.level == ct:
                files = [f for f in container.files if f.name == self.file]
            
            else:
                files = [f for f in container.files if f.name == self.file]
        
        else:
            files = []
        
        return files


    def check_for_analysis(self, container):

        if self.analysis is not None:
            
            ct = container.get('container_type', 'analysis')
            if self.level is not None and self.level == ct:
                analyses = [a for a in container.analyses if a.label == self.analysis]

            elif ct is not "analysis":
                analyses = [a for a in container.analyses if a.label == self.analysis]

        else:
            analyses = []

        return analyses

    def find_groups(self):
        
        if self.group_str:
            if self.group_str.islower():
                try:
                    group = [self.client.get_group(self.group_str)]
                except flywheel.ApiException:
                    log.debug(f"group name {self.group_str} is not an ID.")

            group = fh.run_finder_at_level(self.client, None, 'group', self.group_str)
            
            if group is None or group is []:
                log.error(f"Unable to find a group with a label or ID {self.group_str}")
                group = None
                # raise Exception(f"Group {self.group} Does Not Exist")
                
            self.group_obj = group
            self.highest_level = 'group'
    
    
    
    def find_object(self, object_type, from_container):
        
        working_object = getattr(self, from_container)
        parent = working_object.get('parent')
        
        
        if parent.get("obj") is None:
            
            if parent.get("parent") is None:
                object = fh.find_flywheel_container(self.client, working_object.get("id"), object_type, None)
            else:
                object = self.find_object(object_type, parent.get("type"))
        
        
        else:
            object = []
            for parent in parent.get("obj"):
                object.extend(
                    fh.find_flywheel_container(self.client, working_object.get("id"), working_object.get("type"), parent))
                
        return object
    
        
        
         
        if self.project_str:
            
            if not self.group_obj:
                # fw, name, level, on_container = None
                project = fh.find_flywheel_container(self.client, self.project_str, 'project', None)
            else:
                project = []
                for parent in self.group_obj:
                    project.extend(fh.find_flywheel_container(self.client, self.project_str, 'project', parent))
            
            if project is None or project is []:
                log.error(f"Unable to find a group with a label or ID {self.group_str}")
                project = None
                
            self.project_obj = None
            self.highest_level = 'project'
            
        
    def find_subjects(self):
        
        if self.subject_str:
            
            if not self.project_obj:
    
                if not self.group_obj:
                    subjects = fh.find_flywheel_container(self.client, self.subject_str, 'subject', None)
                
                else:   
                    subjects = []
                    for parent in self.group_obj:
                        subjects.extend(fh.find_flywheel_container(self.client, self.subject_str, 'subject', parent))
            
            else:
                subjects = []
                for parent in self.project_obj:
                    subjects.extend(
                        fh.find_flywheel_container(self.client, self.subject_str, 'subject',
                                                   parent))
            
            
        
        
    def find_project(self):
        
        if self.project:
            project = fh.find_flywheel_container(self.client, self.project, 'project',
                                                 on_container=self.highest_container)
            if project is None:
                log.error(f"Unable to find a project with a label or ID {self.project}")
                # raise Exception(f"Project {self.project} Does Not Exist")

            self.project = project.reload()
            highest_container = project.reload()
            fw_object = project
            

    def find_type(self, container_type):
        
        if getattr(self, container_type):
            container = fh.find_flywheel_container(self.client, getattr(self, container_type), container_type,
                                               on_container=self.highest_container)
            if container is None:
                log.error(f"Unable to find a {container_type} with a label or ID {getattr(self, container_type)}")
                # raise Exception(f"Group {self.group} Does Not Exist")
            else:
                


    def find(self, retry=False):

        highest_container = [None]
        




        if self.subject:
            subject = fh.find_flywheel_container(self.client, self.subject, 'subject',
                                                 on_container=highest_container)
            if subject is None:
                log.error(f"Unable to find a subject with a label or ID {self.subject}")
                # raise Exception(f"Subject {self.subject} Does Not Exist")

            self.subject = subject.reload()
            highest_container = subject.reload()
            fw_object = subject

        if self.session:
            session = fh.find_flywheel_container(self.client, self.session, 'session',
                                                 on_container=highest_container)
            if session is None:
                log.error(f"Unable to find a session with a label or ID {self.session}")
                # raise Exception(f"session {self.session} Does Not Exist")

            self.session = session.reload()
            highest_container = session.reload()
            fw_object = session

        if self.acquisition:
            acquisition = fh.find_flywheel_container(self.client, self.acquisition, 'acquisition',
                                                     on_container=highest_container)
            if acquisition is None:
                log.error(f"Unable to find a acquisition with a label or ID {self.acquisition}")
                # raise Exception(f"acquisition {self.acquisition} Does Not Exist")

            self.acquisition = acquisition.reload()
            highest_container = acquisition.reload()
            fw_object = acquisition

        if self.analysis:
            analysis = fh.find_flywheel_container(self.client, self.analysis, 'analysis',
                                                  on_container=highest_container)
            if analysis is None:
                log.error(f"Unable to find a analysis with a label or ID {self.analysis}")
                # raise Exception(f"analysis {self.analysis} Does Not Exist")

            self.analysis = analysis.reload()
            highest_container = analysis.reload()
            fw_object = analysis

        if self.file:
            
            
            file = fh.find_flywheel_container(self.client, self.file, 'file',
                                              on_container=highest_container)
            if file is None:
                log.error(f"Unable to find a file with a label or ID {self.file}")
                # raise Exception(f"file {self.file} Does Not Exist")
                fw_object = None
            else:
                self.file = file
                fw_object = file

        return fw_object


class DataMap:
    def __init__(self, fw, data, group=None, project=None, subject=None, session=None, acquisition=None, analysis=None,
                 file=None, info=False, namespace=''):
        self.group = group
        self.project = project
        self.subject = subject
        self.session = session
        self.acquisition = acquisition
        self.analysis = analysis
        self.file = file
        self.data = data
        self.info = info
        self.fw = fw
        self.namespace = namespace
        highest_container = None
        

    
    def write_metadata(self, overwrite = False):
        
        fw_object = self.find_flywheel_object()
        if fw_object is not None:
            update_dict = self.make_meta_dict()
            object_info = fw_object.get('info')
            if not object_info:
                object_info = dict()
            
            print(object_info)
            object_info = id.update(object_info, update_dict, overwrite)
            print(object_info)
            fw_object.update_info(object_info)
        
    
    def make_meta_dict(self):
        
        output_dict = {self.namespace: self.data.to_dict()}
        output_dict = cleanse_the_filthy_numpy(output_dict)
        return output_dict
        
        

def cleanse_the_filthy_numpy(dict):
    """change inputs that are numpy classes to python classes

    when you read a csv with Pandas, it makes "int" "numpy_int", and flywheel doesn't like that.
    Does the same for floats and bools, I think.  This fixes it

    Args:
        dict (dict): a dict

    Returns:
        dict (dict): a dict made of only python-base classes.

    """
    for k, v in dict.items():
        if isinstance(v, collections.abc.Mapping):
            dict[k] = cleanse_the_filthy_numpy(dict.get(k, {}))
        else:
            # Flywheel doesn't like numpy data types:
            if type(v).__module__ == np.__name__:
                v = v.item()
                dict[k] = v
    return dict
        

            
        
    

        



class MetadataMapper:
    def __init__(self, group_column=None, project_column=None, subject_column=None, session_column=None, acquisition_column=None,
                 analysis_column=None, file_column=None, import_columns='ALL'):
        
        self.subject_column = subject_column
        self.session_column = session_column
        self.acquisition_column = acquisition_column
        self.analysis_column = analysis_column
        self.file_column = file_column
        self.project_column = project_column
        self.group_column = group_column
        self.import_columns = import_columns
        self.fw = flywheel.Client()
    
    def map_data(self, data, namespace):
        """
        
        Args:
            data (pandas.Dataframe): a dataframe containing metadata to import

        Returns: mappers (list): a list of mapped metadata items

        """
        data.fillna('', inplace=True)
        nrows, ncols = data.shape
        log.info("Starting Mapping")

        data['Gear_Status'] = 'Failed'
        data['Gear_FW_Location'] = None

        success_counter = 0
    
        mappers = []
        for row in range(nrows):
            data_row = data.iloc[row]
            #print(data_row)
            group = panda_pop(data_row, self.group_column)
            project = panda_pop(data_row, self.project_column)
            subject = panda_pop(data_row, self.subject_column)
            session = panda_pop(data_row, self.session_column)
            acquisition = panda_pop(data_row, self.acquisition_column)
            analysis = panda_pop(data_row, self.analysis_column)
            file = panda_pop(data_row, self.file_column)
            
            if self.import_columns == 'ALL':
                import_data = data_row
            else:
                import_data = data.get(self.import_columns)
                
            
            mappers.append(DataMap(fw = self.fw,
                                   data = import_data,
                                   group = group,
                                   project = project,
                                   subject = subject,
                                   session = session,
                                   acquisition = acquisition,
                                   analysis = analysis,
                                   file = file,
                                   namespace = namespace))
        return mappers


def panda_pop(series, key, default=None):
    """recreate the behavior of a dictionary "pop" for a pandas series
    
    behavior:
    if element exists, return the value and remove the element
    if the element doesn't exist, return the default
    the default default is "None"
    
    Args:
        series (pandas.Series): The series to pop from
        key (string): the key to look for and pop
        default (anything): the default value to return if the key isn't present
    
    Returns:
    
    """
    if key in series:
        return series.pop(key)
    else:
        return default
    
    
    
if __name__ == "__main__":
    import utils.flywheel_helpers as fh
    import logging
    import collections
    import flywheel
    import utils.import_data as id
    import numpy as np
    import pandas as pd
    import utils.mapping_class as mc
    logging.basicConfig(level="INFO")
    log = logging.getLogger()
    log.setLevel("INFO")
    df_path = '/Users/davidparker/Documents/Flywheel/SSE/MyWork/Gears/Metadata_import_Errorprone/matlab_metaimport/tests/Data_Import_Status_report_2.csv'
    df = pd.read_table(df_path, delimiter=',')
    mm = mc.MetadataMapper(project_column="Project",
                        subject_column="Subject",
                        session_column="Session",
                        acquisition_column="Acquisition",
                        analysis_column="Analysis",
                        file_column="Image Index",
                        import_columns='ALL')
    
    mappers = mm.map_data(df, 'Test1')
    [m.write_metadata() for m in mappers]


# import flywheel
# import os
# 
# fw = flywheel.Client(os.environ['ROLLOUT_API'])
# 
# project_id = '6037d56c6e67757f166e8aa6'
# project = fw.get_project(project_id)
# 
# projects = fw.projects.find(f"{project.id}")
# print([p.label for p in projects])
# 
# subjects = fw.subjects.find_one(id="605b4e735f814b5297ddf283")


# class testclass:
#     def __init__(self):
#         self.thing = {"a":1,
#                       "b": None }
#     
#     def operate(self,value):
#         mt = self.thing
#         mt["b"] = value
#     