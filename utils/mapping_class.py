import re
import flywheel_helpers as fh
import logging
import collections
import flywheel
import import_data as id
import numpy as np
import pandas as pd

logging.basicConfig(level="DEBUG")
log = logging.getLogger()
log.setLevel("DEBUG")



class FlywheelObjectFinder:
    def __init__(self, fw, group=None, project=None, subject=None, session=None,
                 acquisition=None, analysis=None,
                 file=None, level=None):
        
        self.client = fw

        self.CONTAINER_ID_FORMAT = "^[0-9a-fA-F]{24}$"
        self.level = level
        self.object = None
        self.highest_container = None
        self.lowest_level = 'file'
        self.highest_level = 'group'
        
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
                        "parent": self.lowest_level,
                        "type": "file"}
        
        self.analysis = {"id": analysis,
                        "obj": None,
                        "parent": self.lowest_level,
                        "type": "analysis"}
        
        

        self.find_levels()
    
    
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
    
    
    def find_levels(self):
        order = ["group","project","subject","session","acquisition","analysis","file"]
        
        for o in order[::-1]:
            test_case = getattr(self, o)
            if test_case.get("id") is not None:
                self.highest_level = o

        for o in order[:-1]:
            test_case = getattr(self, o)
            if test_case.get("id") is not None:
                self.lowest_level = o
                
        log.info(f'highest level is {self.highest_level}')
        log.info(f"lowest level is {self.lowest_level}")

    
    def process_matches(self, object_type, from_container=None):
        
        working_object = getattr(self, object_type)
        parent = working_object.get("parent")
                
        # Case 1, if this is the highest level that has a label provided, do a flywheel
        # client fw.<containers>.find(<query>)
        if object_type is self.highest_level:
            log.info("Entering case 1")
            log.debug(f"\n{working_object.get('id')}\n{object_type}\n")
            object = fh.find_flywheel_container(self.client, working_object.get("id"), object_type,
                                                None)
            working_object["obj"] = object
            return object
        
        # Case 2, if from_container is none, recurse up to highest level:
        if from_container is None:
            log.info("entering case 2, finding from_container")
            # First if the parent object has an object, then that's the from
            if parent.get("obj") is not None:
                log.info("parent object has objects")
                from_container = parent.get("obj")

            elif parent.get("obj") is None:
                log.info("parent object is empty, will recurse to parent's parent")
                new_type = parent.get("type")
                new_from = parent.get("parent").get("obj")
                log.info(
                    f'parent object is none, switching from {object_type} to {new_type}, using from_conainer: {new_from}')
                
                # This should resolve...things...
                from_container = self.process_matches(new_type, new_from)
                parent["obj"] = from_container
        
        # Case 3, if this ID is blank, from_container is not none, we already know this isn't the 
        # highest level, so we need to skip to the parent.
        # Since case 2 takes care of the "from_container = None" condition, we...SHOULD always have
        # from_containers...
        # So I think we can just get the from_containers from the parent and pass them down a level?
        
        if working_object.get("id") is None:
            log.info(f"entering case 3.  moving 'from containers' from parent to child")
            log.info(f"container {working_object.get('type')} from containers are now {[p.label for p in parent.get('obj')]}")
            working_object["obj"] = parent.get("obj")
            return working_object["obj"]
        
        
        # Case 4, we have a from_container and an id:
        log.info(f"Case 4, searching for {object_type} on {parent.get('type')}")
        object = []
        for cont in parent.get("obj"):
            object.extend(self.find_flywheel_container(working_object.get("id"), object_type, cont))
        
        working_object["obj"] = object
        return object

    def find_flywheel_container(self, name, level, on_container=None):
        """ Tries to locate a flywheel container at a certain level

        Args:
            name (str): the name (label or ID) of the container to find
            level (str): Level at which to get the container (group, project, subject, session, acquisition)
            on_container (flywheel.Container): a container to find the object on (used for files or analyses)

        Returns: container (flywheel.Container): a flywheel container

        """
        
        fw = self.client
        level = level.lower()

        found = False
        print(name)
        # In this function we require a container to search on if we're looking for an analysis.
        if level == "analysis" and on_container is None:
            log.warning(
                'Cannot use find_flywheel_container() to find analysis without providing a container to search on')
            return None

        # In this function we require a container to search on if we're looking for a file.
        if level == "file" and on_container is None:
            log.warning(
                'Cannot use find_flywheel_container() to find file without providing a container to search on')
            return None

        # If we're looking for a group:
        if level == "group":
            # Check if we're a group id or label (maybe, just guessing here at first)
            # If the name is all lowercase, it might be an ID
            if name.islower():
                try:
                    container = fw.get_group(name)
                    found = True
                except flywheel.ApiException:
                    log.debug(f"group name {name} is not an ID.")
                    found = False


        elif level == "analysis" or level == "file":
            container = self.run_finder_at_level(on_container, level, name)

        if not found:
            if re.match(self.CONTAINER_ID_FORMAT, name):
                try:
                    query = f"_id={name}"
                    container = self.run_finder_at_level(on_container, level, query)
                    if len(container) > 0:
                        container = container[0]


                except flywheel.ApiException:
                    log.debug(f"{level} name {name} is not an ID.  Looking for Labels.")

            if not found:
                query = f"label=\"{name}\""
                container = self.run_finder_at_level(on_container, level, query)
                # log.info(container)

        return container
    
    
    def run_finder_at_level(self, container, level, query):
        
        fw = self.client
        
        if container is None:
            ct = 'instance'
        else:
            try:
                ct = container.container_type
            except Exception:
                ct = 'analysis'

        log.info(
            f"looking for {level} matching {query} on {ct}")

        if ct == level:
            return ([container])

        if level == "acquisition":
            log.info('querying acquisitions')
            if container is None:
                containers = fw.acquisitions.find(query)
            else:
                # Expanding To Children
                if ct == "project" or ct == "subject":
                    containers = []
                    temp_containers = container.sessions()
                    for cont in temp_containers:
                        containers.extend(cont.acquisitions.find(query))

                elif ct == 'session':
                    containers = container.acquisitions.find(query)

                # No queries on parents
                else:
                    containers = [self.get_acquisition(container)]

        elif level == "session":
            log.info('querying sessions')
            if container is None:
                containers = [fw.sessions.find(query)]
            else:
                # Expanding To Children
                if ct == "project" or 'subject':
                    containers = container.sessions.find(query)

                # Shrink to parent
                else:
                    containers = [self.get_session(container)]

        elif level == "subject":
            log.info('querying subjects')
            if container is None:
                log.info('container is None')
                containers = fw.subjects.find(query)
                log.info(containers)
            else:
                # Expanding To Children
                if ct == "project":
                    containers = container.subjects.find(query)

                # Shrink to parent
                else:
                    containers = [self.get_subject(container)]

        elif level == "project":
            log.info('querying projects')
            if container is None:
                containers = fw.projects.find(query)
            else:
                # Expand group to children projects:
                containers = container.projects.find(query)

        elif level == "group":
            log.info('querying groups')
            containers = fw.groups.find(query)


        elif level == 'analysis':
            log.info('matching analysis')
            if container is None:
                log.warning("Can't search for analyses without a parent container")
                containers = [None]
            else:
                containers = [a for a in container.analyses if a.label == query]

        elif level == 'file':
            log.info('matching file')
            if container is None:
                log.warning("Can't search for files without a parent container")
                containers = [None]
            else:
                containers = [f for f in container.files if f.name == query]

        return containers


    def get_subject(self, container):
        
        fw = self.client

        if container is None:
            subjects = fw.subjects()
            return subjects

        ct = container.get('container_type', 'analysis')

        if ct == "group":
            projects = container.projects()
            subjects = []
            for proj in projects:
                subjects.extend(proj.subjects())

        elif ct == "project":
            subject = container.subjects()
        elif ct == "subject":
            subject = [container]
        elif ct == "session":
            subject = [container.subject]
        elif ct == "acquisition":
            subject = [fw.get_subject(container.parents.subject)]
        elif ct == "file":
            subject = self.get_subject(container.parent.reload())
        elif ct == "analysis":
            sub_id = container.parents.subject
            if sub_id is not None:
                subject = [fw.get_subject(sub_id)]
            else:
                subject = None

        return subject


    def get_session(self, container):
        
        fw = self.client

        if container is None:
            session = fw.sessions()
            return session

        ct = container.get('container_type', 'analysis')

        if ct == "group":
            projects = container.projects()
            session = []
            for proj in projects:
                session.extend(proj.sessions())

        elif ct == "project":
            session = container.sessions()

        elif ct == "subject":
            session = container.sessions()

        elif ct == "session":
            session = [container]

        elif ct == "acquisition":
            session = [fw.get_session(container.parents.session)]

        elif ct == "file":
            session = [self.get_session(container.parent.reload())]

        elif ct == "analysis":
            ses_id = container.parents.session
            if ses_id is not None:
                session = [fw.get_session(ses_id)]
            else:
                session = None

        return session
    

    def get_acquisition(self, container):
        
        fw = self.client
        if container is None:
            acquisition = fw.acquisitions()
            return acquisition

        ct = container.get('container_type', 'analysis')

        if ct == "group":
            projects = container.projects()
            acquisition = []
            for proj in projects:
                sessions = proj.sessions()
                for ses in sessions:
                    acquisition.extend(ses.acquisitions())

        elif ct == "project":
            acquisition = []
            sessions = container.sessions()
            for ses in sessions:
                acquisition.extend(ses.acquisitions())

        elif ct == "subject":
            acquisition = []
            sessions = container.sessions()
            for ses in sessions:
                acquisition.extend(ses.acquisitions())

        elif ct == "session":
            acquisition = container.acquisitions()

        elif ct == "acquisition":
            acquisition = [container]

        elif ct == "file":
            acquisition = self.get_acquisition(container.parent.reload())

        elif ct == "analysis":
            ses_id = container.parents.acquisition
            if ses_id is not None:
                acquisition = [fw.get_acquisition(ses_id)]
            else:
                acquisition = None

        return acquisition

    def get_analysis(self, container):
        
        ct = container.get('container_type', 'analysis')

        if ct == "project":
            analysis = container.analyses
        elif ct == "subject":
            analysis = container.analyses
        elif ct == "session":
            analysis = container.analyses
        elif ct == "acquisition":
            analysis = container.analyses
        elif ct == "file":
            analysis = self.get_analysis(container.parent.reload())
        elif ct == "analysis":
            analysis = [container]

        return analysis
    

    def get_project(self, container):
        
        fw = self.client
        
        ct = container.get('container_type', 'analysis')

        if ct == "project":
            project = container
        elif ct == "subject":
            project = [fw.get_project(container.parents.project)]
        elif ct == "session":
            project = [fw.get_project(container.parents.project)]
        elif ct == "acquisition":
            project = [fw.get_project(container.parents.project)]
        elif ct == "file":
            project = self.get_project(container.parent.reload())
        elif ct == "analysis":
            project = [fw.get_project(container.parents.project)]

        return project




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
    
    
    
# if __name__ == "__main__":
#     import utils.flywheel_helpers as fh
#     import logging
#     import collections
#     import flywheel
#     import utils.import_data as id
#     import numpy as np
#     import pandas as pd
#     import utils.mapping_class as mc
#     logging.basicConfig(level="INFO")
#     log = logging.getLogger()
#     log.setLevel("INFO")
#     df_path = '/Users/davidparker/Documents/Flywheel/SSE/MyWork/Gears/Metadata_import_Errorprone/matlab_metaimport/tests/Data_Import_Status_report_2.csv'
#     df = pd.read_table(df_path, delimiter=',')
#     mm = mc.MetadataMapper(project_column="Project",
#                         subject_column="Subject",
#                         session_column="Session",
#                         acquisition_column="Acquisition",
#                         analysis_column="Analysis",
#                         file_column="Image Index",
#                         import_columns='ALL')
#     
#     mappers = mm.map_data(df, 'Test1')
#     [m.write_metadata() for m in mappers]


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


def test_the_class():
    import flywheel
    import os
    import logging
    logging.basicConfig(level="DEBUG")
    log = logging.getLogger()
    log.setLevel("DEBUG")
    
    fw = flywheel.Client(os.environ["FWGA_API"])
    test = FlywheelObjectFinder(fw=fw, project='img2dicom', acquisition='acq')
    #test.session = '12-23-17 8:53 PM'
    #test.project = 'Random Dicom Scans'
    #test.group = 'scien'
    result = test.find_object('acquisition')
    print(result)
    print(len(result))


if __name__ == "__main__":
    test_the_class()
    