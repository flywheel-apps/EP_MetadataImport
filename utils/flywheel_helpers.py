import logging
import re

import flywheel

log = logging.getLogger()

CONTAINER_ID_FORMAT = "^[0-9a-fA-F]{24}$"


valid_finder_types = ['groups','projects','sessions','subjects','acquisitions', 'analyses','files']


class analyses_finder:
    def __init__(self, analyses):
        self.analyses = analyses
    
    def find(self, query):
        if query.startswith('label='):
            name_to_find = query.split('label=')[-1]
            
            matching = [a for a in self.analyses if a.label==name_to_find]
        
        else:
            matching = []
        
        return matching


class file_finder:
    def __init__(self, files):
        self.files = files

    def find(self, query):
        
        if query.startswith('label='):
            name_to_find = query.split('label=')[-1]

            matching = [a for a in self.files if a.name == name_to_find]

        else:
            matching = []

        return matching
        
            
            



def run_query_on_finder(finders, query):
    
    if not isinstance(finders, list):
        finders = [finders]
        
    results = []
    
    for finder in finders:
        results.extend(finder.find(query))
    
    return results


def get_finders_from_container(fw, find_type, container):
    
    if not find_type in valid_finder_types:
        log.warning(f"{find_type} is not a valid finder type")
        return []
    
    if find_type in ['analysis', 'files']:
        pass
        
    
    if container is None:
        finders = getattr(fw, find_type)
    
    else:
        if not hasattr(container, find_type):
            

        if hasattr(container, find_type):
            pass
        pass
    pass


def get_finders_at_level(fw, container, level):
    try:
        ct = container.container_type
    except Exception:
        ct = 'analysis'

    if ct == level:
        return ([container])

    if level == "acquisition":
        # Expanding To Children
        if ct == "project" or ct == "subject":
            containers = []
            temp_containers = container.sessions
            for cont in temp_containers:
                containers.extend(cont.acquisitions)

        elif ct == 'session':
            containers = container.acquisitions

        # Shrink to parent
        else:
            containers = [get_acquisition(fw, container)]

    elif level == "session":
        # Expanding To Children
        if ct == "project" or 'subject':
            containers = container.sessions()

        # Shrink to parent
        else:
            containers = [get_session(fw, container)]

    elif level == "subject":
        # Expanding To Children
        if ct == "project":
            containers = container.subjects()

        # Shrink to parent
        else:
            containers = [get_subject(fw, container)]

    elif level == 'analysis':
        containers = container.analyses
    elif level == 'file':
        containers = container.files

    return containers
            
        
        









########

def get_containers_at_level(fw, container, level):
    try:
        ct = container.container_type
    except Exception:
        ct = 'analysis'
    
    if ct == level:
        return([container])
    
    if level == "acquisition":
        # Expanding To Children
        if ct == "project" or ct == "subject":
            containers = []
            temp_containers = container.sessions()
            for cont in temp_containers:
                containers.extend(cont.acquisitions())
                
        elif ct == 'session':
            containers = container.acquisitions()
            
        # Shrink to parent
        else:
            containers = [get_acquisition(fw, container)]

    elif level == "session":
        # Expanding To Children
        if ct == "project" or 'subject':
            containers = container.sessions()
        
        # Shrink to parent
        else:
            containers = [get_session(fw, container)]

    elif level == "subject":
        # Expanding To Children
        if ct == "project":
            containers = container.subjects()
        
        # Shrink to parent
        else:
            containers = [get_subject(fw, container)]

    elif level == 'analysis':
        containers = container.analyses
    elif level == 'file':
        containers = container.files
    
    return containers


def run_finder_at_level(fw, container, level, query):
    print(query)
    try:
        ct = container.container_type
    except Exception:
        ct = 'analysis'

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
                containers = [get_acquisition(fw, container)]

    elif level == "session":
        log.info('querying sessions')
        if container is None:
            containers = [fw.sessions.find_one(query)]
        else:
            # Expanding To Children
            if ct == "project" or 'subject':
                containers = container.sessions.find(query)
    
            # Shrink to parent
            else:
                containers = [get_session(fw, container)]

    elif level == "subject":
        log.info('querying subjects')
        if container is None:
            containers = [fw.subjects.find_one(query)]
        else:
            # Expanding To Children
            if ct == "project":
                containers = container.subjects.find(query)
    
            # Shrink to parent
            else:
                containers = [get_subject(fw, container)]
                
    elif level == "project":
        log.info('querying projects')
        if container is None:
            containers = [fw.projects.find_one(query)]
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






def get_children(container):

    ct = container.get('container_type', 'analysis')
    if ct == "group":
        children = container.projects()
    elif ct == "project":
        children = container.subjects()
    elif ct == "subject":
        children = container.sessions()
    elif ct == "session":
        children = container.acquisitions()
    elif ct == "acquisition" or ct == "analysis":
        children = container.files
    else:
        children = []

    return children


def get_parent(fw, container):

    ct = container.get('container_type', 'analysis')

    if ct == "project":
        parent = fw.get_group(container.group)
    elif ct == "subject":
        parent = fw.get_project(container.project)
    elif ct == "session":
        parent = container.subject
    elif ct == "acquisition":
        parent = container.get_session(container.session)
    elif ct == "analysis":
        parent = fw.get(container.parent["id"])
    elif ct == 'file':
        parent = container.parent.reload()
    else:
        parent = None

    return parent


def get_subject(fw, container):
    
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
        subject = get_subject(container.parent.reload())
    elif ct == "analysis":
        sub_id = container.parents.subject
        if sub_id is not None:
            subject = [fw.get_subject(sub_id)]
        else:
            subject = None

    return subject


def get_session(fw, container):
    
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
        session = [get_session(container.parent.reload())]
        
    elif ct == "analysis":
        ses_id = container.parents.session
        if ses_id is not None:
            session = [fw.get_session(ses_id)]
        else:
            session = None

    return session



def get_acquisition(fw, container):
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
        acquisition = get_acquisition(container.parent.reload())
        
    elif ct == "analysis":
        ses_id = container.parents.acquisition
        if ses_id is not None:
            acquisition = [fw.get_acquisition(ses_id)]
        else:
            acquisition = None

    return acquisition


def get_analysis(fw, container):
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
        analysis = get_analysis(container.parent.reload())
    elif ct == "analysis":
        analysis = [container]

    return analysis


def get_project(fw, container):
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
        project = get_project(container.parent.reload())
    elif ct == "analysis":
        project = [fw.get_project(container.parents.project)]

    return project


def get_parent_at_level(fw, container, level):

    if level == "project":
        parent = get_project(fw, container)
    elif level == "subject":
        parent = get_subject(fw, container)
    elif level == "session":
        parent = get_session(fw, container)
    elif level == "acquistion":
        parent = get_acquisition(fw, container)
    elif level == "analysis":
        parent = get_analysis(fw, container)

    return parent



def get_level(fw, id, level):
    if level == 'project':
        container = fw.get_project(id)
    elif level == 'subject':
        container = fw.get_subject(id)
    elif level == 'session':
        container = fw.get_session(id)
    elif level == 'acquisition':
        container = fw.get_acquisition(id)
    elif level == 'analysis':
        container = fw.get_analysis(id)
    else:
        container = None
    
    return container
    
    

def generate_path_to_container(
        fw,
        container,
        group = None,
        project = None,
        subject = None,
        session = None,
        acquisition = None,
        analysis = None
):
    
    try:
        ct = container.container_type
    except Exception:
        ct = 'analysis'
    
    
    if ct == "file":
        path_to_file = generate_path_to_container(
            fw,
            container.parent.reload(),
            group,
            project,
            subject,
            session,
            acquisition,
            analysis)

        fw_path = f"{path_to_file}/{container.name}"

    else:
        fw_path = ''
        
        if group is not None:
            append = group
        elif group is None and container.parents.group is not None:
            append = container.parents.group
        else:
            append = ''
        
        fw_path += append
        
        if project is not None:
            append = f"/{project}"
        elif project is None and container.parents.project is not None:
            project = get_project(fw, container)
            append = f"/{project.label}"
        else:
            append = ''
        
        fw_path += append
        
        if subject is not None:
            append = f"/{subject}"
        elif subject is None and container.parents.subject is not None:
            subject = get_subject(fw, container)
            append = f"/{subject.label}"
        else:
            append = ''
        
        fw_path += append
        
        if session is not None:
            append = f"/{session}"
        elif session is None and container.parents.session is not None:
            session = get_session(fw, container)
            append = f"/{session.label}"
        else:
            append = ''
        
        fw_path += append
        
        if acquisition is not None:
            append = f"/{acquisition}"
        elif acquisition is None and container.parents.acquisition is not None:
            acquisition = get_acquisition(fw, container)
            append = f"/{acquisition.label}"
        else:
            append = ''

        fw_path += append
        
        if analysis is not None:
            append = f"/{analysis}"
        elif analysis is None and container.get('container_type', 'analysis') == 'analysis':
            analysis = container.label
            append = f"/{analysis}"
        else:
            analysis = ''
        
        fw_path += append
        
        # append = f"/{container.label}"
        # 
        # fw_path += append
        
    return fw_path



def find_flywheel_container(fw, name, level, on_container = None):
    """ Tries to locate a flywheel container at a certain level

    Args:
        name (str): the name (label or ID) of the container to find
        level (str): Level at which to get the container (group, project, subject, session, acquisition)
        on_container (flywheel.Container): a container to find the object on (used for files or analyses)
        
    Returns: container (flywheel.Container): a flywheel container

    """
    level = level.lower()
    
    found = False
    print(name)
    # In this function we require a container to search on if we're looking for an analysis.
    if level == "analysis" and on_container is None:
        log.warning('Cannot use find_flywheel_container() to find analysis without providing a container to search on')
        return None
    
    # In this function we require a container to search on if we're looking for a file.
    if level == "file" and on_container is None:
        log.warning('Cannot use find_flywheel_container() to find file without providing a container to search on')
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
        container = run_finder_at_level(fw, on_container, level, name)
        container, found = check_for_multiple(container, level, name)
        
    if not found:
        if re.match(CONTAINER_ID_FORMAT, name):
            try:
                query = f"_id={name}"
                container = run_finder_at_level(fw, on_container, level, query)
                if len(container) > 0:
                    container = container[0]
                    
                container, found = check_for_multiple(container, level, name)
            
            except flywheel.ApiException:
                log.debug(f"{level} name {name} is not an ID.  Looking for Labels.")

        if not found:
            query = f"label={name}"
            container = run_finder_at_level(fw, on_container, level, query)
            container, found = check_for_multiple(container, level, name)


    
    return container


            # raise Exception(f"Group {self.group} Does Not Exist")

    #     # If the ID didn't work, look for labels
    #     if not found:
    #         container = fw.groups.find(f"label={name}")
    #         container, found = check_for_multiple(container, level, name)
    # 
    # # If we are looking for projects
    # elif level == 'project':
    #     
    #     # If we're searching the whole instance
    #     if on_container is None:
    #         
    #         # First check for projects with that ID:
    #         if re.match(CONTAINER_ID_FORMAT, name):
    #             try:
    #                 container = fw.get_project(name)
    #                 found = True
    #             except flywheel.ApiException:
    #                 log.debug(f"project name {name} is not an ID.  Looking for Labels.")
    #                 
    #         if not found:
    #             container = fw.projects.find(f"label={name}")
    #             container, found = check_for_multiple(container, level, name)
    #     
    #     elif isinstance(on_container, flywheel.Group):
    #         
    #         # First check for projects with that ID:
    #         if re.match(CONTAINER_ID_FORMAT, name):
    #             container = on_container.projects.find(f"_id={name}")
    #             container, found = check_for_multiple(container, level, name)
    #             
    #         # If it's not an ID, try label
    #         if not found:
    #             log.debug(f"project name {name} is not an ID.  Looking for Labels.")
    #             container = on_container.projects.find(f"label={name}")
    #             
    #                 
    #         
    #     else:
    #         log.error(f"Invalid 'on_container' provided for project search {name}.  Expected"
    #                   f"container type group, got {type(on_container)}")
    #         
            
    # If we are looking for subjects:
  
            
            
                
def check_for_multiple(container, level, name):
    
    found = False
    if len(container) == 0:
        log.warning(
            f"No {level} found with label {name}.  Ensure that this {level} exists and that you have the correct permissions")
        container = []

        # If there are more than one match, we return nothing. 
    elif len(container) > 1:
        log.warning(
            f"Multiple {level}s found with the label {name}.  Please search by ID instead.")
        print([a.label for a in container])
        print([a.id for a in container])
        container = []

        # Otherwise return the single result.
    else:
        # container = container[0]
        found = True
        
    return container, found


if __name__ == "__main__":
    query = "label=XR"
    level = "acquisition"
    container = '602eb7d7c0f6a53b3783e970'
    fw = flywheel.Client()
    container = fw.get_session('6039523c13ed9475c86e8aec')

    containers = run_finder_at_level(fw, container, level, query)
    for c in containers:
        print(c.label)
        