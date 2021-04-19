import re
import logging
import flywheel


log = logging.getLogger()

class FlywheelObjectFinder:
    def __init__(
        self,
        fw,
        group=None,
        project=None,
        subject=None,
        session=None,
        acquisition=None,
        analysis=None,
        file=None,
        level=None,
    ):

        self.client = fw

        self.CONTAINER_ID_FORMAT = "^[0-9a-fA-F]{24}$"
        self.level = level
        self.object = None
        self.highest_container = None
        self.lowest_level = "file"
        self.highest_level = "group"

        self.group = {"id": group, "obj": None, "parent": None, "type": "group"}

        self.project = {
            "id": project,
            "obj": None,
            "parent": self.group,
            "type": "project",
        }

        self.subject = {
            "id": subject,
            "obj": None,
            "parent": self.project,
            "type": "subject",
        }

        self.session = {
            "id": session,
            "obj": None,
            "parent": self.subject,
            "type": "session",
        }

        self.acquisition = {
            "id": acquisition,
            "obj": None,
            "parent": self.session,
            "type": "acquisition",
        }

        self.file = {
            "id": file,
            "obj": None,
            "parent": self.lowest_level,
            "type": "file",
        }

        self.analysis = {
            "id": analysis,
            "obj": None,
            "parent": self.lowest_level,
            "type": "analysis",
        }

        self.find_levels()

        self.found_objects = None

    def __str__(self):
        """ Defines the print function of the class
        
        Prints a summary of the flywheel object being located in the form of a Flywheel
        path

        """
        string_out = ""
        for key, val in self.__dict__.items():
            if key in [
                "file",
                "analysis",
                "acquisition",
                "session",
                "subject",
                "project",
                "group",
            ]:
                string_out += f"{key}: {val.get('id')}\n"
            else:
                string_out += f"{key}: {val}\n"

        given_path = f"{self.group['id']}/{self.project['id']}/{self.subject['id']}/{self.session['id']}/{self.acquisition['id']}/{self.analysis['id']}/{self.file['id']}"
        string_out += f"Given Path: {given_path}"

        return string_out
    
    
    def find_levels(self):
        """ Finds the highest and lowest flywheel object levels given
        
        a level is considered "given" if that level has a specified label to use for
        a query.
        
        This is later used to determine where to start searching for the specified item.
        For example, if the highest level given is "Subject", we will start our search
        at `flywheel.subjects.find()`, as there's nothing specifying which projects we
        should use, meaning we just use them all.
        
        The lowest level is where we stop running searches at.  If the highest level is
        "project" and the lowest level is "acquisition", this will tell us to search for
        all acquisitions that match the provided acquisition label, and to only run this
        search on all subjects within the projects that match the provided project label.
        


        """
        order = [
            "group",
            "project",
            "subject",
            "session",
            "acquisition",
            "analysis",
            "file",
        ]

        for o in order[::-1]:
            test_case = getattr(self, o)
            if test_case.get("id") is not None:
                self.highest_level = o

        for o in order[:-1]:
            test_case = getattr(self, o)
            if test_case.get("id") is not None:
                self.lowest_level = o

        log.info(f"highest level is {self.highest_level}")
        log.info(f"lowest level is {self.lowest_level}")
    
    
    
    def process_matches(self, object_type=None, from_container=None):
        """ The main function that searches for a flywheel object with the provided
        information.
        
        Args:
            object_type (string): The type of the flywheel object we're looking for. Can
            be "group", "project", "subject", "session", "acquisition", "analysis", or
            "file".
            
            from_container (flywheel.ContainerReference):  A flywheel container to start
            the search from.  If present, this function will attempt to use the finders
            on that container, otherwise the client's finders will be used.

        Returns: object (list) a list of all objects found that match the provided
        search criteria.

        """

        # If no object_type is provided, we are initializing our search, and so we will
        # start from the lowest level provided
        if not object_type:
            object_type = self.lowest_level
        
        # Get the currently stored value for the provided "object_type" (will be empty
        # If this is the first run through)
        working_object = getattr(self, object_type)
        parent = working_object.get("parent")

        # Case 1, if this is the highest level that has a label provided, do a flywheel
        # client fw.<containers>.find(<query>)
        if object_type is self.highest_level:
            log.info("Entering case 1")
            log.debug(f"\n{working_object.get('id')}\n{object_type}\n")
            object = self.find_flywheel_container(working_object.get("id"), object_type, None
            )
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
                    f"parent object is none, switching from {object_type} to {new_type}, using from_conainer: {new_from}"
                )

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
            log.info(
                f"container {working_object.get('type')} from containers are now {[p.label for p in parent.get('obj')]}"
            )
            working_object["obj"] = parent.get("obj")
            return working_object["obj"]

        # Case 4, we have a from_container and an id:
        log.info(f"Case 4, searching for {object_type} on {parent.get('type')}")
        object = []
        for cont in parent.get("obj"):
            object.extend(
                self.find_flywheel_container(
                    working_object.get("id"), object_type, cont
                )
            )

        working_object["obj"] = object

        self.found_objects = object
        return object

    def check_for_file(self, container):
        """
        CURRENTLY UNUSED
        Args:
            container (): 

        Returns:

        """

        if self.file is not None:
            ct = container.get("container_type", "analysis")

            if self.level is not None and self.level == ct:
                files = [f for f in container.files if f.name == self.file]

            else:
                files = [f for f in container.files if f.name == self.file]

        else:
            files = []

        return files

    def check_for_analysis(self, container):
        """
        CURRENTLY UNUSED
        Args:
            container (): 

        Returns:

        """

        if self.analysis is not None:

            ct = container.get("container_type", "analysis")
            if self.level is not None and self.level == ct:
                analyses = [a for a in container.analyses if a.label == self.analysis]

            elif ct is not "analysis":
                analyses = [a for a in container.analyses if a.label == self.analysis]

        else:
            analyses = []

        return analyses

    def find_groups(self):
        """
        CURRENTLY UNUSED
        Returns:

        """

        if self.group_str:
            if self.group_str.islower():
                try:
                    group = [self.client.get_group(self.group_str)]
                except flywheel.ApiException:
                    log.debug(f"group name {self.group_str} is not an ID.")

            group = self.run_finder_at_level(self.client, None, "group", self.group_str)

            if group is None or group is []:
                log.error(f"Unable to find a group with a label or ID {self.group_str}")
                group = None
                # raise Exception(f"Group {self.group} Does Not Exist")

            self.group_obj = group
            self.highest_level = "group"





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
                "Cannot use find_flywheel_container() to find analysis without providing a container to search on"
            )
            return None

        # In this function we require a container to search on if we're looking for a file.
        if level == "file" and on_container is None:
            log.warning(
                "Cannot use find_flywheel_container() to find file without providing a container to search on"
            )
            return None
        
        # This function isn't specifically meant for running "queries" with regex or 
        # really anything other than directly matching an ID or a label, so since 
        # I don't know how to search by group labels we're considering this a special 
        # case.  Yes, this means that group values can only be ID's 
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
        
        # If we're looking for an analysis or file and we have a container 
        # to search for it on, just run the search, It'll handle it.
        elif level == "analysis" or level == "file":
            container = self.run_finder_at_level(on_container, level, name)
            
            # If we got something, set 'found' to true
            if len(container) > 0 and not all([c == None for c in container]):
                found = True

            
        # If we haven't found anything yet:
        if not found:
            
            # If the provided string matches the regex of a flywheel container ID, try
            # to search for that using the "_id=" key
            if re.match(self.CONTAINER_ID_FORMAT, name):
                try:
                    query = f"_id={name}"
                    container = self.run_finder_at_level(on_container, level, query)
                    # if len(container) > 0:
                    #     container = container[0]

                except flywheel.ApiException:
                    log.debug(f"{level} name {name} is not an ID.  Looking for Labels.")
            
            # If that didn't work, search for a label with the string provided:
            if not found:
                query = f'label="{name}"'
                container = self.run_finder_at_level(on_container, level, query)

        return container


    def run_finder_at_level(self, container, level, query):
        """ For a given container, run a finder query
        
        For a flywheel container `container`, run the finder query `query` for containers
        of type `level`.  This is an improvement over flywheel's finders as there are 
        gaps in what can be searched for.  For example, code can't simply say 
        "container.find_acquisitions()" because not all containers have the acuisision
        finder object.
        
        Args:
            container (flywheel.ContainerReference): the Flywheel container from which
            to start the search (this determines which child/parent objects will be 
            included in the query.)
            level (string): the level of container that we're looking for (Project, 
            subject, etc)
            query (string): the flywheel search query.

        Returns:
            container (list): any containers that match the search criteria.

        """

        fw = self.client
        
        # If a container is not provided, assume we're looking off the full instance's
        # finders
        if container is None:
            ct = "instance"
        else:
            
            # Otherwise just get the container type
            try:
                ct = container.container_type
                # In old core i think analyses don't have container types, so here's a check.
            except Exception:
                ct = "analysis"

        log.info(f"looking for {level} matching {query} on {ct}")
        
        # If the container we're searching from is the same type we're looking for, just
        # return it...something has probably gone wrong.  
        if ct == level:
            return [container]
        
        
        # If the level we're looking for is an acquisition, we do the following logic:
        if level == "acquisition":
            log.info("querying acquisitions")
            
            # If the container is None, run a client query, easy peasy.
            if container is None:
                containers = fw.acquisitions.find(query)
            else:
                
                # If the container we're searching from is an indirect parent of
                # acquisiton objects, we will expand to the 'direct' parent object
                if ct == "project" or ct == "subject":
                    # Onlty the session object has acquisition finders, so expand down
                    # to those.
                    
                    # query all child containers and append the results.
                    containers = []
                    temp_containers = container.sessions()
                    for cont in temp_containers:
                        containers.extend(cont.acquisitions.find(query))
                
                # Otherwise if our starting container is a session, run the query from there
                
                elif ct == "session":
                    containers = container.acquisitions.find(query)

                # Otherwise, we can't possibly run a query on a CHILD of an acquisition,
                # so just return the single acquisition parent of whatever we're
                # searching for.  I'm not sure this code is ever reached.
                else:
                    containers = [self.get_acquisition(container)]
        
        
        # If the level we're looking for is a session
        elif level == "session":
            log.info("querying sessions")
            # If there is no container to search from, query the instance.
            if container is None:
                containers = fw.sessions.find(query)
            else:
                # Otherwise, run the query from the provided container.
                # Expanding To Children
                if ct == "project" or "subject":
                    containers = container.sessions.find(query)

                # Shrink to parent
                else:
                    containers = [self.get_session(container)]
        
        
        # I think you get the idea now
        elif level == "subject":
            log.info("querying subjects")
            if container is None:
                log.info("container is None")
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
            log.info("querying projects")
            if container is None:
                containers = fw.projects.find(query)
            else:
                # Expand group to children projects:
                containers = container.projects.find(query)


        elif level == "group":
            log.info("querying groups")
            containers = fw.groups.find(query)


        elif level == "analysis":
            log.info("matching analysis")
            if container is None:
                log.warning("Can't search for analyses without a parent container")
                containers = [None]
            else:
                containers = [a for a in container.analyses if a.label == query]


        elif level == "file":
            log.info("matching file")
            if container is None:
                log.warning("Can't search for files without a parent container")
                containers = [None]
            else:
                containers = [f for f in container.files if f.name == query]

        return containers


    def get_subject(self, container):
        """ Gets the subject(s) associated with a container.
        
        Returns a single subject if the container/object is a child of a subject, or 
        returns all children subjects of a given object.
        
        Args:
            container (flywheel.ContainerReference): the container to get the subject(s)
            from.

        Returns:
            subjects (list): a list of all subjects associated with the provided
            container.

        """
        

        fw = self.client
        
        # If no container is provided, return all the subjects >:)
        if container is None:
            subjects = fw.subjects()
            return subjects
        
        # Get the provided container type, if not present it's an alalysis (old core, I
        # think, didn't have this property for analyses).
        ct = container.get("container_type", "analysis")

        # If the container type is a group, groups can't search for subjects, so expand
        # to projects and run individual queries from there.
        # TODO: This could be made into a query (fw.subjects.find('parents.group=<groupID>'))
        if ct == "group":
            projects = container.projects()
            subjects = []
            for proj in projects:
                subjects.extend(proj.subjects())
        
        # Otherwise handle all the other cases.
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
        """ Gets the session(s) associated with a container.

        Returns a single session if the container/object is a child of a session, or 
        returns all children sessions of a given object.
        
        See `get_subject()` for more info 

        Args:
            container (flywheel.ContainerReference): the container to get the session(s)
            from.

        Returns:
            sessions (list): a list of all sessions associated with the provided
            container.

        """

        fw = self.client

        if container is None:
            session = fw.sessions()
            return session

        ct = container.get("container_type", "analysis")

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
        """ Gets the acquisition(s) associated with a container.

        Returns a single acquisition if the container/object is a child of a acquisition, or 
        returns all children acquisitions of a given object.

        See `get_subject()` for more info 

        Args:
            container (flywheel.ContainerReference): the container to get the acquisition(s)
            from.

        Returns:
            acquisitions (list): a list of all acquisitions associated with the provided
            container.

        """

        fw = self.client
        if container is None:
            acquisition = fw.acquisitions()
            return acquisition

        ct = container.get("container_type", "analysis")

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
        """ Gets the analysis(s) associated with a container.

        Returns a single analysis if the container/object is a child of a analysis, or 
        returns all children analysiss of a given object.

        See `get_subject()` for more info 

        Args:
            container (flywheel.ContainerReference): the container to get the analysis(s)
            from.

        Returns:
            analysiss (list): a list of all sessions associated with the provided
            container.

        """
        

        ct = container.get("container_type", "analysis")

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
        """ Gets the project(s) associated with a container.

        Returns a single project if the container/object is a child of a project, or
        returns all children projects of a given object.

        See `get_subject()` for more info

        Args:
            container (flywheel.ContainerReference): the container to get the project(s)
            from.

        Returns:
            project (list): a list of all projects associated with the provided
            container.

        """

        fw = self.client

        ct = container.get("container_type", "analysis")
        
        if ct == 'group':
            project = container.projects()
        if ct == "project":
            project = container
        elif ct == "subject":
            project = fw.get_project(container.parents.project)
        elif ct == "session":
            project = fw.get_project(container.parents.project)
        elif ct == "acquisition":
            project = fw.get_project(container.parents.project)
        elif ct == "file":
            project = self.get_project(container.parent.reload())
        elif ct == "analysis":
            project = fw.get_project(container.parents.project)

        return project

    def generate_path_to_container(self, parent_container=None):
        """ Builds a human readable path to a flywheel container
        
        Once an object has been located by the finder, this function generates a
        Flywheel path to it. Or, a container object can be passed in, and a path
        will be generated from that.
        
        Args:
            parent_container (flywheel.ContainerReference): A Flywheel container to
            generate a flywheel path from.  If not present, will attempt to generate
            a path for any found objects that match the search criterea.

        Returns:
            fw_paths (list): a list of strings to flywheel paths.

        """
        
        fw = self.client
        fw_paths = []

        if parent_container is None:
            log.debug('parent container is None, using self.found_objects:')
            log.debug(f"found {len(self.found_objects)} objects")
            containers_to_loop = self.found_objects
        else:
            log.debug(f'parent container is present. type {type(parent_container)} ')
            containers_to_loop = [parent_container]

        for container in containers_to_loop:
            try:
                ct = container.container_type
                log.debug(f"container type is {ct}")
            except Exception:
                log.debug('container type is assumed to be analysis')
                ct = "analysis"

            if ct == "file":
                path_to_file = self.generate_path_to_container(
                    container.parent.reload()
                )
                path_to_file = path_to_file[0]
                fw_path = f"{path_to_file}/{container.name}"

            else:
                fw_path = ""

                if container.parents.group is not None:
                    append = container.parents.group
                else:
                    append = ""

                fw_path += append

                if container.parents.project is not None:
                    project = self.get_project(container)
                    append = f"/{project.label}"
                else:
                    append = ""

                fw_path += append

                if container.parents.subject is not None:
                    subject = self.get_subject(container)[0]
                    append = f"/{subject.label}"
                else:
                    append = ""

                fw_path += append

                if container.parents.session is not None:
                    session = self.get_session(container)[0]
                    append = f"/{session.label}"
                else:
                    append = ""

                fw_path += append

                if container.parents.acquisition is not None:
                    acquisition = self.get_acquisition(container)[0]
                    append = f"/{acquisition.label}"
                else:
                    append = ""

                fw_path += append

                if container.get("container_type", "analysis") == "analysis":
                    analysis = container.label
                    append = f"/{analysis}"
                else:
                    analysis = ""

                fw_path += append

            fw_paths.append(fw_path)

        return fw_paths
