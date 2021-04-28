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




class DataMap:
    def __init__(
        self,
        fw,
        data,
        group=None,
        project=None,
        subject=None,
        session=None,
        acquisition=None,
        analysis=None,
        file=None,
        info=False,
        namespace="",
    ):

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

        self.finder = FlywheelObjectFinder(
            fw=fw,
            group=group,
            project=project,
            subject=subject,
            session=session,
            acquisition=acquisition,
            analysis=analysis,
            file=file,
            level=None,
        )

        self.found_object = None

        highest_container = None

    def find(self):

        self.found_object = self.finder.process_matches()
        given_path = self.generate_path_from_given_info()

        log.debug(f"Given Path:\n{given_path}\n\n")

        found_paths = self.finder.generate_path_to_container()
        log.debug("Found Paths:\n")

        for fp in found_paths:
            log.debug(f"{fp}")
        log.debug("\n\n")

        log.debug(f"")

    def generate_path_from_given_info(self):
        items = [
            self.group,
            self.project,
            self.subject,
            self.session,
            self.acquisition,
            self.analysis,
            self.file,
        ]

        path_str = ""
        for i in items:
            if i is None:
                path_str += "/None"
            else:
                path_str += f"/{i}"

        return path_str

    def write_metadata(self, overwrite=False):

        if self.found_object is None:
            self.find()

        fw_object = self.finder.process_matches()
        
        if fw_object is not None:
            if len(fw_object) > 1:
                log.warning("Multiple matches found.  Cannont write.")
                pass
            
            fw_object = fw_object[0]
            update_dict = self.make_meta_dict()
            object_info = fw_object.get("info")
            if not object_info:
                object_info = dict()

            print(object_info)
            object_info = self.update(object_info, update_dict, overwrite)
            print(object_info)
            fw_object.update_info(object_info)
            

    def make_meta_dict(self):
        
        if not self.namespace and not self.info:
            log.error('data mapper property `info` must be set to True if '
                      'no namespace is provided for metadata upload')
            pass
        
        if self.info:
            if isinstance(self.data, pd.Series):
                output_dict = self.data.to_dict()
            else:
                output_dict = self.data

        else:
            if isinstance(self.data, pd.Series):
                output_dict = {self.namespace: self.data.to_dict()}
            else:
                output_dict = {self.namespace: self.data}
            
            
        output_dict = self.cleanse_the_filthy_numpy(output_dict)
        return output_dict

    def update(self, d, u, overwrite):
    
        for k, v in u.items():
            if isinstance(v, collections.abc.Mapping):
                d[k] = self.update(d.get(k, {}), v, overwrite)
            else:
                # Flywheel doesn't like numpy data types:
                if type(v).__module__ == np.__name__:
                    v = v.item()
    
                log.debug(f'checking if "{k}" in {d.keys()}')
                if k in d:
                    if overwrite:
                        log.debug(f'Overwriting "{k}" from "{d[k]}" to "{v}"')
                        d[k] = v
                    else:
                        log.debug(f'"{k}" present.  Skipping.')
                else:
                    log.debug(f"setting {k}")
                    d[k] = v
    
        return d
    
    
    def cleanse_the_filthy_numpy(self, dict):
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
                dict[k] = self.cleanse_the_filthy_numpy(dict.get(k, {}))
            else:
                # Flywheel doesn't like numpy data types:
                if type(v).__module__ == np.__name__:
                    v = v.item()
                    dict[k] = v
        return dict



class MetadataMapper:
    def __init__(
        self,
        group_column=None,
        project_column=None,
        subject_column=None,
        session_column=None,
        acquisition_column=None,
        analysis_column=None,
        file_column=None,
        import_columns="ALL",
    ):

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

        data.fillna("", inplace=True)
        nrows, ncols = data.shape
        log.info("Starting Mapping")

        data["Gear_Status"] = "Failed"
        data["Gear_FW_Location"] = None

        success_counter = 0

        mappers = []
        for row in range(nrows):
            data_row = data.iloc[row]
            # print(data_row)
            group = panda_pop(data_row, self.group_column)
            project = panda_pop(data_row, self.project_column)
            subject = panda_pop(data_row, self.subject_column)
            session = panda_pop(data_row, self.session_column)
            acquisition = panda_pop(data_row, self.acquisition_column)
            analysis = panda_pop(data_row, self.analysis_column)
            file = panda_pop(data_row, self.file_column)

            if self.import_columns == "ALL":
                import_data = data_row
            else:
                import_data = data.get(self.import_columns)

            mappers.append(
                DataMap(
                    fw=self.fw,
                    data=import_data,
                    group=group,
                    project=project,
                    subject=subject,
                    session=session,
                    acquisition=acquisition,
                    analysis=analysis,
                    file=file,
                    namespace=namespace,
                )
            )
        return mappers


def panda_pop(series, key, default=None):
    """recreate the behavior of a dictionary "pop" for a pandas series
    
    behavior:
    if element exists, return the value and remove the element
    if the element doesn't exist, return the default
    the default... uh... default is "None"
    
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




def test_the_class():
    import flywheel
    import os
    import logging

    logging.basicConfig(level="DEBUG")
    log = logging.getLogger()
    log.setLevel("DEBUG")

    fw = flywheel.Client(os.environ["FWGA_API"])
    test = FlywheelObjectFinder(fw=fw, project="img2dicom", acquisition="acq")
    print(test)
    # test.session = '12-23-17 8:53 PM'
    # test.project = 'Random Dicom Scans'
    # test.group = 'scien'
    result = test.process_matches()
    # print(result)
    print(len(result))

def test_the_datamap():

    import flywheel
    import os
    import logging

    logging.basicConfig(level="DEBUG")
    log = logging.getLogger()
    log.setLevel("DEBUG")

    fw = flywheel.Client(os.environ["FWGA_API"])
    test = DataMap(fw=fw, data={'test': True}, project="img2dicom", acquisition="acq")
    test.namespace='SubLevel'
    test.find()
    test.write_metadata()
    # test.session = '12-23-17 8:53 PM'
    # test.project = 'Random Dicom Scans'
    # test.group = 'scien'
    #result = test.process_matches()
    # print(result)
    
    print(len(test.found_object))
    
    
    
if __name__ == "__main__":
    test_the_datamap()
    #test_the_class()
