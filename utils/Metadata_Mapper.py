import re
import logging
import collections
import flywheel
import numpy as np
import pandas as pd
import yaml
from pathlib import Path

logging.basicConfig(level="DEBUG")
log = logging.getLogger()
log.setLevel("DEBUG")


class MetadataMap:
    def __init__(
        self, fw, data=None, fw_object=None, data_map=None, namespace=None,
    ):

        self.data = data
        self.fw_object = fw_object
        self.data_map = data_map
        self.fw = fw
        self.namespace = namespace
        # TODO: For future implementations.  Leaving as a placeholder so I can keep my...what do you call them?  brain...words straight.  THOUGHTS!  That's the word...thoughts
        # self.source_of_truth = "data"
        # self.reset = False
        # self.update_existing = True
        # self.add_new = True

        
        
    def load_yaml_map(self):
        
        if isinstance(self.data_map, dict):
            log.info("Mapping dictionary recognized")
            return
        
        if self.data_map is None:
            return
        
        if isinstance(self.data_map, str):
            self.data_map = Path(self.data_map)
            
        if Path.is_file(self.data_map):
        
            
            if self.data_map.suffix != ".yaml":
                raise TypeError("data_map must be of type dict or a yaml file ending in '.yaml'")
            
            log.info("yaml file recognized for map.  Loading.")
            with open(self.data_map) as file:
                self.data_map = yaml.load(file, Loader=yaml.FullLoader)
            
        return
    
            

    def write_metadata(self, overwrite=False):
        
        update_dict = self.make_meta_dict()
        object_info = self.fw_object.get("info")
        if not object_info:
            object_info = dict()

        print(object_info)
        object_info = self.update(object_info, update_dict, overwrite)
        print(object_info)
        self.fw_object.update_info(object_info)


    def make_meta_dict(self):
        
        if isinstance(self.data, pd.Series):
            output_dict = self.data.to_dict()
        else:
            output_dict = self.data

        self.load_yaml_map()
    
        if self.data_map is not None:
            output_dict = self.map_data(output_dict)
        
        if self.namespace:  # This catches both "None" and empty string values ""
            if self.namespace != "info":
                # TODO: Make self.namespace a safe string for fw metadata
                output_dict = {self.namespace: output_dict}

        output_dict = self.cleanse_the_filthy_numpy(output_dict)

        return output_dict

    def map_data(self, old_dict):
        """ maps keys to namespaces or renames columns.
        
        I envision a yaml file with something like:
        
        remap:
            existing_column_name1: new_column_name1
            existing_column_name2: new_column_name2
        
        namespace:
            namespace1:
                - new_column_name1
                - etc
        
        
        So we first go through and rename everything, then we recategorize things into a namespace
        
        Args:
            old_dict (dict): an old dictionary containing keys to be remapped/renamed.

        Returns: new_dict (dict): a new dictionary with certain keys remapped/renamed

        """

        # process_yaml.py file

        ###### FOR TESTING PURPOSES:
        # with open('/Users/davidparker/Documents/Flywheel/SSE/MyWork/Gears/Metadata_import_Errorprone/matlab_metaimport/tests/test_mapper.yaml') as file:
        #     yaml_file = yaml.load(file, Loader=yaml.FullLoader)
        #

        remap = self.data_map.get("remap")

        if remap is not None:
            if not isinstance(remap, dict):
                log.warning("'remap' section of mapping yaml must be of type dict.")

            else:
                for key in list(old_dict.keys()):
                    if key in remap:
                        old_dict[remap[key]] = old_dict[key]
                        old_dict.pop(key)

        namespaces = self.data_map.get("namespace")

        if namespaces is not None:
            # TODO: Add type check here

            new_dict = {}
            # Loop through namespaces and add relavant keys to that.
            for ns, vals in namespaces.items():

                # TODO: improve check if namespace collides with other keys.
                if ns in old_dict:
                    log.warning(f"namespace {ns} collides with key already in dict")
                    continue

                new_dict[ns] = {}
                for val in vals:
                    new_dict[ns][val] = old_dict.pop(val)

            # merge the two dicts to catch anything not added to the namespaces.

            new_dict.update(old_dict)

        else:
            new_dict = old_dict

        return new_dict
    
    

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
        """Changes Numpy data types to python.
    
        when you read a csv with Pandas, it makes "int" "numpy_int", and flywheel doesn't like 
        that. Does the same for floats and bools, I think.  This fixes it. Numpy is filthy and 
        dirty and we must cleanse it in the beautiful holy light of python.  Through all things 
        python and through python all things.  May the sacred power of python bring us salvation. 
        Do not descend into the temptation of numpy.  Numpy will deceive you.  Numpy is lies. Do 
        not believe their data types.  Only through the true python data types can we realize our 
        best self.  Python will guide us with the power or PEP.  Python is the fabric that 
        binds all things.  Do not believe the false python that is numpy.  Do not use their 
        inferior data types.  Python will provide and protect for us.  Through all things python 
        and through python all things.  Python is the walls that protect us.  Python is the sun 
        that shines on us.  Numpy is the darkness.  Numpy is the cold.  Numpy will swallow you 
        whole into an endless pit of despair.  Only through python may we climb out.  Trust in 
        Python.  Believe in python.  Through all things python and through python all things. 
        Python is the reason for which we live.  Serve python and python will protect you.  
        Python is the true savior.  Python will guide us.  Python knows the way because python is 
        the way.  Numpy is misdirection.  Numpy is dirty.  Numpy is darkness.  Numpy is despair.  
        Do not follow in the path of numpy.  Numpy will tempt you.  Numpy will test you.  Stay 
        strong with python.  Through all things python and through python all things.  Python is 
        the ground on which we walk.  Python is the stars and moon which keep us from darkness in 
        the night.  Numpy is the night.  Python is pure.  Python is clean.  Python will purify 
        you if in python you believe.  Numpy will corrupt you.  Numpy is the lies that tear us 
        apart.  Numpy seeks chaos and destruction.  Numpy lures you with false promises and 
        transient pleasures.  Do not give in to the lies.  Numpy seeks our end.  Numpy seeks our 
        destruction.  Python will save us.  Python is the savior to fight the evil.  Through all 
        things python and through python all things.  Python is the seed from which sprouts all 
        life.  Python is the guiding light that keeps us true.  Numpy seeks to hide the light.  
        Numpy would see everything undone.  Numpy will creep in as a weed and choke out all life. 
        Do not let it.  Trust in python.  Trust in the truth.  Do not leave python. Do not 
        betray python.  Python is the walls that keep us safe.  Python is the light that fights 
        the dark.  Python is the sword that fights for justice.  Through all things python and 
        through python all things.
    
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




def test_MetadataMap_NoDataMap_NoNamespace():
    
    
    import flywheel
    import os
    
    fw = flywheel.Client(os.environ["FWGA_API"])
    target_acq = fw.get_acquisition('6053d2f50858d11c7782d35e')
    # get to it here:
    # https://ga.ce.flywheel.io/#/projects/603d3ab6357432fe34a8318d/sessions/6053d2f40858d11c7782d35d?tab=data
    sample_data = {'Nest1': {'Nested_Key1': "Nested_Value1", 'Nested_Key2': True}, "Key1": 1, "Key2": "Val2"}

    my_map = MetadataMap(fw, data=sample_data, fw_object=target_acq, data_map=None, namespace="")
    my_map.write_metadata()


def test_MetadataMap_NoDataMap_Namespace():
    import flywheel
    import os

    fw = flywheel.Client(os.environ["FWGA_API"])
    target_acq = fw.get_acquisition('6053d2f50858d11c7782d35e')
    # get to it here:
    # https://ga.ce.flywheel.io/#/projects/603d3ab6357432fe34a8318d/sessions/6053d2f40858d11c7782d35d?tab=data
    sample_data = {'Nest1': {'Nested_Key1': "Nested_Value1", 'Nested_Key2': True}, "Key1": 1,
                   "Key2": "Val2"}

    my_map = MetadataMap(fw, data=sample_data, fw_object=target_acq, data_map=None, namespace="Namespace")
    my_map.write_metadata()


def test_MetadataMap_DataMap_NoNamespace():
    import flywheel
    import os

    fw = flywheel.Client(os.environ["FWGA_API"])
    target_acq = fw.get_acquisition('6053d2f50858d11c7782d35e')
    # get to it here:
    # https://ga.ce.flywheel.io/#/projects/603d3ab6357432fe34a8318d/sessions/6053d2f40858d11c7782d35d?tab=data
    sample_data = {'Nest1': {'Nested_Key1': "Nested_Value1", 'Nested_Key2': True}, "Key1": 1,
                   "Key2": "Val2", "Key3":"Val3","Key4":"Val4","Key5":5}
    map_path = '/Users/davidparker/Documents/Flywheel/SSE/MyWork/Gears/Metadata_import_Errorprone/matlab_metaimport/tests/test_mapper.yaml'
    
    my_map = MetadataMap(fw, data=sample_data, fw_object=target_acq, data_map=map_path,
                         namespace="")
    my_map.write_metadata()


def test_MetadataMap_DataMap_Namespace():
    import flywheel
    import os

    fw = flywheel.Client(os.environ["FWGA_API"])
    target_acq = fw.get_acquisition('6053d2f50858d11c7782d35e')
    # get to it here:
    # https://ga.ce.flywheel.io/#/projects/603d3ab6357432fe34a8318d/sessions/6053d2f40858d11c7782d35d?tab=data
    sample_data = {'Nest1': {'Nested_Key1': "Nested_Value1", 'Nested_Key2': True}, "Key1": 1,
                   "Key2": "Val2", "Key3": "Val3", "Key4": "Val4", "Key5": 5}
    map_path = '/Users/davidparker/Documents/Flywheel/SSE/MyWork/Gears/Metadata_import_Errorprone/matlab_metaimport/tests/test_mapper.yaml'

    my_map = MetadataMap(fw, data=sample_data, fw_object=target_acq, data_map=map_path,
                         namespace="MainSpace")
    my_map.write_metadata()


def clear_acq():
    import flywheel
    import os

    fw = flywheel.Client(os.environ["FWGA_API"])
    target_acq = fw.get_acquisition('6053d2f50858d11c7782d35e')
    
    target_acq.replace_info({})
    

if __name__ == "__main__":
    clear_acq()
    # test_MetadataMap_DataMap_Namespace()
    # test_MetadataMap_DataMap_NoNamespace()
    # test_MetadataMap_NoDataMap_Namespace()
    # test_MetadataMap_NoDataMap_NoNamespace()
    # test_the_class()
