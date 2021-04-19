import pytest
import flywheel
import os

from utils.Object_Finder import FlywheelObjectFinder

fw = flywheel.Client(os.environ['FWGA_KEY'])

def test_find_levels():
    
    test_object = FlywheelObjectFinder()