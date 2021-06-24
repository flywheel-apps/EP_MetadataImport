import pytest
import flywheel
import os

from utils.Object_Finder import FlywheelObjectFinder

# fw = flywheel.Client(os.environ['FWGA_KEY'])

def test_find_levels_twoprovided_setcorrectly():
    
    # The fw client object isn't needed for this test
    test_object = FlywheelObjectFinder(fw=None,
        group='group',
        project=None,
        subject=None,
        session=None,
        acquisition=None,
        analysis='analysis',
        file=None)
    
    test_object.find_levels()
    
    assert test_object.lowest_level == 'analysis'
    assert test_object.highest_level == 'group'

def test_find_levels_threeprovided_setcorrectly():
    
    # The fw client object isn't needed for this test
    test_object = FlywheelObjectFinder(fw=None,
                                       group=None,
                                       project="project",
                                       subject=None,
                                       session='session',
                                       acquisition='acquisition',
                                       analysis=None,
                                       file=None)

    test_object.find_levels()

    assert test_object.lowest_level == 'acquisition'
    assert test_object.highest_level == 'project'
    

