#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_flord_l_wfp
@file marine-integrations/mi/dataset/parser/test/test_flord_l_wfp.py
@author Maria Lutz
@brief Test code for a flord_l_wfp data parser
"""

import os
import ntplib, struct
from datetime import datetime
from nose.plugins.attrib import attr

from mi.core.log import get_logger ; log = get_logger()
from mi.core.exceptions import SampleException, UnexpectedDataException

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_driver import DataSetDriverConfigKeys
from mi.dataset.parser.sio_mule_common import StateKey
from mi.dataset.parser.flord_l_wfp_sio_mule import FlordLWfpSioMuleParser
from mi.dataset.parser.flord_l_wfp_sio_mule import FlordLWfpSioMuleParserDataParticle

from mi.idk.config import Config

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi',
                             'dataset', 'driver', 'flord_l_wfp',
                             'resource')

@attr('UNIT', group='mi')
class FlordLWfpParserUnitTestCase(ParserUnitTestCase):
    """
    flord_l_wfp Parser unit test suite
    """
    def state_callback(self, state, file_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.state_callback_value = state
        self.file_ingested_value = file_ingested

    def pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.publish_callback_value = pub

    def setUp(self):
	ParserUnitTestCase.setUp(self)
	self.config = {
	    DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.flord_l_wfp',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'FlordLWfpParserDataParticle'
	    }
        # Define test data particles and their associated timestamps which will be 
        # compared with returned results

        self.file_ingested_value = None
        self.state_callback_value = None
        self.publish_callback_value = None
	
        def assert_result(self, result, in_process_data, unprocessed_data, particle):
        self.assertEqual(result, [particle])
        self.assert_state(in_process_data, unprocessed_data) 
        self.assert_(isinstance(self.publish_callback_value, list))
        self.assertEqual(self.publish_callback_value[0], particle)
           
    def assert_state(self, in_process_data, unprocessed_data):
        self.assertEqual(self.parser._state[StateKey.IN_PROCESS_DATA], in_process_data)
        self.assertEqual(self.parser._state[StateKey.UNPROCESSED_DATA], unprocessed_data)
        self.assertEqual(self.state_callback_value[StateKey.IN_PROCESS_DATA], in_process_data)
        self.assertEqual(self.state_callback_value[StateKey.UNPROCESSED_DATA], unprocessed_data)
    
    def timestamp_to_ntp(self, hex_timestamp):
        fields = struct.unpack('>I', hex_timestamp)
        timestamp = float(fields[0])
        return ntplib.system_to_ntp_time(timestamp)
    
    def convert_utc_to_zulu(self, hex_timestamp):
	fields = struct.unpack('>I', hex_timestamp)
        timestamp = float(fields[0])
	zulu_time = datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
	return zulu_time

    def test_simple(self):
        """
	Read test data and pull out data particles one at a time.
	Assert that the results are those we expected.
	"""
        pass

    def test_get_many(self):
	"""
	Read test data and pull out multiple data particles at one time.
	Assert that the results are those we expected.
	"""
        pass

    def test_long_stream(self):
        """
        Test a long stream 
        """
        pass

    def test_mid_state_start(self):
        """
        Test starting the parser in a state in the middle of processing
        """
        pass

    def test_set_state(self):
        """
        Test changing to a new state after initializing the parser and 
        reading data, as if new data has been found and the state has
        changed
        """
        pass

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        pass
