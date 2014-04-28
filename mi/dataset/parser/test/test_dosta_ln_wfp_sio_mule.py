#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_dosta_ln_wfp_sio_mule
@file marine-integrations/mi/dataset/parser/test/test_dosta_ln_wfp_sio_mule.py
@author Christopher Fortin
@brief Test code for a dosta_ln_wfp_sio_mule data parser
"""
#!/usr/bin/env python

import binascii
import unittest
import os
from nose.plugins.attrib import attr

from StringIO import StringIO
from mi.core.log import get_logger ; log = get_logger()
from mi.core.exceptions import SampleException
from mi.core.instrument.data_particle import DataParticleKey

from mi.core.log import get_logger ; log = get_logger()
from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_driver import DataSetDriverConfigKeys
from mi.core.instrument.data_particle import DataParticleKey
from mi.dataset.parser.sio_mule_common import StateKey
from mi.dataset.parser.dosta_ln_wfp_sio_mule import DostaLnWfpSioMuleParser
from mi.dataset.parser.dosta_ln_wfp_sio_mule import DostaLnWfpSioMuleParserDataParticle

from mi.idk.config import Config

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi',
			     'dataset', 'driver', 'dosta_ln',
			     'wfp_sio_mule', 'resource')


@attr('UNIT', group='mi')
class DostaLnWfpSioParserUnitTestCase(ParserUnitTestCase):

    def state_callback(self, state):
        """ Call back method to watch what comes in via the position callback """
        self.state_callback_value = state

    def pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.publish_callback_value = pub

    def exception_callback(self, exception):
        """ Call back method to watch what comes in via the exception callback """
        self.exception_callback_value = exception

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.dosta_ln_wfp_sio_mule',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'DostaLnWfpSioMuleParserDataParticle'
            }

	# First 'WE' SIO header in node58p1.dat, E file header.
	self.particle_e_header = DostaLnWfpSioMuleParserDataParticle(b'\x00\x01\x00\x00\x00' \
	    '\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01Q\xf2V\xb3Q\xf2W.')

	# First 'WE' SIO header in node58p1.dat, first record. 
	self.timestamp_1a = 2986504401 #'Q\xf2W.' # record timestamp. need to convert this.
	self.particle_1a = DostaLnWfpSioMuleParserDataParticle(b'Q\xf2W.\x00\x00\x00\x00A9Y' \
	    '\xb4\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x009\x00e\x02:',
	    internal_timestamp = self.timestamp_1a)
	
	# First 'WE' SIO header in node58p1.dat, second record.
	self.timestamp_1b = 4110643409 # 'Q\xf2Xq'
	self.particle_1b = DostaLnWfpSioMuleParserDataParticle(b'Q\xf2XqB\x8f\x83DA5\x1e\xb8D' \
	    '\xfd\x85qB\x82\x83\x12?\xf9\xba^\x009\x00d\x028',
	    internal_timestamp = self.timestamp_1b)
	
	# First 'WE' SIO header in node58p1.dat, third record.
	self.timestamp_1c  = 5754941649 #'Q\xf2Z\xd3'
	self.particle_1c = DostaLnWfpSioMuleParserDataParticle(b'Q\xf2Z\xd3B\x84\x06GA2\x9a\xd4E' \
	    '\t\xd3\xd7B\x9b\xdc)?\xec\xac\x08\x00:\x00d\x027',
	    internal_timestamp = self.timestamp_1c)

	# Second 'WE' SIO header in node58p1.dat, first record.
	self.timestamp_1d  = 4063916241 #'Q\xf2Z\xd3'
	self.particle_1d = DostaLnWfpSioMuleParserDataParticle(b'\x51\xf2\x8f\x6e\x00\x00\x00\x00\x41' \
							       '\x37\xd5\x66\x00\x00\x00\x00\x00\x00\x00\x00' \
							       '\x00\x00\x00\x00\x00\x38\x00\x61\x02\x3d',
	    internal_timestamp = self.timestamp_1d)

	# Last 'WE' SIO header in node58p1.dat[0:3e5], last record ( when reading 12 ).
	self.timestamp_1l  = 4114903249 #'Q\xf2Z\xd3'
	self.particle_1l = DostaLnWfpSioMuleParserDataParticle(b'\x51\xf2\x99\x71\x43\x22\x09\xce\x41\x2f' \
							       '\x9a\x6c\x45\x4d\x07\x5c\x43\x07\xd7\x0a\x3f' \
							       '\xc3\x95\x81\x00\x37\x00\x5f\x02\x3b',
	    internal_timestamp = self.timestamp_1l)

	# Last 'WE' SIO header in node58p1.dat[0:300000], second to last record ( when reading 12 ).
	self.timestamp_1k  = 3041095889 #'Q\xf2Z\xd3'
	self.particle_1k = DostaLnWfpSioMuleParserDataParticle(b'\x51\xf2\x98\x31\x43\x10\xe5\x6b\x41\x2f\xe4' \
							       '\x26\x45\x47\x8c\x00\x43\x04\xc2\x8f\x3f\xc4\xfd' \
							       '\xf4\x00\x36\x00\x5f\x02\x3b',
	    internal_timestamp = self.timestamp_1k)

        self.file_ingested_value = None
	self.state_callback_value = None
        self.publish_callback_value = None
        self.exception_callback_value = None

    def assert_result(self, result, particle):
	log.debug("result: %s", result)
	log.debug("particle: %s", particle)
	log.debug("*********result: %s", binascii.hexlify(result[0].raw_data))
	log.debug("*********particle: %s", binascii.hexlify(particle.raw_data))
	
        self.assertEqual(result, [particle])
	
        #self.assert_state(in_process_data, unprocessed_data, timestamp)
        self.assert_(isinstance(self.publish_callback_value, list))
        self.assertEqual(self.publish_callback_value[0], particle)

    def assert_state(self, in_process_data, unprocessed_data, timestamp):
        self.assertEqual(self.parser._state[StateKey.IN_PROCESS_DATA], in_process_data)
        self.assertEqual(self.parser._state[StateKey.UNPROCESSED_DATA], unprocessed_data)
        self.assertEqual(self.state_callback_value[StateKey.IN_PROCESS_DATA], in_process_data)
        self.assertEqual(self.state_callback_value[StateKey.UNPROCESSED_DATA], unprocessed_data)
        self.assertAlmostEqual(self.state_callback_value[StateKey.TIMESTAMP], timestamp, places=6)

    def test_simple(self):
        """
        Read test data from the file and pull out data particles one at a time.
        Assert that the results are those we expected.
        """
        log.debug('Starting test_simple')
        self.stream_handle = open(os.path.join(RESOURCE_PATH,
                                               'node58p1.dat'))
        # NOTE: using the unprocessed data state of 0,5000 limits the file to reading
        # just 5000 bytes, so even though the file is longer it only reads the first
        # 5000
        self.state = {StateKey.UNPROCESSED_DATA:[[0, 30000]],
            StateKey.IN_PROCESS_DATA:[], StateKey.TIMESTAMP:0.0}
        self.parser = DostaLnWfpSioMuleParser(self.config, self.state, self.stream_handle,
                                  self.state_callback, self.pub_callback, self.exception_callback)

        result = self.parser.get_records(1)
        self.assert_result(result, self.particle_1a)

	result = self.parser.get_records(1)
        self.assert_result(result, self.particle_1b)

	result = self.parser.get_records(1)
        self.assert_result(result, self.particle_1c)

 	result = self.parser.get_records(1)
        self.assert_result(result, self.particle_1d)

	self.stream_handle.close()
	
	
    def test_get_many(self):
        """
        Read test data from the file and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
	    
	
        log.debug('Starting test_get_many')
        self.state = {StateKey.UNPROCESSED_DATA:[[0, 30000]],
            StateKey.IN_PROCESS_DATA:[], StateKey.TIMESTAMP:0.0}
        self.stream_handle = open(os.path.join(RESOURCE_PATH,
                                               'node58p1.dat'))
        self.parser = DostaLnWfpSioMuleParser(self.config, self.state, self.stream_handle,
                                  self.state_callback, self.pub_callback, self.exception_callback) 

        result = self.parser.get_records(4)
        self.stream_handle.close()
        self.assertEqual(result,
                         [self.particle_1a, self.particle_1b, self.particle_1c, self.particle_1d])

        self.assertEqual(self.publish_callback_value[0], self.particle_1a)
        self.assertEqual(self.publish_callback_value[1], self.particle_1b)
        self.assertEqual(self.publish_callback_value[2], self.particle_1c)
        self.assertEqual(self.publish_callback_value[3], self.particle_1d)

    
    def test_long_stream(self):
        """
        Test a long stream 
        """

        log.debug('Starting test_long_stream')
        self.stream_handle = open(os.path.join(RESOURCE_PATH,
                                               'node58p1.dat'))
        data = self.stream_handle.read()
        data_len = len(data)
        self.stream_handle.seek(0)
        self.state = {StateKey.UNPROCESSED_DATA:[[0, 30000]],
            StateKey.IN_PROCESS_DATA:[], StateKey.TIMESTAMP:0.0}
        self.parser = DostaLnWfpSioMuleParser(self.config, self.state, self.stream_handle,
                                  self.state_callback, self.pub_callback, self.exception_callback)

        result = self.parser.get_records(12)
        self.stream_handle.close()
        self.assertEqual(result[0], self.particle_1a)
        self.assertEqual(result[1], self.particle_1b)
        self.assertEqual(result[2], self.particle_1c)
        self.assertEqual(result[3], self.particle_1d)
	print "---**********----"
	print ":".join("{:02x}".format(ord(c)) for c in result[-2].raw_data)
	print "---"
	print ":".join("{:02x}".format(ord(c)) for c in self.particle_1k.raw_data)
        self.assertEqual(result[-2], self.particle_1k)
        self.assertEqual(result[-1], self.particle_1l)

        self.assertEqual(self.publish_callback_value[-2], self.particle_1k)
        self.assertEqual(self.publish_callback_value[-1], self.particle_1l)
        
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
