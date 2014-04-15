#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_dosta_ln_wfp_sio_mule
@file marine-integrations/mi/dataset/parser/test/test_dosta_ln_wfp_sio_mule.py
@author Christopher Fortin
@brief Test code for a dosta_ln_wfp_sio_mule data parser
"""
#!/usr/bin/env python

import gevent
import unittest
import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger ; log = get_logger()

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.parser.sio_mule_common import StateKey
from mi.dataset.parser.dosta_ln_wfp_sio_mule import DostaLnWfpSioMuleParser
from mi.dataset.parser.dosta_ln_wfp_sio_mule import DostaLnWfpSioMuleParserDataParticle
from mi.dataset.dataset_driver import DataSetDriverConfigKeys
from mi.core.instrument.data_particle import DataParticleKey

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

        self.timestamp1 = 3583725976.97
        self.particle_a = DostaLnWfpSioMuleParserDataParticle(b'\x00\x01\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x51\xf3' \
            '\x37\xb2\x51\xf3\x38\x2d\x51\xf3\x38\x2d\x00\x00\x00\x00\x41\x3b' \
            '\x67\xa1\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x38\x00\x62\x02\x3e',
            internal_timestamp=self.timestamp1, new_sequence=True)
	
        self.state_callback_value = None
        self.publish_callback_value = None
        self.exception_callback_value = None

    def assert_result(self, result, in_process_data, unprocessed_data, timestamp, particle):
        self.assertEqual(result, [particle])
        self.assert_state(in_process_data, unprocessed_data, timestamp)
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
        self.state = {StateKey.UNPROCESSED_DATA:[[0, 20000]],
            StateKey.IN_PROCESS_DATA:[], StateKey.TIMESTAMP:0.0}
        self.parser = DostaLnWfpSioMuleParser(self.config, self.state, self.stream_handle,
                                  self.state_callback, self.pub_callback, self.exception_callback)

        result = self.parser.get_records(1)
        self.assert_result(result,
                           [[1447,1833,1,0],[3827,4214,1,0],[4471,4857,1,0]],
                           [[0,32],[222,871],[1447,3058],[3248,4281],[4471,5000]], 
                           self.timestamp1, self.particle_a)


        self.stream_handle.close()
