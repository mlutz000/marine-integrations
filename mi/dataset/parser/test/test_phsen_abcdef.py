#!/usr/bin/env python

"""
@package mi.dataset.parser.test
@file marine-integrations/mi/dataset/parser/test/test_phsen_abcdef.py
@author Joe Padula
@brief Test code for a Phsen_abcdef data parser
"""

__author__ = 'Joe Padula'
__license__ = 'Apache 2.0'

import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.parser.phsen_abcdef import StateKey
from mi.dataset.dataset_driver import DataSetDriverConfigKeys
from mi.dataset.parser.phsen_abcdef import PhsenRecoveredParser, PhsenRecoveredMetadataDataParticle, \
    PhsenRecoveredInstrumentDataParticle
from mi.core.exceptions import SampleException

from mi.idk.config import Config
RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi', 'dataset', 'driver', 'mflm', 'phsen', 'resource')


@attr('UNIT', group='mi')\


class PhsenRecoveredParserUnitTestCase(ParserUnitTestCase):
    """
    Phsen ABCDEF Parser unit test suite
    """

    def state_callback(self, state, file_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.state_callback_value = state
        self.file_ingested = file_ingested

    def pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.publish_callback_value = pub

    def exception_callback(self, exception):
        """ Call back method to watch what comes in via the exception callback """
        self.exception_callback_value.append(exception)

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.phsen',
            DataSetDriverConfigKeys.PARTICLE_CLASS: ['PhsenRecoveredMetadataDataParticle']
        }
        # Define test data particles and their associated timestamps which will be
        # compared with returned results
        # starts file index 367
        self.particle_a = PhsenRecoveredInstrumentDataParticle(['10', '3456975600', '2276',
                                                               '2955', '2002', '2436', '2495',
                                                                '2962', '1998', '2440', '2492',
                                                                '2960', '2001', '2440', '2494',
                                                                '2964', '2002', '2444', '2496',
                                                                '2962', '2004', '2438', '2496',
                                                                '2960', '2002', '2437', '2494',
                                                                '2959', '1977', '2438', '2477',
                                                                '2963', '1653', '2440', '2219',
                                                                '2961', '1121', '2441', '1757',
                                                                '2962', '694', '2437', '1327',
                                                                '2963', '465', '2439', '1059',
                                                                '2958', '365', '2436', '933',
                                                                '2959', '343', '2434', '901',
                                                                '2961', '370', '2443', '937',
                                                                '2960', '425', '2441', '1013',
                                                                '2961', '506', '2438', '1118',
                                                                '2962', '602', '2441', '1232',
                                                                '2963', '707', '2439', '1356',
                                                                '2964', '828', '2440', '1484',
                                                                '2962', '948', '2439', '1604',
                                                                '2962', '1065', '2440', '1716',
                                                                '2968', '1173', '2444', '1816',
                                                                '2962', '1273', '2440', '1910',
                                                                '2961', '1363', '2442', '1986',
                                                                '2959', '1449', '2439', '2059',
                                                                '2963', '1521', '2442', '2120',
                                                                '2962', '1585', '2439', '2171',
                                                                '0', '2857', '2297'])

        self.particle_b = PhsenRecoveredInstrumentDataParticle(['10', '3456975899', '2342',
                                                                '2969', '1989', '2461', '2501',
                                                                '2965', '1988', '2459', '2498',
                                                                '2969', '1984', '2461', '2497',
                                                                '2964', '1990', '2459', '2498',
                                                                '2964', '1987', '2458', '2497',
                                                                '2968', '1984', '2460', '2496',
                                                                '2965', '1965', '2464', '2485',
                                                                '2969', '1633', '2464', '2278',
                                                                '2966', '1081', '2461', '1881',
                                                                '2968', '647', '2464', '1487',
                                                                '2966', '425', '2461', '1234',
                                                                '2971', '336', '2462', '1117',
                                                                '2967', '321', '2462', '1093',
                                                                '2970', '348', '2464', '1134',
                                                                '2968', '407', '2461', '1212',
                                                                '2968', '484', '2461', '1312',
                                                                '2966', '581', '2461', '1424',
                                                                '2966', '687', '2461', '1537',
                                                                '2968', '806', '2462', '1652',
                                                                '2968', '922', '2463', '1755',
                                                                '2969', '1036', '2463', '1849',
                                                                '2966', '1142', '2465', '1939',
                                                                '2968', '1243', '2464', '2016',
                                                                '2966', '1334', '2463', '2082',
                                                                '2969', '1417', '2467', '2142',
                                                                '2969', '1487', '2469', '2187',
                                                                '2969', '1553', '2468', '2234',
                                                                '0', '2856', '2351'])

        self.particle_c = PhsenRecoveredInstrumentDataParticle(['10', '3456976199',	'2371',
                                                                '2976', '1973',	'2498',	'2509',
                                                                '2978', '1970',	'2496',	'2507',
                                                                '2977', '1972',	'2496',	'2509',
                                                                '2977', '1976',	'2499',	'2511',
                                                                '2978', '1976',	'2498',	'2510',
                                                                '2981', '1972',	'2500',	'2512',
                                                                '2980', '1949',	'2496',	'2494',
                                                                '2975', '1627',	'2499',	'2324',
                                                                '2979', '1064',	'2499',	'1962',
                                                                '2978', '634',	'2501',	'1599',
                                                                '2980', '419',	'2501',	'1363',
                                                                '2979', '335',	'2501',	'1252',
                                                                '2978', '323',	'2502',	'1230',
                                                                '2977', '346',	'2499',	'1264',
                                                                '2979', '393',	'2500',	'1329',
                                                                '2981', '470',	'2500',	'1424',
                                                                '2978', '562',	'2503',	'1524',
                                                                '2984', '670',	'2504',	'1631',
                                                                '2980', '778',	'2500',	'1736',
                                                                '2980', '891',	'2502',	'1830',
                                                                '2983', '1006',	'2505',	'1919',
                                                                '2982', '1115',	'2504',	'1999',
                                                                '2981', '1210',	'2502',	'2065',
                                                                '2981', '1303',	'2502',	'2129',
                                                                '2981', '1382',	'2506',	'2179',
                                                                '2984', '1456',	'2508',	'2227',
                                                                '2982', '1524',	'2502',	'2269',
                                                                '0',	'2854',	'2373'])

        self.particle_128 = PhsenRecoveredMetadataDataParticle(['128',	'3456970176', '65', '1', '0', '512'])

        self.particle_129 = PhsenRecoveredMetadataDataParticle(['129',	'3456975599', '67', '4', '0', '566'])

        self.state_callback_value = None
        self.publish_callback_value = None
        self.exception_callback_value = []

    def assert_result(self, result, in_process_data, unprocessed_data, particle):
        self.assertEqual(result, [particle])
        self.assert_state(in_process_data, unprocessed_data)
        self.assert_(isinstance(self.publish_callback_value, list))
        self.assertEqual(self.publish_callback_value[0], particle)

    def test_simple(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        """

        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'SAMI_P0080_180713_simple.txt'))

        self.parser = PhsenRecoveredParser(self.config, None, stream_handle,
                                           self.state_callback, self.pub_callback, self.exception_callback)

        result = self.parser.get_records(1)
        expected_value = self.particle_128
        self.assertEqual(result, [expected_value])

        result = self.parser.get_records(1)
        expected_value = PhsenRecoveredMetadataDataParticle(['133',	'3456974356', '65', '2', '0', '530'])
        self.assertEqual(result, [expected_value])

        # skipping over second 133 record
        self.parser.get_records(1)

        result = self.parser.get_records(1)
        expected_value = self.particle_129
        self.assertEqual(result, [expected_value])

        # next record is 191, which we handle but should not get
        result = self.parser.get_records(1)
        expected_value = PhsenRecoveredMetadataDataParticle(['191',	'3456975599', '67', '4', '0', '566', '666'])
        self.assertEqual(result, [expected_value])

        # next record is 255, which we handle but should not get
        result = self.parser.get_records(1)
        expected_value = PhsenRecoveredMetadataDataParticle(['255',	'3456975599', '67', '4', '0', '566', '777'])
        self.assertEqual(result, [expected_value])

        result = self.parser.get_records(1)
        expected_value = PhsenRecoveredMetadataDataParticle(['192',	'3456975599', '67', '4', '0', '566', '888'])
        self.assertEqual(result, [expected_value])

        result = self.parser.get_records(1)
        expected_value = PhsenRecoveredMetadataDataParticle(['193',	'3456975599', '67', '4', '0', '566', '999'])
        self.assertEqual(result, [expected_value])

        result = self.parser.get_records(1)
        expected_value = PhsenRecoveredMetadataDataParticle(['134',	'3456970176', '65', '1', '0', '512'])
        self.assertEqual(result, [expected_value])

        result = self.parser.get_records(1)
        expected_value = PhsenRecoveredMetadataDataParticle(['135',	'3456970176', '65', '1', '0', '512'])
        self.assertEqual(result, [expected_value])

        result = self.parser.get_records(1)
        expected_value = PhsenRecoveredMetadataDataParticle(['190',	'3456970176', '65', '1', '0', '512'])
        self.assertEqual(result, [expected_value])

        result = self.parser.get_records(1)
        expected_value = PhsenRecoveredMetadataDataParticle(['194',	'3456970176', '65', '1', '0', '512'])
        self.assertEqual(result, [expected_value])

        result = self.parser.get_records(1)
        expected_value = PhsenRecoveredMetadataDataParticle(['195',	'3456970176', '65', '1', '0', '512'])
        self.assertEqual(result, [expected_value])

        result = self.parser.get_records(1)
        expected_value = PhsenRecoveredMetadataDataParticle(['196',	'3456970176', '65', '1', '0', '512'])
        self.assertEqual(result, [expected_value])

        result = self.parser.get_records(1)
        expected_value = PhsenRecoveredMetadataDataParticle(['197',	'3456970176', '65', '1', '0', '512'])
        self.assertEqual(result, [expected_value])

        result = self.parser.get_records(1)
        expected_value = PhsenRecoveredMetadataDataParticle(['198',	'3456970176', '65', '1', '0', '512'])
        self.assertEqual(result, [expected_value])

        result = self.parser.get_records(1)
        expected_value = PhsenRecoveredMetadataDataParticle(['254',	'3456970176', '65', '1', '0', '512'])
        self.assertEqual(result, [expected_value])

        result = self.parser.get_records(1)
        expected_value = self.particle_a

        # log.debug("actual %s", result[0].raw_data)
        # log.debug("expected %s", expected_value.raw_data)
        self.assertEqual(result, [expected_value])

        result = self.parser.get_records(1)
        expected_value = PhsenRecoveredMetadataDataParticle(['131',	'3456982799', '71', '29', '0', '6128'])
        self.assertEqual(result, [expected_value])
        stream_handle.close()

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_multiple.txt'))
        self.parser = PhsenRecoveredParser(self.config, None, stream_handle, self.state_callback,
                                           self.pub_callback, self.exception_callback)

        result = self.parser.get_records(3)
        self.assertEqual(result, [self.particle_a, self.particle_b, self.particle_c])

        stream_handle.close()

    def test_invalid_num_fields_ph(self):
        """
        Test that the ph records have correct number of fields.
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_invalid_ph.txt'))
        self.parser = PhsenRecoveredParser(self.config, None, stream_handle, self.state_callback,
                                           self.pub_callback, self.exception_callback)

        self.parser.get_records(1)

        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        stream_handle.close()

    def test_invalid_num_fields_control(self):
        """
        Test that the generic control records have correct number of fields
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_invalid_control.txt'))
        self.parser = PhsenRecoveredParser(self.config, None, stream_handle, self.state_callback,
                                           self.pub_callback, self.exception_callback)

        self.parser.get_records(1)

        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        stream_handle.close()

    def test_invalid_num_fields_special_control(self):
        """
        Test that the special control records have correct number of fields
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_invalid_special_control.txt'))
        self.parser = PhsenRecoveredParser(self.config, None, stream_handle, self.state_callback,
                                           self.pub_callback, self.exception_callback)

        self.parser.get_records(1)

        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        stream_handle.close()

    def test_recover(self):
        """
        Test that we can recover after receiving bad record
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_recover.txt'))
        self.parser = PhsenRecoveredParser(self.config, None, stream_handle, self.state_callback,
                                           self.pub_callback, self.exception_callback)
        self.parser.get_records(1)

        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        stream_handle.close()

    def test_unknown_msg_type(self):
        """
        Test that we handle unsupported msg type
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_unknown_msg_type.txt'))
        self.parser = PhsenRecoveredParser(self.config, None, stream_handle, self.state_callback,
                                           self.pub_callback, self.exception_callback)

        self.parser.get_records(1)

        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        stream_handle.close()

    def test_alpha_type(self):
        """
        Test that we handle alpha msg type
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_alpha_type.txt'))
        self.parser = PhsenRecoveredParser(self.config, None, stream_handle, self.state_callback,
                                           self.pub_callback, self.exception_callback)

        self.parser.get_records(1)

        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        stream_handle.close()

    def test_alpha_field(self):
        """
        Test that we handle an alpha field
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_alpha_field.txt'))
        self.parser = PhsenRecoveredParser(self.config, None, stream_handle, self.state_callback,
                                           self.pub_callback, self.exception_callback)

        self.parser.get_records(1)

        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        stream_handle.close()

    def test_space_begin(self):
        """
        Test that we handle record that begin with a space
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_space_begin.txt'))
        self.parser = PhsenRecoveredParser(self.config, None, stream_handle, self.state_callback,
                                           self.pub_callback, self.exception_callback)

        self.parser.get_records(1)

        self.assert_(isinstance(self.exception_callback_value[0], SampleException))

        stream_handle.close()

    def test_no_data_tag(self):
        """
        Test that we do not create a particle if the file does not contain the ':Data' tag
        """
        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_no_data_tag.txt'))
        self.parser = PhsenRecoveredParser(self.config, None, stream_handle, self.state_callback,
                                           self.pub_callback, self.exception_callback)

        result = self.parser.get_records(1)
        self.assertEqual(result, [])

        stream_handle.close()

    def test_mid_state_start(self):
        """
        Test starting the parser in a state in the middle of processing
        """
        new_state = {StateKey.POSITION: 0x50b, StateKey.START_OF_DATA: False}
        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'SAMI_P0080_180713_simple.txt'))
        self.parser = PhsenRecoveredParser(self.config, new_state, stream_handle,
                                           self.state_callback, self.pub_callback, self.exception_callback)
        result = self.parser.get_records(1)
        expected_value = self.particle_128
        self.assertEqual(result, [expected_value])
        stream_handle.close()

    def test_mid_state_after_start(self):
        """
        Test starting the parser in a state after the start point
        """
        new_state = {StateKey.POSITION: 0x55f, StateKey.START_OF_DATA: True}
        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'SAMI_P0080_180713_simple.txt'))
        self.parser = PhsenRecoveredParser(self.config, new_state, stream_handle,
                                           self.state_callback, self.pub_callback, self.exception_callback)
        result = self.parser.get_records(1)
        expected_value = self.particle_129

        self.assertEqual(result, [expected_value])
        stream_handle.close()

    def test_mid_state_before_ph(self):
        """
        Test starting the parser in a state before the ph record
        """
        new_state = {StateKey.POSITION: 0x6db, StateKey.START_OF_DATA: True}
        stream_handle = open(os.path.join(RESOURCE_PATH,
                                          'SAMI_P0080_180713_simple.txt'))
        self.parser = PhsenRecoveredParser(self.config, new_state, stream_handle,
                                           self.state_callback, self.pub_callback, self.exception_callback)
        result = self.parser.get_records(1)
        expected_value = self.particle_a

        self.assertEqual(result, [expected_value])
        stream_handle.close()

    def test_set_state(self):
        """
        Test changing to a new state after initializing the parser and
        reading data, as if new data has been found and the state has
        changed
        """

        stream_handle = open(os.path.join(RESOURCE_PATH, 'SAMI_P0080_180713_simple.txt'))
        self.parser = PhsenRecoveredParser(self.config, None, stream_handle,
                                           self.state_callback, self.pub_callback, self.exception_callback)
        # there should only be 4 records, make sure we stop there
        result = self.parser.get_records(1)
        expected_value = self.particle_128
        self.assertEqual(result, [expected_value])

        new_state = {StateKey.POSITION: 0x6db, StateKey.START_OF_DATA: True}

        self.parser.set_state(new_state)
        result = self.parser.get_records(1)

        expected_value = self.particle_a

        self.assertEqual(result, [expected_value])

        stream_handle.close()
