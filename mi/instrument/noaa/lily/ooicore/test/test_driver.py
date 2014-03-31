"""
@package mi.instrument.noaa.lily.ooicore.test.test_driver
@file marine-integrations/mi/instrument/noaa/lily/ooicore/driver.py
@author David Everett
@brief Test cases for ooicore driver

USAGE:
 Make tests verbose and provide stdout
   * From the IDK
       $ bin/test_driver
       $ bin/test_driver -u [-t testname]
       $ bin/test_driver -i [-t testname]
       $ bin/test_driver -q [-t testname]
"""

__author__ = 'David Everett'
__license__ = 'Apache 2.0'

import time

from nose.plugins.attrib import attr
from mock import Mock

from mi.core.log import get_logger


log = get_logger()

# MI imports.
from mi.idk.unit_test import InstrumentDriverTestCase
from mi.idk.unit_test import InstrumentDriverUnitTestCase
from mi.idk.unit_test import InstrumentDriverIntegrationTestCase
from mi.idk.unit_test import InstrumentDriverQualificationTestCase
from mi.idk.unit_test import DriverTestMixin
from mi.idk.unit_test import ParameterTestConfigKey
from mi.idk.unit_test import AgentCapabilityType

from mi.core.instrument.port_agent_client import PortAgentPacket

from mi.core.instrument.chunker import StringChunker

from mi.instrument.noaa.lily.ooicore.driver import InstrumentDriver, DEFAULT_LEVELING_TIMEOUT
from mi.instrument.noaa.lily.ooicore.driver import DataParticleType
from mi.instrument.noaa.lily.ooicore.driver import LILYDataParticleKey
from mi.instrument.noaa.lily.ooicore.driver import LILYDataParticle
from mi.instrument.noaa.lily.ooicore.driver import InstrumentCommand
from mi.instrument.noaa.lily.ooicore.driver import ProtocolState
from mi.instrument.noaa.lily.ooicore.driver import ProtocolEvent
from mi.instrument.noaa.lily.ooicore.driver import Capability
from mi.instrument.noaa.lily.ooicore.driver import Parameter
from mi.instrument.noaa.lily.ooicore.driver import Protocol
from mi.instrument.noaa.lily.ooicore.driver import Prompt
from mi.instrument.noaa.lily.ooicore.driver import NEWLINE
from mi.instrument.noaa.lily.ooicore.driver import LILY_COMMAND_STRING
from mi.instrument.noaa.lily.ooicore.driver import LILY_DATA_ON
from mi.instrument.noaa.lily.ooicore.driver import LILY_DATA_OFF
from mi.instrument.noaa.lily.ooicore.driver import LILY_DUMP_01
from mi.instrument.noaa.lily.ooicore.driver import LILY_DUMP_02
from mi.instrument.noaa.lily.ooicore.driver import LILY_LEVEL_ON
from mi.instrument.noaa.lily.ooicore.driver import LILY_LEVEL_OFF
from mi.instrument.noaa.lily.ooicore.driver import DEFAULT_XTILT_TRIGGER
from mi.instrument.noaa.lily.ooicore.driver import DEFAULT_YTILT_TRIGGER

from mi.core.exceptions import SampleException
from pyon.agent.agent import ResourceAgentState
from pyon.agent.common import BaseEnum

###
#   Driver parameters for the tests
###
InstrumentDriverTestCase.initialize(
    driver_module='mi.instrument.noaa.lily.ooicore.driver',
    driver_class="InstrumentDriver",

    instrument_agent_resource_id='1D644T',
    instrument_agent_name='noaa_lily_ooicore',
    instrument_agent_packet_config=DataParticleType(),

    driver_startup_config={}
)

GO_ACTIVE_TIMEOUT = 180

#################################### RULES ####################################
#                                                                             #
# Common capabilities in the base class                                       #
#                                                                             #
# Instrument specific stuff in the derived class                              #
#                                                                             #
# Generator spits out either stubs or comments describing test this here,     #
# test that there.                                                            #
#                                                                             #
# Qualification tests are driven through the instrument_agent                 #
#                                                                             #
###############################################################################

###
#   Driver constant definitions
###

INVALID_SAMPLE = "This is an invalid sample; it had better cause an exception." + NEWLINE
VALID_SAMPLE_01 = "LILY,2013/06/24 23:36:02,-235.500,  25.930,194.30, 26.04,11.96,N9655" + NEWLINE
VALID_SAMPLE_02 = "LILY,2013/06/24 23:36:04,-235.349,  26.082,194.26, 26.04,11.96,N9655" + NEWLINE
#                 "LILY,2013/07/24 20:36:27,*  14.667,  81.642,185.21, 33.67,11.59,N9651" + NL


DATA_ON_COMMAND_RESPONSE = "LILY,2013/05/29 00:23:34," + LILY_COMMAND_STRING + LILY_DATA_ON + NEWLINE
DATA_OFF_COMMAND_RESPONSE = "LILY,2013/05/29 00:23:34," + LILY_COMMAND_STRING + LILY_DATA_OFF + NEWLINE
DUMP_01_COMMAND_RESPONSE = "LILY,2013/05/29 00:22:57," + LILY_COMMAND_STRING + LILY_DUMP_01 + NEWLINE
DUMP_02_COMMAND_RESPONSE = "LILY,2013/05/29 00:23:34," + LILY_COMMAND_STRING + LILY_DUMP_02 + NEWLINE
START_LEVELING_COMMAND_RESPONSE = "LILY,2013/05/29 00:23:34," + LILY_COMMAND_STRING + LILY_LEVEL_ON + NEWLINE
STOP_LEVELING_COMMAND_RESPONSE = "LILY,2013/05/29 00:23:34," + LILY_COMMAND_STRING + LILY_LEVEL_OFF + NEWLINE

BOTPT_FIREHOSE_01 = "NANO,P,2013/05/16 17:03:22.000,14.858126,25.243003840" + NEWLINE
BOTPT_FIREHOSE_01 += "HEAT,2013/04/19 22:54:11,-001,0001,0025" + NEWLINE
BOTPT_FIREHOSE_01 += "IRIS,2013/05/29 00:25:34, -0.0882, -0.7524,28.45,N8642" + NEWLINE
BOTPT_FIREHOSE_01 += "NANO,P,2013/05/16 17:03:22.000,14.858126,25.243003840" + NEWLINE
BOTPT_FIREHOSE_01 += "LILY,2013/06/24 23:36:02,-235.500,  25.930,194.30, 26.04,11.96,N9655" + NEWLINE
BOTPT_FIREHOSE_01 += "HEAT,2013/04/19 22:54:11,-001,0001,0025" + NEWLINE

BOTPT_FIREHOSE_02 = "NANO,P,2013/05/16 17:03:22.000,14.858126,25.243003840" + NEWLINE
BOTPT_FIREHOSE_02 += "HEAT,2013/04/19 22:54:11,-001,0001,0025" + NEWLINE
BOTPT_FIREHOSE_02 += "LILY,2013/06/24 22:36:02,-235.500,  25.930,194.30, 26.04,11.96,N9655" + NEWLINE
BOTPT_FIREHOSE_02 += "IRIS,2013/05/29 00:25:34, -0.0882, -0.7524,28.45,N8642" + NEWLINE
BOTPT_FIREHOSE_02 += "NANO,P,2013/05/16 17:03:22.000,14.858126,25.243003840" + NEWLINE
BOTPT_FIREHOSE_02 += "LILY,2013/06/24 23:36:02,-235.500,  25.930,194.30, 26.04,11.96,N9655" + NEWLINE
BOTPT_FIREHOSE_02 += "HEAT,2013/04/19 22:54:11,-001,0001,0025" + NEWLINE

DUMP_01_STATUS = \
    "LILY,2013/06/24 23:35:41,*APPLIED GEOMECHANICS LILY Firmware V2.1 SN-N9655 ID01" + NEWLINE + \
    "LILY,2013/06/24 23:35:41,*01: Vbias= 0.0000 0.0000 0.0000 0.0000" + NEWLINE + \
    "LILY,2013/06/24 23:35:41,*01: Vgain= 0.0000 0.0000 0.0000 0.0000" + NEWLINE + \
    "LILY,2013/06/24 23:35:41,*01: Vmin:  -2.50  -2.50   2.50   2.50" + NEWLINE + \
    "LILY,2013/06/24 23:35:41,*01: Vmax:   2.50   2.50   2.50   2.50" + NEWLINE + \
    "LILY,2013/06/24 23:35:41,*01: a0=    0.00000    0.00000    0.00000    0.00000    0.00000    0.00000" + NEWLINE + \
    "LILY,2013/06/24 23:35:41,*01: a1=    0.00000    0.00000    0.00000    0.00000    0.00000    0.00000" + NEWLINE + \
    "LILY,2013/06/24 23:35:41,*01: a2=    0.00000    0.00000    0.00000    0.00000    0.00000    0.00000" + NEWLINE + \
    "LILY,2013/06/24 23:35:41,*01: a3=    0.00000    0.00000    0.00000    0.00000    0.00000    0.00000" + NEWLINE + \
    "LILY,2013/06/24 23:35:41,*01: Tcoef 0: Ks=           0 Kz=           0 Tcal=           0" + NEWLINE + \
    "LILY,2013/06/24 23:35:41,*01: Tcoef 1: Ks=           0 Kz=           0 Tcal=           0" + NEWLINE + \
    "LILY,2013/06/24 23:35:41,*01: N_SAMP= 360 Xzero=  0.00 Yzero=  0.00" + NEWLINE + \
    "LILY,2013/06/24 23:35:41,*01: TR-PASH-OFF E99-ON  SO-NMEA-SIM XY-EP 19200 baud FV-" + NEWLINE

DUMP_02_STATUS = \
    "LILY,2013/06/24 23:36:05,*01: TBias: 5.00" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Above 0.00(KZMinTemp): kz[0]=           0, kz[1]=           0" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Below 0.00(KZMinTemp): kz[2]=           0, kz[3]=           0" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: ADCDelay:  310" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: PCA Model: 84833-14" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Firmware Version: 2.1 Rev D" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: X Ch Gain= 1.0000, Y Ch Gain= 1.0000, Temperature Gain= 1.0000" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Calibrated in uRadian, Current Output Mode: uRadian" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Using RS232" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Real Time Clock: Installed" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Use RTC for Timing: Yes" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: External Flash: 2162688 Bytes Installed" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Flash Status (in Samples) (Used/Total): (-1/55424)" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Low Power Logger Data Rate: -1 Seconds per Sample" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Calibration method: Dynamic " + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Positive Limit=330.00   Negative Limit=-330.00 " + NEWLINE + \
    "IRIS,2013/06/24 23:36:05, -0.0680, -0.3284,28.07,N3616" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Calibration Points:023  X: Enabled  Y: Enabled" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Uniaxial (x2) Sensor Type (1)" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: ADC: 16-bit(external)" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Compass: Installed   Magnetic Declination: 0.000000" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Compass: Xoffset:   12, Yoffset:  210, Xrange: 1371, Yrange: 1307" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: PID Coeff: iMax:100.0, iMin:-100.0, iGain:0.0150, pGain: 2.50, dGain: 10.0" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Motor I_limit: 90.0mA" + NEWLINE + \
    "LILY,2013/06/24 23:36:05,*01: Current Time: 01/11/00 02:12:32" + NEWLINE + \
    "LILY,2013/06/24 23:36:06,*01: Supply Voltage: 11.96 Volts" + NEWLINE + \
    "LILY,2013/06/24 23:36:06,*01: Memory Save Mode: Off" + NEWLINE + \
    "LILY,2013/06/24 23:36:06,*01: Outputting Data: Yes" + NEWLINE + \
    "LILY,2013/06/24 23:36:06,*01: Auto Power-Off Recovery Mode: Off" + NEWLINE + \
    "LILY,2013/06/24 23:36:06,*01: Advanced Memory Mode: Off, Delete with XY-MEMD: No" + NEWLINE

LEVELING_STATUS = \
    "LILY,2013/07/24 20:36:27,*  14.667,  81.642,185.21, 33.67,11.59,N9651" + NEWLINE

LEVELED_STATUS = \
    "LILY,2013/06/28 17:29:21,*  -2.277,  -2.165,190.81, 25.69,,Leveled!11.87,N9651" + NEWLINE

SWITCHING_STATUS = \
    "LILY,2013/06/28 18:04:41,*  -7.390, -14.063,190.91, 25.83,,Switching to Y!11.87,N9651"


###############################################################################
#                           DRIVER TEST MIXIN                                 #
#     Defines a set of constants and assert methods used for data particle    #
#     verification                                                            #
#                                                                             #
#  In python mixin classes are classes designed such that they wouldn't be    #
#  able to stand on their own, but are inherited by other classes generally   #
#  using multiple inheritance.                                                #
#                                                                             #
# This class defines a configuration structure for testing and common assert  #
# methods for validating data particles.                                      #
###############################################################################
class LILYTestMixinSub(DriverTestMixin):
    TYPE = ParameterTestConfigKey.TYPE
    READONLY = ParameterTestConfigKey.READONLY
    STARTUP = ParameterTestConfigKey.STARTUP
    DA = ParameterTestConfigKey.DIRECT_ACCESS
    VALUE = ParameterTestConfigKey.VALUE
    REQUIRED = ParameterTestConfigKey.REQUIRED
    DEFAULT = ParameterTestConfigKey.DEFAULT
    STATES = ParameterTestConfigKey.STATES

    _driver_parameters = {
        # Parameters defined in the IOS
        Parameter.AUTO_RELEVEL: {TYPE: bool, READONLY: False, DA: False, STARTUP: False},
    }

    _sample_parameters_01 = {
        LILYDataParticleKey.TIME: {TYPE: float, VALUE: 3581130962.0, REQUIRED: True},
        LILYDataParticleKey.X_TILT: {TYPE: float, VALUE: -235.500, REQUIRED: True},
        LILYDataParticleKey.Y_TILT: {TYPE: float, VALUE: 25.930, REQUIRED: True},
        LILYDataParticleKey.MAG_COMPASS: {TYPE: float, VALUE: 194.30, REQUIRED: True},
        LILYDataParticleKey.TEMP: {TYPE: float, VALUE: 26.04, REQUIRED: True},
        LILYDataParticleKey.SUPPLY_VOLTS: {TYPE: float, VALUE: 11.96, REQUIRED: True},
        LILYDataParticleKey.SN: {TYPE: unicode, VALUE: 'N9655', REQUIRED: True}
    }

    _sample_parameters_02 = {
        LILYDataParticleKey.TIME: {TYPE: float, VALUE: 3581130964.0, REQUIRED: True},
        LILYDataParticleKey.X_TILT: {TYPE: float, VALUE: -235.349, REQUIRED: True},
        LILYDataParticleKey.Y_TILT: {TYPE: float, VALUE: 26.082, REQUIRED: True},
        LILYDataParticleKey.MAG_COMPASS: {TYPE: float, VALUE: 194.26, REQUIRED: True},
        LILYDataParticleKey.TEMP: {TYPE: float, VALUE: 26.04, REQUIRED: True},
        LILYDataParticleKey.SUPPLY_VOLTS: {TYPE: float, VALUE: 11.96, REQUIRED: True},
        LILYDataParticleKey.SN: {TYPE: unicode, VALUE: 'N9655', REQUIRED: True}
    }

    def assert_particle_sample_01(self, data_particle, verify_values=False):
        """
        Verify sample particle
        @param data_particle:  LILYDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(LILYDataParticleKey, self._sample_parameters_01)
        self.assert_data_particle_header(data_particle, DataParticleType.LILY_PARSED, require_instrument_timestamp=True)
        self.assert_data_particle_parameters(data_particle, self._sample_parameters_01, verify_values)

    def assert_particle_sample_02(self, data_particle, verify_values=False):
        """
        Verify sample particle
        @param data_particle:  LILYDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(LILYDataParticleKey, self._sample_parameters_02)
        self.assert_data_particle_header(data_particle, DataParticleType.LILY_PARSED, require_instrument_timestamp=True)
        self.assert_data_particle_parameters(data_particle, self._sample_parameters_02, verify_values)

    def assert_particle_sample_firehose(self, data_particle, verify_values=False):
        """
        Verify sample particle
        @param data_particle:  LILYDataParticle data particle
        @param verify_values:  bool, should we verify parameter values
        """
        self.assert_data_particle_keys(LILYDataParticleKey, self._sample_parameters_01)
        self.assert_data_particle_header(data_particle, DataParticleType.LILY_PARSED, require_instrument_timestamp=True)
        self.assert_data_particle_parameters(data_particle, self._sample_parameters_01, verify_values)

    def assert_particle_status(self, status_particle, verify_values=False):
        pass


###############################################################################
#                                UNIT TESTS                                   #
#         Unit tests test the method calls and parameters using Mock.         #
#                                                                             #
#   These tests are especially useful for testing parsers and other data      #
#   handling.  The tests generally focus on small segments of code, like a    #
#   single function call, but more complex code using Mock objects.  However  #
#   if you find yourself mocking too much maybe it is better as an            #
#   integration test.                                                         #
#                                                                             #
#   Unit tests do not start up external processes like the port agent or      #
#   driver process.                                                           #
###############################################################################
# noinspection PyProtectedMember,PyUnusedLocal
@attr('UNIT', group='mi')
class DriverUnitTest(InstrumentDriverUnitTestCase, LILYTestMixinSub):
    def setUp(self):
        InstrumentDriverUnitTestCase.setUp(self)

    @staticmethod
    def my_send(driver):
        def inner(data):
            if data.startswith(InstrumentCommand.DATA_ON):
                my_response = DATA_ON_COMMAND_RESPONSE
            elif data.startswith(InstrumentCommand.DATA_OFF):
                my_response = DATA_OFF_COMMAND_RESPONSE
            elif data.startswith(InstrumentCommand.DUMP_SETTINGS_01):
                my_response = DUMP_01_COMMAND_RESPONSE
            elif data.startswith(InstrumentCommand.DUMP_SETTINGS_02):
                my_response = DUMP_02_COMMAND_RESPONSE
            elif data.startswith(InstrumentCommand.START_LEVELING):
                my_response = START_LEVELING_COMMAND_RESPONSE
            elif data.startswith(InstrumentCommand.STOP_LEVELING):
                my_response = STOP_LEVELING_COMMAND_RESPONSE
            else:
                my_response = None
            if my_response is not None:
                log.debug("my_send: data: %s, my_response: %s", data, my_response)
                driver._protocol._promptbuf += my_response
                return len(my_response)

        return inner

    def _send_port_agent_packet(self, driver, data):
        port_agent_packet = PortAgentPacket()
        port_agent_packet.attach_data(data)
        port_agent_packet.attach_timestamp(self.get_ntp_timestamp())
        port_agent_packet.pack_header()
        # Push the response into the driver
        driver._protocol.got_data(port_agent_packet)

    # Test the connection to the BOTPT
    def test_connect(self, initial_protocol_state=ProtocolState.COMMAND):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        driver = InstrumentDriver(self._got_data_event_callback)
        self.assert_initialize_driver(driver, initial_protocol_state)
        return driver

    def test_async_send_event(self):
        class TestStates(BaseEnum):
            test_state = 'TEST_STATE'

        class TestEvents(BaseEnum):
            test_event = 'TEST_EVENT'

        # Create and initialize the instrument driver
        driver = self.test_connect()

        # Patch the FSM's states and events variables to our own test states and
        # events; need to do this because the test wouldn't work for all drivers
        # unless we new a common event and state that they all have, and that
        # haven't already had a handler associated with them.
        driver._protocol._protocol_fsm.states = TestStates
        driver._protocol._protocol_fsm.events = TestEvents

        self.my_handler_entered = False

        # Define a test handler for the fsm to call that will set a variable that
        # we can then test to make sure the handler was called.

        def _my_handler_test(*args, **kwargs):
            next_state = None
            result = None

            self.my_handler_entered = True
            return next_state, result

        driver._protocol._protocol_fsm.add_handler(TestStates.test_state, TestEvents.test_event, _my_handler_test)
        self.assert_force_state(driver, TestStates.test_state)
        driver._protocol._protocol_fsm.on_event(TestEvents.test_event)

        self.assertTrue(self.my_handler_entered)

    def test_driver_enums(self):
        """
        Verify that all driver enumeration has no duplicate values that might cause confusion.  Also
        do a little extra validation for the Capabilities
        """
        self.assert_enum_has_no_duplicates(DataParticleType())
        self.assert_enum_has_no_duplicates(ProtocolState())
        self.assert_enum_has_no_duplicates(ProtocolEvent())
        self.assert_enum_has_no_duplicates(Parameter())
        self.assert_enum_has_no_duplicates(InstrumentCommand())

        # Test capabilities for duplicates, then verify that capabilities is a subset of protocol events
        self.assert_enum_has_no_duplicates(Capability())
        self.assert_enum_complete(Capability(), ProtocolEvent())

    def test_chunker(self):
        """
        Test the chunker and verify the particles created.
        """
        chunker = StringChunker(Protocol.sieve_function)
        self.assert_chunker_sample(chunker, LEVELED_STATUS)
        self.assert_chunker_sample(chunker, VALID_SAMPLE_01)
        self.assert_chunker_sample(chunker, DUMP_01_STATUS)
        self.assert_chunker_sample(chunker, DUMP_02_STATUS)

    def test_get_handler(self):
        driver = self.test_connect()

        args = [Parameter.AUTO_RELEVEL]
        result = driver._protocol._handler_command_get(args)
        dict_response = result[1]
        get_auto_relevel_response = dict_response['auto_relevel']
        log.debug("get_auto_relevel_response: %r", get_auto_relevel_response)
        self.assertTrue(get_auto_relevel_response)

    def test_set_handler(self):
        driver = self.test_connect()
        driver._protocol._handler_command_set({Parameter.XTILT_RELEVEL_TRIGGER: 10})

    def test_combined_samples(self):
        chunker = StringChunker(Protocol.sieve_function)

        sample = BOTPT_FIREHOSE_02

        ts = self.get_ntp_timestamp()
        chunker.add_chunk(sample, ts)

        timestamp, result = chunker.get_next_data()
        self.assertTrue(result in sample)
        self.assertEqual(timestamp, ts)

        timestamp, result = chunker.get_next_data()
        self.assertTrue(result in sample)
        self.assertEqual(timestamp, ts)

        timestamp, result = chunker.get_next_data()
        self.assertEqual(result, None)

    def test_leveling_status(self):
        chunker = StringChunker(Protocol.sieve_function)

        sample = LEVELING_STATUS

        ts = self.get_ntp_timestamp()
        chunker.add_chunk(sample, ts)

        (timestamp, result) = chunker.get_next_data()
        self.assertEqual(result, sample)
        self.assertEqual(timestamp, ts)

        (timestamp, result) = chunker.get_next_data()
        self.assertEqual(result, None)

    def test_data_build_parsed_values(self):
        """
        Verify that the BOTPT LILY driver build_parsed_values method
        raises SampleException when an invalid sample is encountered
        and that it returns a result when a valid sample is encountered
        """
        driver = self.test_connect()

        items = [
            (INVALID_SAMPLE, False),
            (VALID_SAMPLE_01, True),
            (VALID_SAMPLE_02, True),
        ]

        for raw_data, is_valid in items:
            sample_exception = False
            result = None
            try:
                test_particle = LILYDataParticle(raw_data, False)
                result = test_particle._build_parsed_values()
            except SampleException as e:
                log.debug('SampleException caught: %s.', e)
                sample_exception = True
            finally:
                if is_valid:
                    self.assertFalse(sample_exception)
                    self.assertTrue(isinstance(result, list))
                else:
                    self.assertTrue(sample_exception)

    def test_got_data(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = self.test_connect()

        self.assert_particle_published(driver, VALID_SAMPLE_01, self.assert_particle_sample_01, True)
        self.assert_particle_published(driver, VALID_SAMPLE_02, self.assert_particle_sample_02, True)

    def test_firehose(self):
        """
        Verify sample data passed through the got data method produces the correct data particles
        Verify that the BOTPT LILY driver publishes a particle correctly when the LILY packet is
        embedded in the stream of other BOTPT sensor output.
        """
        # Create and initialize the instrument driver with a mock port agent
        driver = self.test_connect()
        self.assert_particle_published(driver, BOTPT_FIREHOSE_01, self.assert_particle_sample_01, True)

    def test_data_on_response(self):
        """
        Verify that the driver correctly parses the DATA_ON response
        """
        driver = self.test_connect()
        log.debug("DATA ON command response: %s", DATA_ON_COMMAND_RESPONSE)
        # Create and populate the port agent packet.
        self._send_port_agent_packet(driver, DATA_ON_COMMAND_RESPONSE)
        self.assertTrue(driver._protocol._get_response(expected_prompt=LILY_DATA_ON))

    def test_data_on_response_with_data(self):
        """
        Verify that the driver correctly parses the DATA_ON response works
        when a data packet is right in front of it
        """
        driver = self.test_connect()
        # Create a data packet and push to the driver
        log.debug("VALID SAMPLE : %s", VALID_SAMPLE_01)
        # Create and populate the port agent packet.
        self._send_port_agent_packet(driver, VALID_SAMPLE_01)

        log.debug("DATA ON command response: %s", DATA_ON_COMMAND_RESPONSE)
        # Create and populate the port agent packet.
        self._send_port_agent_packet(driver, DATA_ON_COMMAND_RESPONSE)
        self.assertTrue(driver._protocol._get_response(expected_prompt=LILY_DATA_ON))

    def test_status_01(self):
        """
        Verify that the driver correctly parses the DUMP-SETTINGS response
        """
        driver = self.test_connect()

        data_list = [
            DUMP_01_STATUS.rstrip(),
            "HEAT,2013/06/19 23:04:37,-001,0000,0026",
            "LILY,2013/06/19 23:04:38, -49.455,  34.009,193.91, 26.02,11.96,N9655",
            "NANO,V,2013/06/19 23:04:38.000,13.987223,25.126694121",
            "LILY,2013/06/19 23:04:39, -49.483,  33.959,193.85, 26.03,11.96,N9655",
            "NANO,V,2013/06/19 23:04:39.000,13.987191,25.126709409",
            "LILY,2013/06/19 23:04:40, -49.355,  33.956,193.79, 26.02,11.96,N9655",
            "NANO,V,2013/06/19 23:04:40.000,13.987253,25.126725854",
            "HEAT,2013/06/19 23:04:40,-001,0000,0026",
            "IRIS,2013/06/19 21:46:54,*APPLIED GEOMECHANICS Model MD900-T Firmware V5.2 SN-N3616 ID01",
            "NANO,V,2013/06/19 21:46:54.000,13.990480,25.027793612",
            "IRIS,2013/06/19 21:46:54,*01: Vbias= 0.0000 0.0000 0.0000 0.0000",
            "IRIS,2013/06/19 21:46:54,*01: Vgain= 0.0000 0.0000 0.0000 0.0000",
            "IRIS,2013/06/19 21:46:54,*01: Vmin:  -2.50  -2.50   2.50   2.50",
            "IRIS,2013/06/19 21:46:54,*01: Vmax:   2.50   2.50   2.50   2.50",
            "IRIS,2013/06/19 21:46:54,*01: a0=    0.00000    0.00000    0.00000    0.00000    0.00000    0.00000",
            "IRIS,2013/06/19 21:46:54,*01: a1=    0.00000    0.00000    0.00000    0.00000    0.00000    0.00000",
            "IRIS,2013/06/19 21:46:54,*01: a2=    0.00000    0.00000    0.00000    0.00000    0.00000    0.00000",
            "IRIS,2013/06/19 21:46:54,*01: a3=    0.00000    0.00000    0.00000    0.00000    0.00000    0.00000",
            "IRIS,2013/06/19 21:46:55,*01: Tcoef 0: Ks=           0 Kz=           0 Tcal=           0",
            "IRIS,2013/06/19 21:46:55,*01: Tcoef 1: Ks=           0 Kz=           0 Tcal=           0",
            "IRIS,2013/06/19 21:46:55,*01: N_SAMP= 460 Xzero=  0.00 Yzero=  0.00",
            "IRIS,2013/06/19 21:46:55,*01: TR-PASH-OFF E99-ON  SO-NMEA-SIM XY-EP  9600 baud FV-   ",
            "IRIS,2013/06/19 22:04:55,*9900XY-DUMP-SETTINGS"
        ]

        for item in data_list:
            self._send_port_agent_packet(driver, item + NEWLINE)

    def test_status_02(self):
        """
        Verify that the driver correctly parses the DUMP2 response
        """
        driver = self.test_connect()
        log.debug("DUMP_02_STATUS: %s", DUMP_02_STATUS)
        # Create and populate the port agent packet.
        self._send_port_agent_packet(driver, DUMP_02_STATUS + DUMP_02_COMMAND_RESPONSE)

    def test_data_off_response(self):
        """
        Verify that the driver correctly parses the DATA_OFF response
        """
        driver = self.test_connect()
        log.debug("DATA OFF command response: %s", DATA_OFF_COMMAND_RESPONSE)
        # Create and populate the port agent packet.
        self._send_port_agent_packet(driver, DATA_OFF_COMMAND_RESPONSE)
        self.assertTrue(driver._protocol._get_response(expected_prompt=LILY_DATA_OFF))

    def test_dump_settings_response(self):
        """
        Verify that the driver correctly parses the DUMP_SETTINGS response
        """
        driver = self.test_connect()
        log.debug("DUMP_SETTINGS_01 command response: %s", DUMP_01_COMMAND_RESPONSE)
        # Create and populate the port agent packet.
        self._send_port_agent_packet(driver, DUMP_01_COMMAND_RESPONSE)
        response = driver._protocol._get_response(expected_prompt=LILY_DUMP_01)
        self.assertTrue(response[1].endswith(LILY_DUMP_01))

        # Clear out the linebuf and promptbuf (do_cmd_resp normally does this)
        driver._protocol._linebuf = ''
        driver._protocol._promptbuf = ''

        log.debug("DUMP_SETTINGS_02 command response: %s", DUMP_02_COMMAND_RESPONSE)
        # Create and populate the port agent packet.
        self._send_port_agent_packet(driver, DUMP_02_COMMAND_RESPONSE)
        response = driver._protocol._get_response(expected_prompt=LILY_DUMP_02)
        self.assertTrue(response[1].endswith(LILY_DUMP_02))

    def test_start_stop_autosample(self):
        driver = self.test_connect()
        driver._connection.send.side_effect = self.my_send(driver)

        driver._protocol._protocol_fsm.on_event(ProtocolEvent.START_AUTOSAMPLE)
        self.assertEqual(driver._protocol.get_current_state(), ProtocolState.AUTOSAMPLE)

        driver._protocol._protocol_fsm.on_event(ProtocolEvent.STOP_AUTOSAMPLE)
        self.assertEqual(driver._protocol.get_current_state(), ProtocolState.COMMAND)

    def test_status_handlers(self):
        driver = self.test_connect()
        driver._connection.send.side_effect = self.my_send(driver)

        driver._protocol._protocol_fsm.on_event(ProtocolEvent.DUMP_01)
        driver._protocol._protocol_fsm.on_event(ProtocolEvent.DUMP_02)

    def test_leveling_timeout(self):
        # stand up the driver in test mode
        driver = self.test_connect()
        driver._connection.send.side_effect = self.my_send(driver)

        # set the leveling timeout to 1 to speed up timeout
        driver._protocol._leveling_timeout = 1
        driver._protocol._protocol_fsm.on_event(ProtocolEvent.START_LEVELING)

        current_state = driver._protocol.get_current_state()
        self.assertEqual(current_state, ProtocolState.COMMAND_LEVELING)

        # sleep for the length of timeout, assert we have returned to COMMAND
        time.sleep(driver._protocol._leveling_timeout + 2)
        current_state = driver._protocol.get_current_state()
        self.assertEqual(current_state, ProtocolState.COMMAND)

    def test_leveling_complete(self):
        driver = self.test_connect()
        driver._connection.send.side_effect = self.my_send(driver)
        driver._protocol._protocol_fsm.on_event(ProtocolEvent.START_LEVELING)
        # assert we have entered a leveling state
        self.assertEqual(driver._protocol.get_current_state(), ProtocolState.COMMAND_LEVELING)
        # feed in a leveling complete status message
        self._send_port_agent_packet(driver, LEVELED_STATUS)
        # Assert we have returned to the command state
        self.assertEquals(driver._protocol.get_current_state(), ProtocolState.COMMAND)

    def test_protocol_filter_capabilities(self):
        """
        This tests driver filter_capabilities.
        Iterate through available capabilities, and verify that they can pass successfully through the filter.
        Test silly made up capabilities to verify they are blocked by filter.
        """
        mock_callback = Mock()
        protocol = Protocol(Prompt, NEWLINE, mock_callback)
        driver_capabilities = Capability().list()
        test_capabilities = Capability().list()

        # Add a bogus capability that will be filtered out.
        test_capabilities.append("BOGUS_CAPABILITY")

        # Verify "BOGUS_CAPABILITY was filtered out
        self.assertEquals(sorted(driver_capabilities),
                          sorted(protocol._filter_capabilities(test_capabilities)))


###############################################################################
#                            INTEGRATION TESTS                                #
#     Integration test test the direct driver / instrument interaction        #
#     but making direct calls via zeromq.                                     #
#     - Common Integration tests test the driver through the instrument agent #
#     and common for all drivers (minimum requirement for ION ingestion)      #
###############################################################################
@attr('INT', group='mi')
class DriverIntegrationTest(InstrumentDriverIntegrationTestCase, LILYTestMixinSub):
    def setUp(self):
        InstrumentDriverIntegrationTestCase.setUp(self)

    def test_connection(self):
        self.assert_initialize_driver()

    def test_get(self):
        self.assert_initialize_driver()
        self.assert_get(Parameter.AUTO_RELEVEL, True)
        self.assert_get(Parameter.XTILT_RELEVEL_TRIGGER, DEFAULT_XTILT_TRIGGER)
        self.assert_get(Parameter.YTILT_RELEVEL_TRIGGER, DEFAULT_YTILT_TRIGGER)

    def test_set(self):
        """
        Test all set commands. Verify all exception cases.
        """
        self.assert_initialize_driver()

        self.assert_set(Parameter.AUTO_RELEVEL, False)
        self.assert_get(Parameter.AUTO_RELEVEL, False)

        self.assert_set(Parameter.AUTO_RELEVEL, True)
        self.assert_get(Parameter.AUTO_RELEVEL, True)

        self.assert_set(Parameter.XTILT_RELEVEL_TRIGGER, 200)
        self.assert_get(Parameter.XTILT_RELEVEL_TRIGGER, 200)

        self.assert_set(Parameter.YTILT_RELEVEL_TRIGGER, 200)
        self.assert_get(Parameter.YTILT_RELEVEL_TRIGGER, 200)

    def test_autosample_leveling(self):
        """
        @brief Test for autosample state leveling
        """
        # This test should be run PRIOR to test_auto_relevel, as it will
        # increase the likelihood that the sensor will be out of level
        # enough to trigger an auto-relevel.
        self.assert_initialize_driver()

        # Begin autosampling
        response = self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.START_AUTOSAMPLE)
        log.debug('START_AUTOSAMPLE returned: %r', response)
        self.assert_state_change(ProtocolState.AUTOSAMPLE, 30)

        #Issue start leveling command
        response = self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.START_LEVELING)
        log.debug("START_LEVELING returned: %r", response)
        self.assert_state_change(ProtocolState.AUTOSAMPLE_LEVELING, 30)

        # Issue stop leveling command
        response = self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.STOP_LEVELING)
        log.debug("STOP_LEVELING returned: %r", response)
        self.assert_state_change(ProtocolState.AUTOSAMPLE, 30)

    def test_command_leveling(self):
        """
        @brief Test for command state leveling
        """
        # This test should be run PRIOR to test_auto_relevel, as it will
        # increase the likelihood that the sensor will be out of level
        # enough to trigger an auto-relevel.
        self.assert_initialize_driver()

        #Issue start leveling command
        response = self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.START_LEVELING)
        log.debug("START_LEVELING returned: %r", response)
        self.assert_state_change(ProtocolState.COMMAND_LEVELING, 30)

        # Issue stop leveling command
        response = self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.STOP_LEVELING)
        log.debug("STOP_LEVELING returned: %r", response)
        self.assert_state_change(ProtocolState.COMMAND, 30)

    def test_auto_relevel(self):
        """
        @brief Test for turning data on
        """
        self.assert_initialize_driver()

        # Begin autosampling
        response = self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.START_AUTOSAMPLE)
        log.debug('START_AUTOSAMPLE returned: %r', response)
        self.assert_state_change(ProtocolState.AUTOSAMPLE, 30)

        # Set the XTILT to a low threshold so that the driver will
        # automatically start the re-leveling operation
        # NOTE: This test MAY fail if the instrument completes
        # leveling before the triggers have been reset to 300

        self.assert_set(Parameter.XTILT_RELEVEL_TRIGGER, 0)
        self.assert_state_change(ProtocolState.AUTOSAMPLE_LEVELING, 30)

        # Now set the XTILT back to normal so that the driver will not
        # automatically start the re-leveling operation

        self.assert_set(Parameter.XTILT_RELEVEL_TRIGGER, 300)

        self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.STOP_LEVELING)
        self.assert_state_change(ProtocolState.AUTOSAMPLE, 30)

    def test_data_on(self):
        """
        @brief Test for turning data on
        """
        self.assert_initialize_driver()
        self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.START_AUTOSAMPLE)
        self.assert_async_particle_generation(DataParticleType.LILY_PARSED,
                                              self.assert_particle_sample_01,
                                              particle_count=5,
                                              timeout=10)
        self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.STOP_AUTOSAMPLE)
        self.assert_state_change(ProtocolState.COMMAND, 1)

    def test_dump_status(self):
        """
        @brief Test for acquiring status
        """
        self.assert_initialize_driver()

        # Issue acquire status command

        response = self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.DUMP_01)
        log.debug("DUMP_01 returned: %r", response)

        response = self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.DUMP_02)
        log.debug("DUMP_02 returned: %r", response)

    def test_leveling_complete(self):
        """
        @brief Test for leveling
        """
        self.assert_initialize_driver()

        #Issue start leveling command

        response = self.driver_client.cmd_dvr('execute_resource', ProtocolEvent.START_LEVELING)
        log.debug("START_LEVELING returned: %r", response)

        # Leveling should complete or abort after DEFAULT_LEVELING_TIMEOUT seconds
        self.assert_state_change(ProtocolState.COMMAND, DEFAULT_LEVELING_TIMEOUT + 10)


###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for doing final testing of ion      #
# integration.  The generally aren't used for instrument debugging and should #
# be tackled after all unit and integration tests are complete                #
###############################################################################
@attr('QUAL', group='mi')
class DriverQualificationTest(InstrumentDriverQualificationTestCase, LILYTestMixinSub):
    def setUp(self):
        InstrumentDriverQualificationTestCase.setUp(self)

    def test_poll(self):
        """
        No polling for a single sample
        """

    def test_get_set_parameters(self):
        """
        verify that all parameters can be get set properly, this includes
        ensuring that read only parameters fail on set.
        """
        self.assert_enter_command_mode()

    def test_get_capabilities(self):
        """
        @brief Verify that the correct capabilities are returned from get_capabilities
        at various driver/agent states.
        """
        self.assert_enter_command_mode()

        ##################
        #  Command Mode
        ##################
        capabilities = {
            AgentCapabilityType.AGENT_COMMAND: self._common_agent_commands(ResourceAgentState.COMMAND),
            AgentCapabilityType.AGENT_PARAMETER: self._common_agent_parameters(),
            AgentCapabilityType.RESOURCE_COMMAND: [
                ProtocolEvent.GET,
                ProtocolEvent.SET,
                ProtocolEvent.START_AUTOSAMPLE,
                ProtocolEvent.DUMP_01,
                ProtocolEvent.DUMP_02,
            ],
            AgentCapabilityType.RESOURCE_INTERFACE: None,
            AgentCapabilityType.RESOURCE_PARAMETER: self._driver_parameters.keys()
        }

        self.assert_capabilities(capabilities)

        ##################
        #  Streaming Mode
        ##################

        capabilities[AgentCapabilityType.AGENT_COMMAND] = self._common_agent_commands(ResourceAgentState.STREAMING)
        capabilities[AgentCapabilityType.RESOURCE_COMMAND] = [
            ProtocolEvent.STOP_AUTOSAMPLE,
            ProtocolEvent.DUMP_01,
            ProtocolEvent.DUMP_02,
        ]

        self.assert_start_autosample()
        self.assert_capabilities(capabilities)
        self.assert_stop_autosample()