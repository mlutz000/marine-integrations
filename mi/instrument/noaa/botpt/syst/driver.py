"""
@package mi.instrument.noaa.syst.ooicore.driver
@file marine-integrations/mi/instrument/noaa/syst/ooicore/driver.py
@author David Everett
@brief Driver for the ooicore
Release notes:

This is the SYST driver, which handles the raw particles as well as the SYST particles from the BOTPT.
"""

__author__ = 'David Everett'
__license__ = 'Apache 2.0'

import re

from mi.core.log import get_logger

log = get_logger()

from mi.core.common import BaseEnum
from mi.core.instrument.instrument_fsm import InstrumentFSM
from mi.core.instrument.instrument_driver import SingleConnectionInstrumentDriver
from mi.core.instrument.instrument_driver import DriverEvent
from mi.core.instrument.instrument_driver import DriverProtocolState
from mi.core.instrument.instrument_driver import DriverParameter
from mi.core.instrument.instrument_driver import ResourceAgentState
from mi.core.instrument.data_particle import CommonDataParticleType
from mi.core.instrument.chunker import StringChunker
from mi.instrument.noaa.botpt.driver import BotptProtocol
from mi.instrument.noaa.botpt.driver import BotptStatusParticle
from mi.instrument.noaa.botpt.driver import BotptStatusParticleKey
from mi.instrument.noaa.botpt.driver import NEWLINE
from mi.core.exceptions import InstrumentProtocolException


###
#    Driver Constant Definitions
###
SYST_STRING = 'SYST,'
DUMP_STATUS = '1'


class DataParticleType(BaseEnum):
    """
    Data particle types produced by this driver
    """
    RAW = CommonDataParticleType.RAW
    STATUS = 'syst_status'


class ProtocolState(BaseEnum):
    """
    Instrument protocol states
    """
    UNKNOWN = DriverProtocolState.UNKNOWN
    COMMAND = DriverProtocolState.COMMAND
    DIRECT_ACCESS = DriverProtocolState.DIRECT_ACCESS


class ProtocolEvent(BaseEnum):
    """
    Protocol events
    """
    ENTER = DriverEvent.ENTER
    EXIT = DriverEvent.EXIT
    GET = DriverEvent.GET
    SET = DriverEvent.SET
    DISCOVER = DriverEvent.DISCOVER
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS
    START_DIRECT = DriverEvent.START_DIRECT
    EXECUTE_DIRECT = DriverEvent.EXECUTE_DIRECT
    STOP_DIRECT = DriverEvent.STOP_DIRECT


class Capability(BaseEnum):
    """
    Protocol events that should be exposed to users (subset of above).
    """
    ACQUIRE_STATUS = DriverEvent.ACQUIRE_STATUS


class InstrumentCommand(BaseEnum):
    """
    Instrument command strings
    """
    ACQUIRE_STATUS = SYST_STRING + DUMP_STATUS


###############################################################################
# Data Particles
###############################################################################

class SYSTStatusParticleKey(BotptStatusParticleKey):
    NAME = 'name'
    SERIAL_NUMBER = 'serial_number'
    UPTIME = 'uptime'
    MEM_STATS = 'memory_stats'
    MEM_TOTAL = 'memory_total_kB'
    MEM_FREE = 'memory_free_kB'
    BUFFERS = 'buffers_kB'
    CACHED = 'cached_kB'
    SWAP_CACHED = 'swap_cached_kB'
    ACTIVE = 'active_kB'
    INACTIVE = 'inactive_kB'
    SWAP_TOTAL = 'swap_total_kB'
    SWAP_FREE = 'swap_free_kB'
    DIRTY = 'dirty_kB'
    WRITEBACK = 'writeback_kB'
    ANONPAGES = 'anon_pages_kB'
    MAPPED = 'mapped_kB'
    SLAB = 'slab_kB'
    S_RECLAIMABLE = 's_reclaimable_kB'
    S_UNRECLAIMABLE = 's_unreclaimable_kB'
    PAGE_TABLES = 'pagetables_kB'
    NFS_UNSTABLE = 'nfs_unstable_kB'
    BOUNCE = 'bounce_kB'
    COMMIT_LIMIT = 'commit_limit_kB'
    COMMITTED_AS = 'committed_as_kB'
    VMALLOC_TOTAL = 'vmalloc_total_kB'
    VMALLOC_USED = 'vmalloc_used_kB'
    VMALLOC_CHUNK = 'vmalloc_chunk_kB'
    NETSTAT = 'netstat'
    PROCESSES = 'processes'


class SYSTStatusParticle(BotptStatusParticle):
    _data_particle_type = DataParticleType.STATUS
    _DEFAULT_ENCODER_KEY = int
    _timestamp_re = r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})'
    _encoders = {
        SYSTStatusParticleKey.SENSOR_ID: unicode,
        SYSTStatusParticleKey.TIME: unicode,
        SYSTStatusParticleKey.NAME: unicode,
        SYSTStatusParticleKey.SERIAL_NUMBER: unicode,
        SYSTStatusParticleKey.UPTIME: unicode,
        SYSTStatusParticleKey.MEM_STATS: unicode,
        SYSTStatusParticleKey.NETSTAT: unicode,
        SYSTStatusParticleKey.PROCESSES: unicode,
    }
    # SYST,2014/04/07 20:46:35,*BOTPT BPR and tilt instrument controller
    # SYST,2014/04/07 20:46:35,*ts7550n3
    # SYST,2014/04/07 20:46:35,*System uptime
    # SYST,2014/04/07 20:46:35,* 20:17:02 up 13 days, 19:11,  0 users,  load average: 0.00, 0.00, 0.00
    # SYST,2014/04/07 20:46:35,*Memory stats
    # SYST,2014/04/07 20:46:35,*             total       used       free     shared    buffers     cached
    # SYST,2014/04/07 20:46:35,*Mem:         62888      18520      44368          0       2260       5120
    # SYST,2014/04/07 20:46:35,*-/+ buffers/cache:      11140      51748
    # SYST,2014/04/07 20:46:35,*Swap:            0          0          0
    # SYST,2014/04/07 20:46:35,*MemTotal:        62888 kB
    # SYST,2014/04/07 20:46:35,*MemFree:         44392 kB
    # SYST,2014/04/07 20:46:35,*Buffers:          2260 kB
    # SYST,2014/04/07 20:46:35,*Cached:           5120 kB
    # SYST,2014/04/07 20:46:35,*SwapCached:          0 kB
    # SYST,2014/04/07 20:46:35,*Active:          10032 kB
    # SYST,2014/04/07 20:46:35,*Inactive:         3328 kB
    # SYST,2014/04/07 20:46:35,*SwapTotal:           0 kB
    # SYST,2014/04/07 20:46:35,*SwapFree:            0 kB
    # SYST,2014/04/07 20:46:35,*Dirty:               0 kB
    # SYST,2014/04/07 20:46:35,*Writeback:           0 kB
    # SYST,2014/04/07 20:46:35,*AnonPages:        6000 kB
    # SYST,2014/04/07 20:46:35,*Mapped:           3976 kB
    # SYST,2014/04/07 20:46:35,*Slab:             3128 kB
    # SYST,2014/04/07 20:46:35,*SReclaimable:      800 kB
    # SYST,2014/04/07 20:46:35,*SUnreclaim:       2328 kB
    # SYST,2014/04/07 20:46:35,*PageTables:        512 kB
    # SYST,2014/04/07 20:46:35,*NFS_Unstable:        0 kB
    # SYST,2014/04/07 20:46:35,*Bounce:              0 kB
    # SYST,2014/04/07 20:46:35,*CommitLimit:     31444 kB
    # SYST,2014/04/07 20:46:35,*Committed_AS:   167276 kB
    # SYST,2014/04/07 20:46:35,*VmallocTotal:   188416 kB
    # SYST,2014/04/07 20:46:35,*VmallocUsed:         0 kB
    # SYST,2014/04/07 20:46:35,*VmallocChunk:   188416 kB
    # SYST,2014/04/07 20:46:35,*Listening network services
    # SYST,2014/04/07 20:46:35,*tcp        0      0 *:9337-commands         *:*                     LISTEN
    # SYST,2014/04/07 20:46:35,*tcp        0      0 *:9338-data             *:*                     LISTEN
    # SYST,2014/04/07 20:46:35,*udp        0      0 *:323                   *:*
    # SYST,2014/04/07 20:46:35,*udp        0      0 *:54361                 *:*
    # SYST,2014/04/07 20:46:35,*udp        0      0 *:mdns                  *:*
    # SYST,2014/04/07 20:46:35,*udp        0      0 *:ntp                   *:*
    # SYST,2014/04/07 20:46:35,*Data processes
    # SYST,2014/04/07 20:46:35,*root       643  0.0  2.2  20100  1436 ?        Sl   Mar25   0:01 /root/bin/COMMANDER
    # SYST,2014/04/07 20:46:35,*root       647  0.0  2.5  21124  1604 ?        Sl   Mar25   0:16 /root/bin/SEND_DATA
    # SYST,2014/04/07 20:46:35,*root       650  0.0  2.2  19960  1388 ?        Sl   Mar25   0:00 /root/bin/DIO_Rel1
    # SYST,2014/04/07 20:46:35,*root       654  0.0  2.1  19960  1360 ?        Sl   Mar25   0:02 /root/bin/HEAT
    # SYST,2014/04/07 20:46:35,*root       667  0.0  2.2  19960  1396 ?        Sl   Mar25   0:00 /root/bin/IRIS
    # SYST,2014/04/07 20:46:35,*root       672  0.0  2.2  19960  1396 ?        Sl   Mar25   0:01 /root/bin/LILY
    # SYST,2014/04/07 20:46:35,*root       678  0.0  2.2  19964  1400 ?        Sl   Mar25   0:12 /root/bin/NANO
    # SYST,2014/04/07 20:46:35,*root       685  0.0  2.2  19960  1396 ?        Sl   Mar25   0:00 /root/bin/RESO
    # SYST,2014/04/07 20:46:35,*root      7860  0.0  0.9   1704   604 ?        S    20:17   0:00 grep root/bin

    def __init__(self, *args, **kwargs):
        BotptStatusParticle.__init__(self, *args, **kwargs)
        self._orig_raw_data = self.raw_data
        self._clean_raw()

    @staticmethod
    def regex():
        return r'(SYST,.*?BOTPT BPR.*?grep.*?)' + NEWLINE

    @staticmethod
    def regex_compiled():
        return re.compile(SYSTStatusParticle.regex(), re.DOTALL)

    def _regex_multiline(self):
        return {
            SYSTStatusParticleKey.SENSOR_ID: self.sensor_id,
            SYSTStatusParticleKey.TIME: self._timestamp_re,
            SYSTStatusParticleKey.NAME: r'(BOTPT.*?)' + NEWLINE,
            SYSTStatusParticleKey.SERIAL_NUMBER: r'(ts.*?)' + NEWLINE,
            SYSTStatusParticleKey.UPTIME: r'(System uptime.*?' + NEWLINE + r'.*?)' + NEWLINE,
            SYSTStatusParticleKey.MEM_STATS: r'(Memory stats.*?' + '.*?'.join([NEWLINE] * 4) + '.*?)' + NEWLINE,
            SYSTStatusParticleKey.MEM_TOTAL: r'MemTotal:\s+(\d+) kB',
            SYSTStatusParticleKey.MEM_FREE: r'MemFree:\s+(\d+) kB',
            SYSTStatusParticleKey.BUFFERS: r'Buffers:\s+(\d+) kB',
            SYSTStatusParticleKey.CACHED: r'Cached:\s+(\d+) kB',
            SYSTStatusParticleKey.SWAP_CACHED: r'SwapCached:\s+(\d+) kB',
            SYSTStatusParticleKey.ACTIVE: r'Active:\s+(\d+) kB',
            SYSTStatusParticleKey.INACTIVE: r'Inactive:\s+(\d+) kB',
            SYSTStatusParticleKey.SWAP_TOTAL: r'SwapTotal:\s+(\d+) kB',
            SYSTStatusParticleKey.SWAP_FREE: r'SwapFree:\s+(\d+) kB',
            SYSTStatusParticleKey.DIRTY: r'Dirty:\s+(\d+) kB',
            SYSTStatusParticleKey.WRITEBACK: r'Writeback:\s+(\d+) kB',
            SYSTStatusParticleKey.ANONPAGES: r'AnonPages:\s+(\d+) kB',
            SYSTStatusParticleKey.MAPPED: r'Mapped:\s+(\d+) kB',
            SYSTStatusParticleKey.SLAB: r'Slab:\s+(\d+) kB',
            SYSTStatusParticleKey.S_RECLAIMABLE: r'SReclaimable:\s+(\d+) kB',
            SYSTStatusParticleKey.S_UNRECLAIMABLE: r'SUnreclaim:\s+(\d+) kB',
            SYSTStatusParticleKey.PAGE_TABLES: r'PageTables:\s+(\d+) kB',
            SYSTStatusParticleKey.NFS_UNSTABLE: r'NFS_Unstable:\s+(\d+) kB',
            SYSTStatusParticleKey.BOUNCE: r'Bounce:\s+(\d+) kB',
            SYSTStatusParticleKey.COMMIT_LIMIT: r'CommitLimit:\s+(\d+) kB',
            SYSTStatusParticleKey.COMMITTED_AS: r'Committed_AS:\s+(\d+) kB',
            SYSTStatusParticleKey.VMALLOC_TOTAL: r'VmallocTotal:\s+(\d+) kB',
            SYSTStatusParticleKey.VMALLOC_USED: r'VmallocUsed:\s+(\d+) kB',
            SYSTStatusParticleKey.VMALLOC_CHUNK: r'VmallocChunk:\s+(\d+) kB',
            SYSTStatusParticleKey.NETSTAT: r'Listening network services(.*)Data processes',
            SYSTStatusParticleKey.PROCESSES: r'(root.*)',
        }

    def _regex_multiline_compiled(self):
        """
        return a dictionary containing compiled regex used to match patterns
        in SBE multiline results.  Overridden to use re.DOTALL.
        @return: dictionary of compiled regexes
        """
        result = {}
        for key, regex in self._regex_multiline().iteritems():
            if key not in result:
                result[key] = re.compile(regex, re.DOTALL)
        return result

    def _clean_raw(self):
        """
        Strip leading 'SYST,date time,*' from each line, preserving the first instance
        on a separate line.
        """
        delimiter = ',*'
        first_time_stamp = self.raw_data.split(delimiter)[0]
        cleaned = re.sub(r'SYST,%s,\*' % self._timestamp_re, '', self.raw_data)
        self.raw_data = first_time_stamp + NEWLINE + cleaned


###############################################################################
# Driver
###############################################################################

class InstrumentDriver(SingleConnectionInstrumentDriver):
    """
    InstrumentDriver subclass
    Subclasses SingleConnectionInstrumentDriver with connection state
    machine.
    """

    def __init__(self, evt_callback):
        """
        Driver constructor.
        @param evt_callback Driver process event callback.
        """
        #Construct superclass.
        SingleConnectionInstrumentDriver.__init__(self, evt_callback)

    ########################################################################
    # Superclass overrides for resource query.
    ########################################################################

    # noinspection PyMethodMayBeStatic
    def get_resource_params(self):
        """
        Return list of device parameters available.
        """
        return DriverParameter.list()

    ########################################################################
    # Protocol builder.
    ########################################################################

    def _build_protocol(self):
        """
        Construct the driver protocol state machine.
        """
        self._protocol = Protocol(BaseEnum, NEWLINE, self._driver_event)


###########################################################################
# Protocol
###########################################################################

# noinspection PyUnusedLocal,PyMethodMayBeStatic
class Protocol(BotptProtocol):
    """
    Instrument protocol class
    Subclasses CommandResponseInstrumentProtocol
    """

    def __init__(self, prompts, newline, driver_event):
        """
        Protocol constructor.
        @param prompts A BaseEnum class containing instrument prompts.
        @param newline The newline.
        @param driver_event Driver process event callback.
        """
        # Construct protocol superclass.
        BotptProtocol.__init__(self, prompts, newline, driver_event)

        # Build protocol state machine.
        self._protocol_fsm = InstrumentFSM(ProtocolState, ProtocolEvent,
                                           ProtocolEvent.ENTER, ProtocolEvent.EXIT)

        # Add event handlers for protocol state machine.
        handlers = {
            ProtocolState.UNKNOWN: [
                (ProtocolEvent.ENTER, self._handler_unknown_enter),
                (ProtocolEvent.EXIT, self._handler_unknown_exit),
                (ProtocolEvent.DISCOVER, self._handler_unknown_discover),
            ],
            ProtocolState.COMMAND: [
                (ProtocolEvent.ENTER, self._handler_command_enter),
                (ProtocolEvent.EXIT, self._handler_command_exit),
                (ProtocolEvent.GET, self._handler_command_get),
                (ProtocolEvent.SET, self._handler_command_set),
                (ProtocolEvent.ACQUIRE_STATUS, self._handler_command_acquire_status),
                (ProtocolEvent.START_DIRECT, self._handler_command_start_direct),
            ],
            ProtocolState.DIRECT_ACCESS: [
                (ProtocolEvent.ENTER, self._handler_direct_access_enter),
                (ProtocolEvent.EXIT, self._handler_direct_access_exit),
                (ProtocolEvent.EXECUTE_DIRECT, self._handler_direct_access_execute_direct),
                (ProtocolEvent.STOP_DIRECT, self._handler_direct_access_stop_direct),
            ]
        }
        for state in handlers:
            for event, handler in handlers[state]:
                self._protocol_fsm.add_handler(state, event, handler)

        # Add build handlers for device commands.
        self._add_build_handler(InstrumentCommand.ACQUIRE_STATUS, self._build_command)
        self._add_response_handler(InstrumentCommand.ACQUIRE_STATUS, self._resp_handler)

        # State state machine in UNKNOWN state.
        self._protocol_fsm.start(ProtocolState.UNKNOWN)

        self._build_command_dict()

        # commands sent sent to device to be filtered in responses for telnet DA
        self._sent_cmds = []

        self._chunker = StringChunker(Protocol.sieve_function)
        self._filter_string = SYST_STRING

    @staticmethod
    def sieve_function(raw_data):
        """
        The method that splits samples
        """
        matchers = []
        return_list = []

        matchers.append(SYSTStatusParticle.regex_compiled())

        for matcher in matchers:
            for match in matcher.finditer(raw_data):
                return_list.append((match.start(), match.end()))

        return return_list

    def got_raw(self, port_agent_packet):
        """
        Called by the port agent client when raw data is available, such as data
        sent by the driver to the instrument, the instrument responses,etc.
        """
        self.publish_raw(port_agent_packet)

    def _got_chunk(self, chunk, ts):
        """
        The base class got_data has gotten a chunk from the chunker.  Pass it to extract_sample
        with the appropriate particle objects and regexes.
        """
        if not self._extract_sample(SYSTStatusParticle, SYSTStatusParticle.regex_compiled(), chunk, ts):
            raise InstrumentProtocolException('Unhandled chunk: %r', chunk)

    def _build_command_dict(self):
        """
        Populate the command dictionary with command.
        """
        self._cmd_dict.add(Capability.ACQUIRE_STATUS, display_name="acquire status")

    def _filter_capabilities(self, events):
        """
        Return a list of currently available capabilities.
        """
        return [x for x in events if Capability.has(x)]

    def _resp_handler(self, response, prompt):
        log.debug('_resp_handler - response: %r prompt: %r', response, prompt)
        return response

    ########################################################################
    # Unknown handlers.
    ########################################################################

    def _handler_unknown_discover(self, *args, **kwargs):
        """
        Always in COMMAND
        """
        next_state = ProtocolState.COMMAND
        result = ResourceAgentState.IDLE

        return next_state, result

    ########################################################################
    # Command handlers.
    ########################################################################

    def _handler_command_acquire_status(self):
        return self._handler_command_generic(InstrumentCommand.ACQUIRE_STATUS,
                                             None, None,
                                             response_regex=SYSTStatusParticle.regex_compiled())