#!/usr/bin/env python

"""
@package mi.dataset.parser.dosta_ln_wfp_sio_mule
@file marine-integrations/mi/dataset/parser/dosta_ln_wfp_sio_mule.py
@author Christopher Fortin
@brief Parser for the dosta_ln_wfp_sio_mule dataset driver
Release notes:

Initial Release
"""

__author__ = 'Christopher Fortin'
__license__ = 'Apache 2.0'

import re
import struct
import ntplib
import time
import datetime
from dateutil import parser


from mi.core.log import get_logger; log = get_logger()
from mi.dataset.parser.sio_mule_common import SioMuleParser, SIO_HEADER_MATCHER
from mi.core.common import BaseEnum
from mi.core.exceptions import SampleException, RecoverableSampleException, DatasetParserException
from mi.core.instrument.data_particle import DataParticle, DataParticleKey
from mi.core.time import string_to_ntp_date_time

from mi.dataset.dataset_parser import BufferLoadingParser


class DataParticleType(BaseEnum):
    SAMPLE = 'dosta_ln_wfp_sio_mule_parsed'


class DostaLnWfpSioMuleParserDataParticleKey(BaseEnum):
    OPTODE_OXYGEN='optode_oxygen'
    OPTODE_TEMPERATURE='optode_temperature'
 


# *** Need to define data regex for this parser ***
#DATA_REGEX = ''
#DATA_MATCHER = re.compile(DATA_REGEX)
DATA_WRAPPER_REGEX = b'\x00\x01\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x01\x00\x01'
DATA_WRAPPER_MATCHER = re.compile(DATA_WRAPPER_REGEX)
DATA_REGEX = b'\x6e\x7f[\x00-\xFF]{32}([\x00-\xFF]+)([\x00-\xFF]{2})'
DATA_MATCHER = re.compile(DATA_REGEX)



class DostaLnWfpSioMuleParserDataParticle(DataParticle):
    """
    Class for parsing data from the dosta_ln_wfp_sio_mule data set
    """

    _data_particle_type = DataParticleType.SAMPLE
    
    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
	
        # match the data inside the wrapper
        match = DATA_MATCHER.match(self.raw_data)
        if not match:
            raise SampleException("DostaLnWfpSioMuleParserDataParticle: No regex match of \
                                  parsed sample data [%s]", self.raw_data)
        try:
            # placekeeper for now
            fields = struct.unpack('<HHIBBBdHhhhIbBB', match.group(0)[0:34])
            packet_id = fields[0]
            num_bytes = fields[1]

        except (ValueError, TypeError, IndexError) as ex:
            raise SampleException("Error (%s) while decoding parameters in data: [%s]"
                                  % (ex, match.group(0)))

        result = [{DataParticleKey.VALUE_ID: DostaLnWfpSioMuleDataParticleKey.OPTODE_OXYGEN,
                   DataParticleKey.VALUE: optode_ozygen},
                  {DataParticleKey.VALUE_ID: DostaLnWfpSioMuleDataParticleKey.OPTODE_TEMPERATURE,
                   DataParticleKey.VALUE: optode_temperature},]

        log.debug('DostLnWfpSioMuleDataParticle: particle=%s', result)
        return result

    @staticmethod
    def unpack_date(data):
        fields = struct.unpack('HBBBBBB', data)
        log.debug('Unpacked data into date fields %s', fields)
        zulu_ts = "%04d-%02d-%02dT%02d:%02d:%02d.%02dZ" % (
            fields[0], fields[1], fields[2], fields[3],
            fields[4], fields[5], fields[6])
        return zulu_ts


class DostaLnWfpSioMuleParser(BufferLoadingParser):

    def __init__(self,
                 config,
                 state,
                 stream_handle,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 *args, **kwargs):
        super(DostaLnWfpSioMuleParser, self).__init__(config,
                                          stream_handle,
                                          state,
                                          self.sieve_function,
                                          state_callback,
                                          publish_callback,
                                          exception_callback,
                                          'WE',
                                          *args,
                                          **kwargs)

    """
    def set_state(self, state_obj):
        """
        Set the value of the state object for this parser
        @param state_obj The object to set the state to. 
        @throws DatasetParserException if there is a bad state structure
        """
        if not isinstance(state_obj, dict):
            raise DatasetParserException("Invalid state structure")
        self._timestamp = state_obj[StateKey.TIMESTAMP]
        self._state = state_obj
        self._read_state = state_obj

    def _increment_state(self, increment):
        """
        Increment the parser state
        @param timestamp The timestamp completed up to that position
        """
        self._read_state[StateKey.POSITION] += increment
    """

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle, update the position and
        timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state. An empty list of nothing was parsed.
        """            
        result_particles = []
        (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
        self.handle_non_data(non_data, non_end, start)

        while (chunk != None):
             header_match = SIO_HEADER_MATCHER.match(chunk)
            sample_count = 0
            log.debug('parsing header %s', header_match.group(0)[1:32])
            if header_match.group(1) == self._instrument_id:
                log.debug("matched chunk header %s", chunk[1:32])

                print processed_match[1:32]
                print ":".join("{:02x}".format(ord(c)) for c in processed_match[33:])


                data_wrapper_match = DATA_WRAPPER_MATCHER.search(chunk)
                if data_wrapper_match:
                    data_match = DATA_MATCHER.search(data_wrapper_match.group(1))
                    if data_match:
                        log.debug('Found data match in chunk %s', chunk[1:32])
                        # pull out the date string from the data
                        date_str = AdcpsParserDataParticle.unpack_date(data_match.group(0)[11:19])
                        # convert to ntp
                        converted_time = float(parser.parse(date_str).strftime("%s.%f"))
                        adjusted_time = converted_time - time.timezone
                        self._timestamp = ntplib.system_to_ntp_time(adjusted_time)
                        # round to ensure the timestamps match
                        self._timestamp = round(self._timestamp*100)/100
                        log.debug("Converted time \"%s\" (unix: %10.9f) into %10.9f", date_str, adjusted_time, self._timestamp)
                        # particle-ize the data block received, return the record
                        sample = self._extract_sample(DostaLnWfpSioMuleParserDataParticle,
                                                      DATA_MATCHER,
                                                      data_match.group(0),
                                                      self._timestamp)
                        if sample:
                            # create particle
                            result_particles.append(sample)
                            sample_count += 1

            self._chunk_sample_count.append(sample_count)

            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index()

        return result_particles

