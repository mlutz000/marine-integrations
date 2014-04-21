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


class DostaLnWfpSioMuleDataParticleKey(BaseEnum):
    OPTODE_OXYGEN='optode_oxygen'
    OPTODE_TEMPERATURE='optode_temperature'
 

# *** Need to define data regex for this parser ***
HEADER_REGEX = b'(\x00\x01\x00{5,5}\x01\x00{7,7}\x01)([\x00-\xff]{8,8})'
HEADER_MATCHER = re.compile(HEADER_REGEX)

STATUS_START_REGEX = b'\xff\xff\xff[\xfa-\xff]'
STATUS_START_MATCHER = re.compile(STATUS_START_REGEX)

PROFILE_REGEX = b'\xff\xff\xff[\xfa-\xff][\x00-\xff]{12}'
PROFILE_MATCHER = re.compile(PROFILE_REGEX)

SIO_HEADER_BYTES = 32
HEADER_BYTES = 24
E_GLOBAL_SAMPLE_BYTES = 30
STATUS_BYTES = 16


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
	
	print ":".join("{:02x}".format(ord(c)) for c in self.raw_data)

        try:	    
            fields_prof = struct.unpack('>I f f f f f H H H',self.raw_data)
            time_stamp = int(fields_prof[0])

        except (ValueError, TypeError, IndexError) as ex:
            raise SampleException("Error (%s) while decoding parameters in data: [%s]"
                                  % (ex, match.group(0)))

        result = [self._encode_value(DostaLnWfpSioMuleDataParticleKey.OPTODE_OXYGEN, fields_prof[5], float),
		  self._encode_value(DostaLnWfpSioMuleDataParticleKey.OPTODE_TEMPERATURE, fields_prof[6], float)]


        log.debug('DostLnWfpSioMuleDataParticle: particle=%s', result)
        return result


class DostaLnWfpSioMuleParser(SioMuleParser):

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
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index()

        sample_count = 0

        while (chunk != None):
            sio_header_match = SIO_HEADER_MATCHER.match(chunk)
	    
            sample_count = 0
            log.debug('parsing header %s', sio_header_match.group(0)[1:32])
            if sio_header_match.group(1) == self._instrument_id:
                log.debug("matched chunk header %s", chunk[1:32])
		
                data_wrapper_match = HEADER_MATCHER.search(chunk)		
                if data_wrapper_match:
		    
		    payload = chunk[SIO_HEADER_BYTES+HEADER_BYTES+1:]
                    data_sieve = self.we_sieve_function(payload)
		    
                    if data_sieve:
			log.debug('Found data match in chunk %s', chunk[1:32])
			for ii in range(0,len(data_sieve)):    
			    e_record = payload[data_sieve[ii][0]:data_sieve[ii][1]]
			    # particle-ize the data block received, return the record
			    		    
		            fields = struct.unpack('<I', e_record[0:4])
		            timestamp = int(fields[0])
		            self._timestamp = ntplib.system_to_ntp_time(timestamp)
			    
			    if len(e_record) == E_GLOBAL_SAMPLE_BYTES:
			        sample = self._extract_sample(DostaLnWfpSioMuleParserDataParticle,
			                                      None,
			                                      e_record,
			                                      self._timestamp)
			        if sample:
			            # create particle
			            result_particles.append(sample)
			            sample_count += 1

            self._chunk_sample_count.append(sample_count)

            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index()

        return result_particles


    def we_sieve_function(self, raw_data):
        """
        Sort through the raw data to identify new blocks of data that need processing.
        This is needed instead of a regex because blocks are identified by position
        in this binary file.
        """
        data_index = 0
        return_list = []
        raw_data_len = len(raw_data)

        while data_index < raw_data_len:
            # check if this is a status or data sample message
            if STATUS_START_MATCHER.match(raw_data[data_index:data_index+4]):
                return_list.append((data_index, data_index + STATUS_BYTES))
                data_index += STATUS_BYTES
            else:
                return_list.append((data_index, data_index + E_GLOBAL_SAMPLE_BYTES))
                data_index += E_GLOBAL_SAMPLE_BYTES

            remain_bytes = raw_data_len - data_index
            # if the remaining bytes are less than the data sample bytes, all we might have left is a status sample, if we don't we're done
            if remain_bytes < STATUS_BYTES or (remain_bytes < E_GLOBAL_SAMPLE_BYTES and remain_bytes >= STATUS_BYTES and \
            not STATUS_START_MATCHER.match(raw_data[data_index:data_index+4])):
                break
        log.debug("returning we sieve list %s", return_list)
        return return_list
