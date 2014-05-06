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

SIO_HEADER_BYTES = 32
HEADER_BYTES = 24
E_GLOBAL_SAMPLE_BYTES = 30
STATUS_BYTES = 16
STATUS_BYTES_AUGMENTED = 18


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
	
	# NOTE: since we are dropping the status messages in the sieve, only
	# sampes should make it here	
	if len(self.raw_data) != E_GLOBAL_SAMPLE_BYTES:
		raise SampleException("Error (%s) while decoding parameters in data: [%s]"
				      % (ex, match.group(0)))
	else:
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
                                          *args,
                                          **kwargs)


    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle, update the position and
        timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state. An empty list if nothing was parsed.
        """            
        result_particles = []
        (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index()

        sample_count = 0

        while (chunk != None):
            sio_header_match = SIO_HEADER_MATCHER.match(chunk)
	    
            sample_count = 0
            log.debug('parsing header %s', sio_header_match.group(0)[1:32])
            if sio_header_match.group(1) == 'WE':
		
                log.debug("matched chunk header %s", chunk[0:32])
		
                data_wrapper_match = HEADER_MATCHER.search(chunk)		
                if data_wrapper_match:
		    
		    payload = chunk[SIO_HEADER_BYTES+HEADER_BYTES+1:]
                    data_sieve = self.we_sieve_function(payload)
		    
                    if data_sieve:
			log.debug('Found data match in chunk %s', chunk[1:32])
			for ii in range(0,len(data_sieve)):    
			    e_record = payload[data_sieve[ii][0]:data_sieve[ii][1]]

			    # particle-ize the data block received, return the record		    			    
			    if not STATUS_START_MATCHER.match(e_record[0:STATUS_BYTES]):
				
				fields = struct.unpack('<I', e_record[0:4])
				timestampS = float(fields[0])
				timestamp = ntplib.system_to_ntp_time(timestampS)
				
				if len(e_record) == E_GLOBAL_SAMPLE_BYTES:
				    sample = self._extract_sample(DostaLnWfpSioMuleParserDataParticle,
								  None,
								  e_record,
								  timestamp)
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
	
        form_list = []
        raw_data_len = len(raw_data)
	
	"""
	Ok, there is a new issue with parsing these records.  The Status messages
	can have an optional 2 bytes on the end, and since the rest of the data
	is relatively unformated packed binary records, detecting the presence of
	that optional 2 bytes can be difficult.  The only pattern we have to detect
	is the STATUS_START field ( 4 bytes FF FF FF F[A-F] ).  So, we peel this
	appart be parsing backwards, using the end-of-record as an additional anchor
	point.
	"""
	
	# '-1' to remove the '\x03' end-of-record marker
	parse_end_point = raw_data_len - 1
        while parse_end_point > 0:
	    
	    # look for a status message at postulated message header position
	    
	    header_start = STATUS_BYTES_AUGMENTED
	    
	    # look for an augmented status
            if STATUS_START_MATCHER.match(raw_data[parse_end_point-STATUS_BYTES_AUGMENTED:parse_end_point]):
		# A hit for the status message at the augmented offset
		# NOTE, we don't need the status messages, so we drop them on the floor here
		# and only deliver a stream of samples to build_parse_values
                #form_list.append((parse_end_point-STATUS_BYTES_AUGMENTED, parse_end_point))
		print 'matched stat aug'
                parse_end_point = parse_end_point-STATUS_BYTES_AUGMENTED 
		
            # check if this is a unaugmented status
            elif STATUS_START_MATCHER.match(raw_data[parse_end_point-STATUS_BYTES:parse_end_point]):
		# A hit for the status message at the unaugmented offset
		# NOTE: same as above
                #form_list.append((parse_end_point-STATUS_BYTES, parse_end_point))
		print 'matched stat'
                parse_end_point = parse_end_point-STATUS_BYTES
		
            else:
		# assume if not a stat that hit above, we have a sample.  If we missparse, we
		# will end up with extra bytes when we finish, and sample_except at that point.
                form_list.append((parse_end_point-E_GLOBAL_SAMPLE_BYTES, parse_end_point))
                parse_end_point = parse_end_point-E_GLOBAL_SAMPLE_BYTES
 
	    print parse_end_point
            # if the remaining bytes are less than the data sample bytes, all we might have left is a status sample, if we don't we're done
            if parse_end_point != 0 and parse_end_point < STATUS_BYTES and parse_end_point < E_GLOBAL_SAMPLE_BYTES  and parse_end_point < STATUS_BYTES_AUGMENTED:
		raise SampleException("Error sieving WE data, inferred sample/status alignment incorrect")
	    
	# since we parsed this backwards, we need to reverse to list to deliver the data in the correct order    
	return_list = []    
	return_list = form_list[::-1]    
        log.debug("returning we sieve list %s", return_list)
        return return_list
