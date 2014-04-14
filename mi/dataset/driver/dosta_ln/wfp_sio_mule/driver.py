"""
@package mi.dataset.driver.dosta_ln.wfp_sio_mule.driver
@file marine-integrations/mi/dataset/driver/dosta_ln/wfp_sio_mule/driver.py
@author Christopher Fortin
@brief Driver for the dosta_ln_wfp_sio_mule
Release notes:

Initial Release
"""

__author__ = 'Christopher Fortin'
__license__ = 'Apache 2.0'

import string

from mi.core.log import get_logger ; log = get_logger()

from mi.dataset.dataset_driver import SimpleDataSetDriver
from mi.dataset.parser.dosta_ln_wfp_sio_mule import DostaLnWfpSioMuleParser, DostaLnWfpSioMuleParserDataParticle

class DostaLnWfpSioMuleDataSetDriver(SimpleDataSetDriver):
    
    @classmethod
    def stream_config(cls):
        return [DostaLnWfpSioMuleParserDataParticle.type()]

    def _build_parser(self, parser_state, infile):
        """
        Build and return the parser
        """
        config = self._parser_config
        config.update({
            'particle_module': 'mi.dataset.parser.dosta_ln_wfp_sio_mule',
            'particle_class': 'DostaLnWfpSioMuleParserDataParticle'
        })
        log.debug("My Config: %s", config)
        self._parser = DostaLnWfpSioMuleParser(
            config,
            parser_state,
            infile,
            self._save_parser_state,
            self._data_callback,
            self._sample_exception_callback 
        )
        return self._parser

    def _build_harvester(self, driver_state):
        """
        Build and return the harvester
        """
        # *** Replace the following with harvester initialization ***
        self._harvester = None     
        return self._harvester
