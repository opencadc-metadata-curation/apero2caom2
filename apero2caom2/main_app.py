# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2026.                            (c) 2026.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

"""
This module implements the ObsBlueprint mapping.
"""

from datetime import datetime, timedelta
from os.path import basename
from re import search

from caom2 import ProductType
from caom2pipe.execute_composable import (
    CaomExecuteRunnerMeta,
    OrganizeExecutesRunnerMeta,
    NoFheadScrapeRunnerMeta,
    NoFheadStoreVisitRunnerMeta,
    NoFheadVisitRunnerMeta,
)
from caom2pipe.manage_composable import (
    CadcException, get_keyword, search_for_source_name, StorageName, TaskType
)
from caom2utils.data_util import get_local_file_info, get_local_file_headers
from apero2caom2.cfht_name import CFHTName


__all__ = [
    'APEROName',
]


class APEROName(CFHTName):
    """Naming rules:
    - support mixed-case file name storage, and mixed-case obs id values
    - support CFHT compression rules for files in storage
    """

    APERO_NAME_PATTERN = '*'

    def __init__(self, instrument, source_names):
        self._file_uri = None
        self._object = None
        self._instrument_value = instrument
        self._blueprint_name = None
        try:
            super().__init__(instrument=instrument, source_names=source_names)
        except ValueError as e:
            self._bitpix = None
            self._instrument = instrument
            StorageName.__init__(self, source_names=source_names)

    def _get_uri(self, file_name, scheme):
        return f'{scheme}:{self.collection}/{self._instrument_value}/{basename(file_name)}'

    @property
    def blueprint_name(self):
        # energy in Simple
        # time in RDB, Template
        # position in Template

        obs_cardinality = 'derived'
        if self._product_id.startswith('DRS_POST_'):
            obs_cardinality = 'simple'

        wcs_axes = 'auxiliary'
        if obs_cardinality == 'simple':
            if self._file_name.endswith('.png'):
                wcs_axes = 'no_wcs'
            else:
                if self._suffix == 'p':
                    wcs_axes = 'polarization_spatial_spectral_temporal'
                else:
                    wcs_axes = 'spatial_spectral_temporal'
        elif self._file_name.endswith('.rdb'):
            wcs_axes = 'temporal'
        elif 'Template' in self._file_name or self._product_id == 'LBL_FITS':
            wcs_axes = 'spatial_temporal'
        else:
            wcs_axes = 'no_wcs'
        self._blueprint_name = f'{self._instrument_value.lower()}_{obs_cardinality}_{wcs_axes}.bp'
        return self._blueprint_name

    @property
    def instrument_value(self):
        return self._instrument_value

    @property
    def prev(self):
        """The preview file name for the file."""
        return self._file_name

    @property
    def thumb(self):
        """The thumbnail file name for the file."""
        return f'{self._file_name.replace(".png", "_256.png")}'

    def is_valid(self):
        return True

    def set_file_id(self):
        self._file_id = APEROName.remove_extensions(self._file_name)
        self._suffix = None
        if self.sequence_number is not None:
            # for file names that have _flag or _diag in them
            self._suffix = self._file_id.split('_')[0][-1]

    def set_obs_id(self, **kwargs):
        temp = search('[0-9]{5,7}', self._file_name)
        if temp:
            self._suffix = self._file_name[temp.end()]
        if self._suffix:
            self._logger.error(self._file_name)
            if self._file_name.endswith('.png'):
                self._obs_id = self._file_id.replace('.png', '').replace('_256', '')[:-1]
            else:
                self._obs_id = self._file_id[:-1]
        else:
            super().set_obs_id(**kwargs)


    def set_product_id(self, **kwargs):
        # DRS_POST_<suffix>
        # TELLU_TEMP_S1DW
        # TELLU_TEMP_S1DV
        # TELLU_TEMP
        # LBL_FITS
        # LBL_RDB_FITS
        # LBL_RDB
        # LBL_RDB2
        # LBL_RDB_DRIFT
        # LBL_RDB2_DRIFT
        self._logger.debug(f'Begin set_product_id {self._file_id}')
        if '_lbl' in self._file_id:
            if self._file_name.endswith('.rdb'):
                if '_drift' in self._file_id:
                    self._product_id = 'LBL_RDB_DRIFT'
                else:
                    self._product_id = 'LBL_RDB'
            elif self._file_name.endswith('.fits') or self._file_name.endswith('.png'):
                if '_tcorr_' in self._file_name:
                    self._product_id = 'LBL_FITS'
                else:
                    self._product_id = 'LBL_RDB_FITS'
        elif '_lbl2_' in self._file_id and self._file_name.endswith('.rdb'):
            if '_drift' in self._file_id:
                self._product_id = 'LBL_RDB2_DRIFT'
            else:
                self._product_id = 'LBL_RDB2'
        elif '_Template_' in self._file_id:
            if '_s1dw_' in self._file_id:
                self._product_id = 'TELLU_TEMP_S1DW'
            elif '_s1dv_' in self._file_id:
                self._product_id = 'TELLU_TEMP_S1DV'
            else:
                self._product_id = 'TELLU_TEMP'
        else:
            if self._suffix:
                self._product_id = f'DRS_POST_{self._suffix.upper()}'
            else:
                self._product_id = 'NO_GUIDANCE'
        self._logger.debug(f'End set_product_id {self._product_id} for {self._file_name}')

    @staticmethod
    def remove_extensions(name):
        return CFHTName.remove_extensions(name).replace('.rdb', '').replace('.png', '')


class APERONoFheadVisitRunnerMeta(NoFheadVisitRunnerMeta):
    """Defines a pipeline step for all the operations that require access to the file on disk for metdata and data
    operations.
    """

    def _set_preconditions(self):
        """This is probably not the best approach, but I want to think about where the optimal location for the
        retrieve_file_info and retrieve_headers methods will be long-term. So, for the moment, use them here."""
        self._logger.debug(f'Begin _set_preconditions for {self._storage_name.file_name}')
        set_storage_name_from_local_preconditions(self._storage_name, self._config.working_directory, self._logger)
        self._logger.debug('End _set_preconditions')


class APERONoFheadLocalVisitRunnerMeta(CaomExecuteRunnerMeta):

    def __init__(self, clients, config, data_visitors, meta_visitors, reporter):
        super().__init__(clients, config, meta_visitors, reporter)
        self._data_visitors = data_visitors

    def _set_preconditions(self):
        """This is probably not the best approach, but I want to think about where the optimal location for the
        retrieve_file_info and retrieve_headers methods will be long-term. So, for the moment, use them here."""
        self._logger.debug(f'Begin _set_preconditions for {self._storage_name.file_name}')
        set_storage_name_from_local_preconditions(self._storage_name, self._config.working_directory, self._logger)
        self._logger.debug('End _set_preconditions')

    def execute(self, context):
        self._logger.debug('begin execute with the steps:')
        self.storage_name = context.get('storage_name')

        self._logger.debug('set the preconditions')
        self._set_preconditions()

        self._logger.debug('get the observation for the existing model')
        self._caom2_read()

        self._logger.debug('execute the meta visitors')
        self._visit_meta()

        self._logger.debug('execute the data visitors')
        self._visit_data()

        self._logger.debug('write the observation to disk for debugging')
        self._write_model()

        self._logger.debug('store the updated xml')
        self._caom2_store()

        self._logger.debug('End execute.')


class APERONoFheadScrapeVisitRunnerMeta(NoFheadScrapeRunnerMeta):

    def _set_preconditions(self):
        """This is probably not the best approach, but I want to think about where the optimal location for the
        retrieve_file_info and retrieve_headers methods will be long-term. So, for the moment, use them here."""
        self._logger.debug(f'Begin _set_preconditions for {self._storage_name.file_name}')
        set_storage_name_from_local_preconditions(self._storage_name, self._config.working_directory, self._logger)
        self._logger.debug('End _set_preconditions')


class APERONoFheadStoreVisitRunnerMeta(NoFheadStoreVisitRunnerMeta):

    def __init__(self, clients, config, data_visitors, meta_visitors, reporter, store_transferrer):
        super().__init__(config, clients, store_transferrer, meta_visitors, data_visitors, reporter)

    def _set_preconditions(self):
        """This is probably not the best approach, but I want to think about where the optimal location for the
        retrieve_file_info and retrieve_headers methods will be long-term. So, for the moment, use them here."""
        self._logger.debug(f'Begin _set_preconditions for {self._storage_name.file_name}')
        set_storage_name_from_local_preconditions(self._storage_name, self._config.working_directory, self._logger)
        self._logger.debug('End _set_preconditions')


class APEROOrganizeExecutesRunnerMeta(OrganizeExecutesRunnerMeta):
    """A class that extends OrganizeExecutes to handle the choosing of the correct executors based on the config.yml.
    Attributes:
        _needs_delete (bool): if True, the CAOM repo action is delete/create instead of update.
        _reporter: An instance responsible for reporting the execution status.
    Methods:
        _choose():
            Determines which descendants of CaomExecute to instantiate based on the content of the config.yml
            file for an application.
    """

    def _choose(self):
        """The logic that decides which descendants of CaomExecute to instantiate. This is based on the content of
        the config.yml file for an application.
        """
        super()._choose()
        if self._needs_delete:
            raise CadcException('No need identified for this yet.')

        if TaskType.SCRAPE in self.task_types:
            self._logger.debug(
                f'Over-riding with executor APERONoFheadScrapeVisitRunnerMeta for tasks {self.task_types}.'
            )
            self._executors = []
            self._executors.append(
                APERONoFheadScrapeVisitRunnerMeta(
                    self.config, self._meta_visitors, self._data_visitors, self._reporter
                )
            )
        elif TaskType.STORE in self.task_types:
            if self.config.use_local_files:
                self._logger.debug(
                    f'Over-riding with executor APERONoFheadStoreVisitRunnerMeta for tasks {self.task_types}.'
                )
                self._executors = []
                self._executors.append(
                    APERONoFheadStoreVisitRunnerMeta(
                        self._clients,
                        self.config,
                        self._data_visitors,
                        self._meta_visitors,
                        self._reporter,
                        self._store_transfer,
                    )
                )
            else:
                raise CadcException('Cannot store files without use_local_files set.')
        elif TaskType.MODIFY in self.task_types:
            if self.config.use_local_files:
                self._logger.debug(
                    f'Over-riding with executor APERONoFheadLocalVisitRunnerMeta for tasks {self.task_types}.'
                )
                self._executors = []
                self._executors.append(
                    APERONoFheadLocalVisitRunnerMeta(
                        self._clients,
                        self.config,
                        self._data_visitors,
                        self._meta_visitors,
                        self._reporter,
                    )
                )
            else:
                self._logger.debug(
                    f'Over-riding with executor APERONoFheadVisitRunnerMeta for tasks {self.task_types}.'
                )
                self._executors = []
                self._executors.append(
                    APERONoFheadVisitRunnerMeta(
                        self._clients,
                        self.config,
                        self._data_visitors,
                        self._meta_visitors,
                        self._modify_transfer,
                        self._reporter,
                    )
                )


def set_storage_name_from_local_preconditions(storage_name, working_directory, logger):
    """Retrieve FileInfo and header metadata into memory from files on disk. These files have extension names and
    compression as expected and support by CADC's Storage Inventory system."""
    logger.debug(f'Begin set_storage_name_from_local_preconditions in {working_directory}')

    if len(storage_name.metadata) == 0:
        file_info = {}
        headers = {}
        storage_name._destination_uris = []
        target = None
        for source_name in storage_name.source_names:
            local_fqn = search_for_source_name(storage_name.obs_id, source_name, working_directory)
            file_info[source_name] = get_local_file_info(local_fqn)
            if '.fits' in source_name:
                headers[source_name] = get_local_file_headers(local_fqn)
                target = get_keyword(headers[source_name], 'DRSOBJN')
            else:
                headers[source_name] = []

            uri = (
                f'{storage_name.scheme}:{storage_name.collection}/{storage_name.instrument_value}/'
                f'{basename(source_name).replace('.header', '')}'
            )
            storage_name._file_uri = uri
            storage_name._destination_uris.append(uri)

        for index, source_name in enumerate(storage_name.source_names):
            uri = storage_name.destination_uris[index]
            storage_name.file_info[uri] = file_info.get(source_name)
            storage_name.metadata[uri] = headers.get(source_name)
    logger.debug('End set_storage_name_from_local_preconditions')
