# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2025.                            (c) 2025.
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

from caom2 import Chunk, Part, ProductType
from caom2pipe.astro_composable import get_geocentric_location
from caom2pipe.caom_composable import TelescopeMapping2
from caom2pipe.execute_composable import (
    CaomExecuteRunnerMeta,
    OrganizeExecutesRunnerMeta,
    NoFheadScrapeRunnerMeta,
    NoFheadStoreVisitRunnerMeta,
    NoFheadVisitRunnerMeta,
)
from caom2pipe.manage_composable import (
    CadcException, get_keyword, search_for_source_name, StorageName, TaskType, to_int
)
from caom2utils.data_util import get_local_file_info, get_local_file_headers
from caom2utils.wcs_parsers import FitsWcsParser
from apero2caom2.cfht_name import CFHTName


__all__ = [
    # 'APEROMapping',
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
        try:
            super().__init__(instrument=instrument, source_names=source_names)
        except ValueError as e:
            self._bitpix = None
            self._instrument = instrument
            StorageName.__init__(self, source_names=source_names)

    def _get_uri(self, file_name, scheme):
        return f'{scheme}:{self.collection}/{self._instrument_value}/{basename(file_name).replace('.header', '')}'

    @property
    def instrument_value(self):
        return self._instrument_value

    @property
    def prev(self):
        """The preview file name for the file."""
        return self._file_name

    @property
    def product_type(self):
        result = ProductType.SCIENCE
        if '.png' in self._file_name:
            result = ProductType.PREVIEW
        elif '.rdb' in self._file_name:
            result = ProductType.AUXILIARY
        return result

    @property
    def thumb(self):
        """The thumbnail file name for the file."""
        return f'{self._file_name.replace(".png", "_256.png")}'

    def is_valid(self):
        return True

    def set_obs_id(self, **kwargs):
        iv_lower = self._instrument_value.lower()
        bits = self.file_name.split('_')
        if self._file_id.startswith('debug_'):
            self._object = bits[bits.index(iv_lower) - 1] if iv_lower in bits else 'target'
        elif self._file_id.startswith('ccf_'):
            self._object = bits[bits.index(iv_lower) - 1] if iv_lower in bits else 'target'
        elif 'tellu_' in self._file_id:
            self._object = bits[bits.index('tellu') - 1] if 'tellu' in bits else 'target'
        elif self._file_id.startswith('lbl'):
            self._object = bits[bits.index(iv_lower) - 1] if iv_lower in bits else bits[1]
        elif '_s1dw_' in self._file_id or '_s1dv_' in self._file_id:
            self._object = bits[bits.index('s1dw') + 1] if 's1dw' in bits else 'target'
            if self._object == 'target':
                self._object = bits[bits.index('s1dv') + 1] if 's1dv' in bits else 'target'
        elif self._file_id.startswith('spec_'):
            self._object = bits[bits.index(iv_lower) - 1] if iv_lower in bits else bits[1]
        else:
            raise CadcException(f'Could not set observation ID for {self.file_name}')
        self._obs_id = f'Template_{self._object}'

    def set_product_id(self, **kwargs):
        if self._file_id.startswith('debug_'):
            self._product_id = f'debug_{self._object}'
        elif self._file_id.startswith('ccf_'):
            self._product_id = f'ccf_{self._object}'
        elif 'tellu_' in self._file_id:
            self._product_id = f'telluric_{self._object}'
        elif self._file_id.startswith('lbl'):
            self._product_id = f'lbl_{self._object}'
        elif '_s1dw_' in self._file_id or '_s1dv_' in self._file_id:
            self._product_id = f'spectrum_{self._object}'
        elif self._file_id.startswith('spec_'):
            self._product_id = f'spectrum_{self._object}'
        else:
            raise CadcException(f'Could not set observation ID for {self.file_name}')

    @staticmethod
    def remove_extensions(name):
        return CFHTName.remove_extensions(name).replace('.rdb', '').replace('.png', '')


# class APEROPostageStampMapping(TelescopeMapping2):

#     def accumulate_blueprint(self, bp):
#         """Configure the telescope-specific ObsBlueprint at the CAOM model Observation level."""
#         self._logger.debug('Begin accumulate_bp.')
#         super().accumulate_blueprint(bp)

#         bp.set('DerivedObservation.members', {})
#         bp.set('Observation.algorithm.name', 'LineByLine')
#         if self._storage_name.instrument_value.lower() == 'spirou':
#             bp.set('Observation.telescope.name', 'CFHT 3.6m')
#             x, y, z = get_geocentric_location('cfht')
#             bp.set('Observation.telescope.geoLocationX', x)
#             bp.set('Observation.telescope.geoLocationY', y)
#             bp.set('Observation.telescope.geoLocationZ', z)

#         # No proposal information for DerivedObservations

#         bp.set('Plane.calibrationLevel', 2)
#         bp.set('Plane.dataProductType', 'spectrum')
#         if '.png' in self._storage_name.file_name:
#             bp.set('Artifact.productType', 'preview')
#         elif '.rdb' in self._storage_name.file_name:
#             bp.set('Plane.dataProductType', 'timeseries')
#             bp.set('Artifact.productType', 'auxiliary')
#         bp.set('Artifact.releaseType', 'data')

#         self._logger.debug('Done accumulate_bp.')

#     def _init_read_groups(self, config):
#         super()._init_read_groups(config)
#         if self._observation:
#             for group in self._observation.meta_read_groups:
#                 self._meta_read_groups.add(group)
#                 self._data_read_groups.add(group)

#     def _update_artifact(self, artifact):
#         pass

#     def update(self):
#         """Called to fill multiple CAOM model elements and/or attributes (an n:n relationship between TDM attributes
#         and CAOM attributes).
#         """
#         self._observation = super().update()
#         self._set_authorization_plane_metadata()
#         return self._observation

#     def _set_authorization_plane_metadata(self):
#         # The Plane-level metadata for the ccf, debug and lbl planes has to come from a different plane, as there's no
#         # metadata to scrape from the png or rdb files. Pick the any plane with fits files for the data source.
#         plane_bits = self._storage_name.product_id.split('_')
#         ccf_product_id = f'ccf_{plane_bits[1]}'
#         debug_product_id = f'debug_{plane_bits[1]}'
#         lbl_product_id = f'lbl_{plane_bits[1]}'
#         spectrum_product_id = f'spectrum_{plane_bits[1]}'
#         telluric_product_id = f'telluric_{plane_bits[1]}'
#         self._logger.debug(
#             f'Begin _set_authorization_plane_metadata with keys {ccf_product_id}, {debug_product_id}, '
#             f'{lbl_product_id}, {spectrum_product_id} and {telluric_product_id}'
#         )
#         if (
#             spectrum_product_id in self._observation.planes.keys()
#             or telluric_product_id in self._observation.planes.keys()
#         ):
#             source_plane = self._observation.planes.get(spectrum_product_id)
#             if source_plane is None:
#                 source_plane = self._observation.planes.get(telluric_product_id)
#             if source_plane.meta_release:
#                 self._observation.meta_release = source_plane.meta_release
#             for destination_product_id in [ccf_product_id, debug_product_id, lbl_product_id]:
#                 if destination_product_id in self._observation.planes.keys():
#                     self._logger.debug(f'Copying plane authorization metadata for {destination_product_id}')
#                     destination_plane = self._observation.planes.get(destination_product_id)

#                     # copy over information that supports authorization
#                     destination_plane.data_read_groups = source_plane.data_read_groups
#                     destination_plane.meta_read_groups = source_plane.meta_read_groups
#                     if source_plane.meta_release:
#                         destination_plane.meta_release = source_plane.meta_release
#                     if source_plane.data_release:
#                         destination_plane.data_release = source_plane.data_release
#         self._logger.debug('End _set_authorization_plane_metadata')


# class APEROMapping(APEROPostageStampMapping):

#     def accumulate_blueprint(self, bp):
#         """Configure the telescope-specific ObsBlueprint at the CAOM model Observation level."""
#         self._logger.debug('Begin accumulate_bp.')
#         super().accumulate_blueprint(bp)

#         bp.set('DerivedObservation.members', {})
#         bp.set('Observation.algorithm.name', 'LineByLine')

#         # No proposal information for DerivedObservations

#         bp.add_attribute('Observation.target.name', 'DRSOBJN')
#         bp.add_attribute('Observation.target_position.point.cval1', 'PP_RA')
#         bp.add_attribute('Observation.target_position.point.cval2', 'PP_DEC')
#         bp.add_attribute('Observation.target_position.coordsys', 'RADECSYS')

#         bp.set('Plane.calibrationLevel', 2)
#         bp.set('Plane.dataProductType', 'spectrum')
#         # bp.add_attribute('Plane.dataRelease', 'DRSVDATE')
#         bp.add_attribute('Plane.metrics.magLimit', 'OBJMAG')
#         # approximately two years in the future, to cover the cases where provenance metadata is not scrapable
#         default_release_date = (datetime.now() + timedelta(weeks=104)).isoformat()
#         bp.set_default('Plane.dataRelease', default_release_date)
#         bp.set_default('Plane.metaRelease', default_release_date)
#         bp.set('Plane.provenance.name', 'APERO')
#         bp.add_attribute('Plane.provenance.lastExecuted', 'DRSPDATE')
#         bp.set('Plane.provenance.reference', 'https://apero.exoplanets.ca/')
#         bp.add_attribute('Plane.provenance.runID', 'DRSPID')
#         bp.add_attribute('Plane.provenance.version', 'VERSION')
#         bp.set('Artifact.productType', 'science')
#         bp.set('Artifact.releaseType', 'data')

#         bp.configure_position_axes((1, 2))
#         bp.set('Chunk.position.axis.axis1.ctype', 'RA---TAN')
#         bp.set('Chunk.position.axis.axis2.ctype', 'DEC--TAN')
#         bp.set('Chunk.position.axis.function.dimension.naxis1', 1)
#         bp.set('Chunk.position.axis.function.dimension.naxis2', 1)
#         bp.set('Chunk.position.axis.function.refCoord.coord1.pix', 1.0)
#         bp.set('Chunk.position.axis.function.refCoord.coord2.pix', 1.0)
#         bp.set('Chunk.position.axis.function.refCoord.coord1.val', '_get_ra_deg_from_0th_header()')
#         bp.set('Chunk.position.axis.function.refCoord.coord2.val', '_get_dec_deg_from_0th_header()')
#         bp.set('Chunk.position.axis.function.cd11', -0.00035833)
#         bp.set('Chunk.position.axis.function.cd12', 0.0)
#         bp.set('Chunk.position.axis.function.cd21', 0.0)
#         bp.set('Chunk.position.axis.function.cd22', 0.00035833)
#         bp.set('Chunk.position.coordsys', '_get_position_coordsys_from_0th_header()')
#         bp.set('Chunk.position.equinox', '_get_position_equinox_from_0th_header()')

#         bp.configure_time_axis(3)
#         bp.set('Chunk.time.resolution', '_get_time_resolution()')
#         bp.add_attribute('Chunk.time.exposure', 'EXPTIME')
#         bp.set('Chunk.time.timesys', 'UTC')
#         bp.set('Chunk.time.axis.axis.ctype', 'TIME')
#         bp.set('Chunk.time.axis.axis.cunit', 'd')
#         bp.set('Chunk.time.axis.error.syser', '1e-07')
#         bp.set('Chunk.time.axis.error.rnder', '1e-07')
#         bp.set('Chunk.time.axis.function.naxis', '1')
#         bp.set('Chunk.time.axis.function.delta', '_get_time_function_delta()')
#         bp.set('Chunk.time.axis.function.refCoord.pix', '0.5')
#         # bp.set('Chunk.time.axis.function.refCoord.val', '_get_time_function_val()')
#         bp.add_attribute('Chunk.time.axis.function.refCoord.val', 'MJDMID')

#         # bp.configure_energy_axis(4)
#         # bp.configure_polarization_axis(5)
#         # bp.configure_observable_axis(6)
#         self._logger.debug('Done accumulate_bp.')

#     def _get_dec_deg_from_0th_header(self, ext):
#         return self._storage_name.metadata.get(self._storage_name.file_uri)[0].get('PP_DEC')

#     def _get_position_coordsys_from_0th_header(self, ext):
#         return self._headers[ext].get('RADECSYS')

#     def _get_position_equinox_from_0th_header(self, ext):
#         return self._headers[ext].get('EQUINOX')

#     def _get_time_function_delta(self, ext):
#         result = None
#         temp = self._headers[ext].get('FRMTIME')
#         if temp is not None:
#             result = temp / (24.0 * 3600.0)
#         return result

#     def _get_ra_deg_from_0th_header(self, ext):
#         return self._storage_name.metadata.get(self._storage_name.file_uri)[0].get('PP_RA')

#     def _get_time_resolution(self, ext):
#         result = self._get_time_function_delta(ext)
#         if result is not None:
#             result = result * (24.0 * 3600.0)
#         return result

#     def _update_artifact(self, artifact):
#         self._logger.debug(f'Begin _update_artifact {artifact.uri}')
#         # a Part instance in parts is immutable
#         SCIENCE_PARTS = ['TELLU_TEMP_S1DW', 'TELLU_TEMP_S1DV', 'TELLU_TEMP']
#         new_parts = []
#         old_parts = []
#         for part in artifact.parts.values():
#             # rename the part names to the extension names
#             try:
#                 idx = to_int(part.name)
#             except (ValueError, TypeError):
#                 # if this happens, all the fixes meant to occur in this loop have already been done
#                 continue
#             header = self._storage_name.metadata.get(self._storage_name.file_uri)[idx]
#             extname = header.get('EXTNAME', '0')
#             new_part = Part(extname)
#             new_parts.append(new_part)
#             old_parts.append(part)
#             if extname in SCIENCE_PARTS:
#                 new_part.product_type = ProductType.SCIENCE
#             else:
#                 new_part.product_type = ProductType.AUXILIARY

#             # add the Chunk metadata to the relevant Part
#             if new_part.name in SCIENCE_PARTS:
#                 self._logger.debug(f'Adding Chunk to {new_part.name} Part')
#                 primary_chunk = Chunk()
#                 primary_header = self._storage_name.metadata.get(self._storage_name.file_uri)[0]
#                 wcs_parser = FitsWcsParser(primary_header, self._storage_name.obs_id, 0)
#                 wcs_parser.augment_temporal(primary_chunk)
#                 wcs_parser.augment_position(primary_chunk)
#                 primary_chunk.position_axis_1 = None
#                 primary_chunk.position_axis_2 = None
#                 primary_chunk.time_axis = None
#                 new_part.chunks.append(primary_chunk)

#         for part in old_parts:
#             artifact.parts.pop(part.name)

#         for part in new_parts:
#             artifact.parts.add(part)

#         self._logger.debug('End _update_artifact')


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

        # now reset the storage name instance to use the destination URIs, because common code expects URIs
        if target:
            if 'Template' in storage_name.file_name:
                storage_name._obs_id = f'Template_{target}'
            else:
                storage_name._obs_id = target
        for index, source_name in enumerate(storage_name.source_names):
            uri = storage_name.destination_uris[index]
            storage_name.file_info[uri] = file_info.get(source_name)
            storage_name.metadata[uri] = headers.get(source_name)
    logger.debug('End set_storage_name_from_local_preconditions')
