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

from os.path import basename

from caom2pipe.caom_composable import TelescopeMapping2
from caom2pipe.manage_composable import get_keyword
from apero2caom2.cfht_name import CFHTName


__all__ = [
    'APEROMapping',
    'APEROName',
]


class APEROName(CFHTName):
    """Naming rules:
    - support mixed-case file name storage, and mixed-case obs id values
    - support CFHT compression rules for files in storage
    """

    APERO_NAME_PATTERN = '*'

    def is_valid(self):
        return True

    # @property
    # def file_uri(self):
    #     if self._metadata:
    #         return ''


class APEROMapping(TelescopeMapping2):
    group_entries = [
        'ivo://cadc.nrc.ca/gms?APERO-RW', 'ivo://cadc.nrc.ca/gms?APERO-RO', 'ivo://cadc.nrc.ca/gms?CADC'
    ]

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model Observation level."""
        self._logger.debug('Begin accumulate_bp.')
        super().accumulate_blueprint(bp)

        bp.set('Observation.algorithm.name', 'LineByLine')

        # No proposal information for DerivedObservations

        bp.add_attribute('Observation.target.name', 'DRSOBJN')
        bp.add_attribute('Observation.target_position.point.cval1', 'PP_RA')
        bp.add_attribute('Observation.target_position.point.cval2', 'PP_DEC')
        bp.add_attribute('Observation.target_position.coordsys', 'RADECSYS')

        bp.set('Plane.calibrationLevel', 2)
        bp.set('Plane.dataProductType', 'spectrum')
        # bp.add_attribute('Plane.dataRelease', 'DRSVDATE')
        bp.add_attribute('Plane.metrics.magLimit', 'OBJMAG')
        bp.set('Plane.dataRelease', '2027-01-01T00:00:00')
        bp.set('Plane.provenance.name', 'APERO')
        bp.add_attribute('Plane.provenance.lastExecuted', 'DRSPDATE')
        bp.set('Plane.provenance.reference', 'https://apero.exoplanets.ca/')
        bp.add_attribute('Plane.provenance.runID', 'DRSPID')
        bp.add_attribute('Plane.provenance.version', 'VERSION')
        bp.set('Artifact.productType', 'science')
        bp.set('Artifact.releaseType', 'data')

        bp.configure_position_axes((1, 2))
        bp.set('Chunk.position.axis.axis1.ctype', 'RA---TAN')
        bp.set('Chunk.position.axis.axis2.ctype', 'DEC--TAN')
        bp.set('Chunk.position.axis.function.dimension.naxis1', 1)
        bp.set('Chunk.position.axis.function.dimension.naxis2', 1)
        bp.set('Chunk.position.axis.function.refCoord.coord1.pix', 1.0)
        bp.set('Chunk.position.axis.function.refCoord.coord2.pix', 1.0)
        bp.set('Chunk.position.axis.function.refCoord.coord1.val', '_get_ra_deg_from_0th_header()')
        bp.set('Chunk.position.axis.function.refCoord.coord2.val', '_get_dec_deg_from_0th_header()')
        bp.set('Chunk.position.axis.function.cd11', -0.00035833)
        bp.set('Chunk.position.axis.function.cd12', 0.0)
        bp.set('Chunk.position.axis.function.cd21', 0.0)
        bp.set('Chunk.position.axis.function.cd22', 0.00035833)
        bp.set('Chunk.position.coordsys', '_get_position_coordsys_from_0th_header()')
        bp.set('Chunk.position.equinox', '_get_position_equinox_from_0th_header()')

        bp.configure_time_axis(3)
        bp.set('Chunk.time.resolution', '_get_time_resolution()')
        bp.add_attribute('Chunk.time.exposure', 'EXPTIME')
        bp.set('Chunk.time.timesys', 'UTC')
        bp.set('Chunk.time.axis.axis.ctype', 'TIME')
        bp.set('Chunk.time.axis.axis.cunit', 'd')
        bp.set('Chunk.time.axis.error.syser', '1e-07')
        bp.set('Chunk.time.axis.error.rnder', '1e-07')
        bp.set('Chunk.time.axis.function.naxis', '1')
        bp.set('Chunk.time.axis.function.delta', '_get_time_function_delta()')
        bp.set('Chunk.time.axis.function.refCoord.pix', '0.5')
        # bp.set('Chunk.time.axis.function.refCoord.val', '_get_time_function_val()')
        bp.add_attribute('Chunk.time.axis.function.refCoord.val', 'MJDMID')

        # bp.configure_energy_axis(4)
        # bp.configure_polarization_axis(5)
        # bp.configure_observable_axis(6)
        bp.set('DerivedObservation.members', {})
        self._logger.debug('Done accumulate_bp.')

    def _get_dec_deg_from_0th_header(self, ext):
        return self._storage_name.metadata.get(self._storage_name.file_uri)[0].get('PP_DEC')

    def _get_position_coordsys_from_0th_header(self, ext):
        return self._headers[ext].get('RADECSYS')

    def _get_position_equinox_from_0th_header(self, ext):
        return self._headers[ext].get('EQUINOX')

    def _get_time_function_delta(self, ext):
        result = None
        temp = self._headers[ext].get('FRMTIME')
        if temp is not None:
            result = temp / (24.0 * 3600.0)
        return result

    # def _get_time_function_val(self, ext):

    def _get_ra_deg_from_0th_header(self, ext):
        return self._storage_name.metadata.get(self._storage_name.file_uri)[0].get('PP_RA')

    def _get_time_resolution(self, ext):
        result = self._get_time_function_delta(ext)
        if result is not None:
            result = result * (24.0 * 3600.0)
        return result

    def update(self):
        """Called to fill multiple CAOM model elements and/or attributes (an n:n relationship between TDM attributes
        and CAOM attributes).
        """
        self._set_read_groups()
        self._observation = super().update()
        for plane in self._observation.planes.values():
            self._logger.error(plane.product_id)
            for artifact in plane.artifacts.values():
                self._logger.error(artifact.uri)
                if artifact.uri != self._storage_name.file_uri:
                    self._logger.error(f'looking for {self._storage_name.file_uri}, found {artifact.uri}')
                    continue
                self._logger.error('stupid repetitive stuff because CFHT was the model')
                from caom2 import Chunk
                from caom2utils.wcs_parsers import FitsWcsParser
                primary_chunk = Chunk()
                primary_header = self._storage_name.metadata.get(self._storage_name.file_uri)[0]
                wcs_parser = FitsWcsParser(primary_header, self._storage_name.obs_id, 0)
                wcs_parser.augment_temporal(primary_chunk)
                wcs_parser.augment_position(primary_chunk)
                # self._logger.error(primary_chunk)
                for part in artifact.parts.values():
                    self._logger.error(part.name)
                    if part.name == '0':
                        # self._logger.error(primary_chunk)
                        self._logger.error('adding chunk')
                        part.chunks.append(primary_chunk)
        # self._logger.error(self._observation)
        return self._observation

    def _update_artifact(self, artifact):
        pass

    def _set_read_groups(self):
        self._logger.debug('Begin _set_read_groups')
        program_id = get_keyword(self._storage_name.metadata.get(self._storage_name.file_uri), 'RUNID')
        entries = APEROMapping.group_entries
        if program_id:
            current_entry = f'ivo://cadc.nrc.ca/gms?CFHT-{program_id}'
            entries = APEROMapping.group_entries + [current_entry]
            self._logger.debug(f'Found {current_entry} for meta and data read groups.')

        for group_list in [self._data_read_groups, self._meta_read_groups]:
            for entry in entries:
                group_list.add(entry)
        self._logger.debug('End _set_read_groups')
