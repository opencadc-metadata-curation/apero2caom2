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

import logging

from caom2 import Chunk, Part, ProductType
from caom2utils.caom2blueprint import update_artifact_meta
from caom2utils.wcs_parsers import FitsWcsParser
from caom2pipe.manage_composable import to_int


def _get_time_function_delta(header):
    result = None
    temp = header.get('FRMTIME')
    if temp is not None:
        result = temp / (24.0 * 3600.0)
    return result


def update(observation, **kwargs):
    """Called to fill multiple CAOM model elements and/or attributes (an n:n relationship between TDM attributes
    and CAOM attributes).
    """
    logging.debug(f'Begin update for {observation.observation_id}')
    product_id = kwargs.get('product_id')
    headers = kwargs.get('headers')
    uri = kwargs.get('uri')
    file_info = kwargs.get('file_info')
    _set_authorization_plane_metadata(observation, product_id)
    for plane in observation.planes.values():
        for artifact in plane.artifacts.values():
            if uri == artifact.uri:
                update_artifact_meta(artifact, file_info)
                _update_artifact(artifact, headers, observation.observation_id)
    logging.debug('End update')
    return observation


def _set_authorization_plane_metadata(observation, product_id):
    # The Plane-level metadata for the ccf, debug and lbl planes has to come from a different plane, as there's no
    # metadata to scrape from the png or rdb files. Pick the any plane with fits files for the data source.
    plane_bits = product_id.split('_')
    ccf_product_id = f'ccf_{plane_bits[1]}'
    debug_product_id = f'debug_{plane_bits[1]}'
    lbl_product_id = f'lbl_{plane_bits[1]}'
    spectrum_product_id = f'spectrum_{plane_bits[1]}'
    telluric_product_id = f'telluric_{plane_bits[1]}'
    logging.debug(
        f'Begin _set_authorization_plane_metadata with keys {ccf_product_id}, {debug_product_id}, '
        f'{lbl_product_id}, {spectrum_product_id} and {telluric_product_id}'
    )
    if (
        spectrum_product_id in observation.planes.keys()
        or telluric_product_id in observation.planes.keys()
    ):
        source_plane = observation.planes.get(spectrum_product_id)
        if source_plane is None:
            source_plane = observation.planes.get(telluric_product_id)
        if source_plane.meta_release:
            observation.meta_release = source_plane.meta_release
        for destination_product_id in [ccf_product_id, debug_product_id, lbl_product_id]:
            if destination_product_id in observation.planes.keys():
                logging.debug(f'Copying plane authorization metadata for {destination_product_id}')
                destination_plane = observation.planes.get(destination_product_id)

                # copy over information that supports authorization
                destination_plane.data_read_groups = source_plane.data_read_groups
                destination_plane.meta_read_groups = source_plane.meta_read_groups
                if source_plane.meta_release:
                    destination_plane.meta_release = source_plane.meta_release
                if source_plane.data_release:
                    destination_plane.data_release = source_plane.data_release
    logging.debug('End _set_authorization_plane_metadata')


def _update_artifact(artifact, headers, obs_id):
    # over-ride default part names with the extension names, and set the ProductTypes accordingly
    logging.debug(f'Begin _update_artifact {artifact.uri}')
    # a Part instance in parts is immutable
    SCIENCE_PARTS = ['TELLU_TEMP_S1DW', 'TELLU_TEMP_S1DV', 'TELLU_TEMP']
    new_parts = []
    old_parts = []
    for part in artifact.parts.values():
        try:
            idx = to_int(part.name)
        except (ValueError, TypeError):
            # if this happens, all the fixes meant to occur in this loop have already been done
            continue
        header = headers[idx]
        extname = header.get('EXTNAME', '0')
        new_part = Part(extname)
        new_parts.append(new_part)
        old_parts.append(part)
        if extname in SCIENCE_PARTS:
            new_part.product_type = ProductType.SCIENCE
        else:
            new_part.product_type = ProductType.AUXILIARY

        # add the Chunk metadata to the relevant Part
        if new_part.name in SCIENCE_PARTS:
            logging.debug(f'Adding Chunk to {new_part.name} Part')
            primary_chunk = Chunk()
            primary_header = headers[0]
            wcs_parser = FitsWcsParser(primary_header, obs_id, 0)
            wcs_parser.augment_temporal(primary_chunk)
            wcs_parser.augment_position(primary_chunk)
            primary_chunk.position_axis_1 = None
            primary_chunk.position_axis_2 = None
            primary_chunk.time_axis = None
            if primary_chunk.time and primary_chunk.time.axis and primary_chunk.time.axis.function:
                primary_chunk.time.axis.function.delta = _get_time_function_delta(headers[0])
            new_part.chunks.append(primary_chunk)

    for part in old_parts:
        artifact.parts.pop(part.name)

    for part in new_parts:
        artifact.parts.add(part)

    logging.debug('End _update_artifact')
