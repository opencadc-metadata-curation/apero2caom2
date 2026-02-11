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

import logging

from caom2 import Chunk, DerivedObservation, Part, ProductType, SimpleObservation
from caom2utils.caom2blueprint import update_artifact_meta
from caom2utils.wcs_parsers import FitsWcsParser
from caom2pipe.astro_composable import build_ra_dec_as_deg
from caom2pipe.manage_composable import to_int


def _get_algorithm_name(parameter):
    """
    We would like to propose to use the static algorithm name as follow:
    For *e.fits, *p.fits, *t.fits, *s.fits, *v.fits:
        Algorithm name is : apero_postprocess_spirou.py
    For Template*:
        Algorithm name is : apero_mk_template_spirou.py
    For LBL_FITS:
        Algorithm name is : lbl_compute.py
    For LBL_RDB:
        Algorithm name is : lbl_compile.py
    """
    uri = parameter.get('uri')
    if '_Template' in uri:
        result = 'apero_mk_template_spirou.py'
    elif '_lbl' in uri:
        if '.fits' in uri:
            result = 'lbl_compute.py'
        else:
            result = 'lbl_compile.py'
    else:
        result = 'apero_postprocess_spirou.py'
    return result


def _get_dec(parameter):
    _, dec = _get_spatial(parameter)
    return dec


def _get_polarization_function_val(parameter):
    header = parameter.get('header')
    result = header.get('STOKES')
    lookup = {
        'I': 1.0,
        'Q': 2.0,
        'U': 3.0,
        'V': 4.0,
        'W': 5.0,
    }
    if result in lookup.keys():
        result = lookup.get(result)
    return result


def _get_product_id(parameter):
    header = parameter.get('header')
    uri = parameter.get('uri')
    result = header.get('RUNID') if header else None
    if not result:
        result = uri.split()[-1]
    return result


def _get_product_type(parameter):
    uri = parameter.get('uri')
    result = ProductType.SCIENCE
    if '.png' in uri:
        if '256' in uri:
            result = ProductType.THUMBNAIL
        else:
            result = ProductType.PREVIEW
    elif '.rdb' in uri:
        result = ProductType.AUXILIARY
    return result


def _get_ra(parameter):
    ra, _ = _get_spatial(parameter)
    return ra


def _get_spatial(parameter):
    ra = None
    dec = None
    header = parameter.get('header')
    objra = header.get('OBJRA')
    objdec = header.get('OBJDEC')
    if objra and objdec:
        ra, dec = build_ra_dec_as_deg(objra, objdec, frame='fk5')
    else:
        ra = header.get('PP_RA')
        dec = header.get('PP_DEC')
    return ra, dec


def _get_time_function_delta(header):
    result = None
    temp = header.get('FRMTIME')
    if temp is not None:
        result = temp / (24.0 * 3600.0)
    return result


def _get_time_function_val(parameter):
    header = parameter.get('header')
    return header.get('MJDMID')


def _get_time_resolution(parameter):
    header = parameter.get('header')
    return header.get('FRMTIME')


def _update_artifact(artifact, headers, observation, plane):
    logging.debug(f'Begin _udpate_artifact for {artifact.uri}')
    if plane.product_id == 'LBL_RDB_FITS':
        # remove all the parts - there is no metadata useful for CAOM in the headers for this file
        while len(artifact.parts) > 0:
            artifact.parts.popitem(0)
        return
    extname = headers[0].get('EXTNAME')
    if extname is None and len(headers) > 1:
        extname = headers[1].get('EXTNAME')
    if extname is not None:
        _update_artifact_rename_parts(artifact, headers, observation)
    else:
        _update_artifact_remove_cutout_metadata(artifact)
    logging.debug('End _udpate_artifact')


def _update_artifact_remove_cutout_metadata(artifact):
    logging.debug(f'Begin _update_artifact_remove_cutout_metadata {artifact.uri}')
    # no cutout support for these files, so remove the metadata that indicates that there is
    for part in artifact.parts.values():
        for chunk in part.chunks:
            chunk.naxis = None
            chunk.position_axis_1 = None
            chunk.position_axis_2 = None
            chunk.time_axis = None
            chunk.energy_axis = None
            chunk.polarization_axis = None
    logging.debug('End _update_artifact_remove_cutout_metadata')


def _update_artifact_rename_parts(artifact, headers, observation):
    # over-ride default part names with the extension names, and set the ProductTypes accordingly
    logging.debug(f'Begin _update_artifact_rename_parts {artifact.uri}')
    # a Part instance in parts is immutable, so changing the name requires removing old parts, and adding new parts
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
            if isinstance(observation, DerivedObservation):
                new_part.product_type = ProductType.AUXILIARY

        for chunk in part.chunks:
            if new_part.name not in SCIENCE_PARTS and new_part.product_type != ProductType.AUXILIARY:
                new_part.chunks.append(chunk)
            # no cutout support for these files, so remove the metadata that indicates that there is
            chunk.naxis = None
            chunk.position_axis_1 = None
            chunk.position_axis_2 = None
            chunk.time_axis = None
            chunk.energy_axis = None
            chunk.polarization_axis = None
            if (
                chunk.polarization
                and chunk.polarization.axis
                and chunk.polarization.axis.function
                and chunk.polarization.axis.function.ref_coord
                and chunk.polarization.axis.function.ref_coord.val == 0.0
            ):
                # only some HDUs have polarization data
                chunk.polarization = None

        # add the Chunk metadata to the relevant Part
        if new_part.name in SCIENCE_PARTS:
            logging.info(f'Adding Chunk to {new_part.name} Part')
            primary_chunk = Chunk()
            primary_header = headers[0]
            wcs_parser = FitsWcsParser(primary_header, observation.observation_id, 0)
            wcs_parser.augment_temporal(primary_chunk, idx)
            wcs_parser.augment_position(primary_chunk, idx)
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

    logging.debug('End _update_artifact_rename_parts')


def _update_simple_groups(observation, plane, headers):
    logging.debug('Begin _update_simple_groups')
    if isinstance(observation, SimpleObservation):
        run_id = headers[0].get('RUNID') if headers else None
        if run_id:
            group = f'ivo://cadc.nrc.ca/gms?CFHT-{run_id}'
            observation.meta_read_groups.add(group)
            plane.data_read_groups.add(group)
            plane.meta_read_groups.add(group)
    logging.debug('End _update_simple_groups')


def update(observation, **kwargs):
    """Called to fill multiple CAOM model elements and/or attributes (an n:n relationship between TDM attributes
    and CAOM attributes).
    """
    logging.debug(f'Begin update for {observation.observation_id}')
    headers = kwargs.get('headers')
    uri = kwargs.get('uri')
    file_info = kwargs.get('file_info')
    for plane in observation.planes.values():
        for artifact in plane.artifacts.values():
            if uri == artifact.uri:
                update_artifact_meta(artifact, file_info)
                _update_artifact(artifact, headers, observation, plane)
                _update_simple_groups(observation, plane, headers)
    logging.debug('End update')
    return observation
