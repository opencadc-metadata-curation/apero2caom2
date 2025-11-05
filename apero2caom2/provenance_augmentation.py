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
#  : 4 $
#
# ***********************************************************************
#

import logging

from astropy.io import fits

from caom2pipe.caom_composable import make_plane_uri
from caom2pipe.client_composable import query_tap_client
from caom2pipe.manage_composable import CadcException, make_datetime, search_for_file
from apero2caom2.cfht_name import CFHTName


__all__ = ['APEROFits2caom2Visitor']


class APEROProvenanceVisitor:
    def __init__(self, observation, **kwargs):
        self.observation = observation
        self.clients = kwargs.get('clients')
        self.config = kwargs.get('config')
        self.storage_name = kwargs.get('storage_name')
        self.logger = logging.getLogger(__class__.__name__)

    def visit(self):
        if '.fits' not in self.storage_name.file_name:
            self.logger.debug(f'No provenance metadata in {self.storage_name.file_name}. Returning.')
            return self.observation

        self.logger.debug(f'Begin visit for {self.observation.observation_id}')
        counts = {}
        visit_plane = None
        for plane in self.observation.planes.values():
            if self.storage_name.product_id == plane.product_id:
                for artifact_uri in plane.artifacts.keys():
                    if artifact_uri == self.storage_name.file_uri:
                        visit_plane = plane
                        break

        if visit_plane is None:
            raise CadcException(f'Could not find a plane for {self.storage_name}')
        add_these_groups = []
        for source_name in self.storage_name.source_names:
            if 'fits' in source_name:
                fqn = search_for_file(self.storage_name, self.config.working_directory).replace('.header', '')
                self.logger.debug(f'Begin visit for {fqn}')
                hdus = fits.open(fqn)
                if 'TEMPLATE_TABLE' in hdus:
                    temp = []
                    provenance = hdus['TEMPLATE_TABLE'].data
                    for column in ['Filename', 'DARKFILE']:
                        for f_name in provenance[column]:
                            temp.append(f_name.split('_')[0])
                    f_name_prefixes = list(set(temp))
                    self.logger.info(f'Found {len(f_name_prefixes)} provenance entries in {fqn}.')

                    for f_name_prefix in f_name_prefixes:
                        temp_storage_name = CFHTName(
                            instrument=self.storage_name.instrument, source_names=[f_name_prefix]
                        )
                        obs_member_uri, prov_plane_uri = make_plane_uri(
                            temp_storage_name.obs_id, temp_storage_name.product_id, 'CFHT'
                        )
                        # if obs_member_uri not in self.observation.members:
                        if visit_plane.provenance and prov_plane_uri not in visit_plane.provenance.inputs:
                            qs = f"""
                            SELECT O.proposal_id, P.dataRelease
                            FROM caom2.Observation AS O
                            JOIN caom2.Plane AS P on P.obsID = O.obsID
                            WHERE P.productID = '{temp_storage_name.product_id}'
                            AND O.observationID = '{temp_storage_name.obs_id}'
                            AND O.collection = 'CFHT'
                            """
                            result = query_tap_client(qs, self.clients.query_client)
                            if len(result) > 0:
                                for entry in result:
                                    prov_proposal_id = entry['proposal_id']
                                    prov_data_release = entry['dataRelease']

                                    if visit_plane.provenance and prov_plane_uri not in visit_plane.provenance.inputs:
                                        visit_plane.provenance.inputs.add(prov_plane_uri)
                                        self.logger.debug(f'Adding provenance input {prov_plane_uri}')
                                    prov_data_release_dt = make_datetime(prov_data_release)
                                    if visit_plane.data_release is not None and prov_data_release_dt is not None:
                                        visit_plane.data_release = max(
                                            visit_plane.data_release, prov_data_release_dt
                                        )
                                    else:
                                        visit_plane.data_release = prov_data_release_dt
                                    self.logger.debug(f'Setting release date to {visit_plane.data_release}')
                                    group_name = f'ivo://cadc.nrc.ca/gms?CFHT-{prov_proposal_id}'
                                    add_these_groups.append(group_name)
                                    counts['plane'] = 1
                                    if obs_member_uri not in self.observation.members:
                                        self.observation.members.add(obs_member_uri)
                                    self.logger.debug(f'Adding observation member {obs_member_uri}')
                                    counts['observation'] = 1
                else:
                    self.logger.warning(f'TEMPLATE_TABLE HDU not found in {fqn}')

        for group_name in add_these_groups:
            for plane in self.observation.planes.values():
                # some planes have no metadata to speak of that is not obtained from other planes, so ensure the
                # authorization metadata is consistent.
                self.logger.debug(f'Adding group {group_name} in plane {visit_plane.product_id}')
                plane.data_read_groups.add(group_name)
                plane.meta_read_groups.add(group_name)
                self.observation.meta_read_groups.add(group_name)

        self.logger.debug(f'End visit with counts {counts}')
        return self.observation


def visit(observation, **kwargs):
    return APEROProvenanceVisitor(observation, **kwargs).visit()
