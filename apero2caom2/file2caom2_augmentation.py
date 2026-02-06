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
#  : 4 $
#
# ***********************************************************************
#

import importlib
import logging
import sys
import traceback

from os.path import basename, dirname, exists

from caom2 import Algorithm, SimpleObservation, DerivedObservation
from caom2utils.blueprints import ObsBlueprint
from caom2utils.parsers import BlueprintParser, Caom2Exception, FitsParser
from caom2utils.caomvalidator import validate
from caom2utils.wcsvalidator import InvalidWCSError


__all__ = ['File2caom2Visitor']


class File2caom2Visitor:
    """
    Create a CAOM2 record, as expected by the execute_composable.MetaVisits class, using configurable blueprint files
    and modules that are loaded at run-time.
    """

    def __init__(self, observation, **kwargs):
        self.observation = observation
        self.storage_name = kwargs.get('storage_name')
        self.clients = kwargs.get('clients')
        self.reporter = kwargs.get('reporter')
        self.config = kwargs.get('config')
        self.logger = logging.getLogger(self.__class__.__name__)
        self.module = None
        self.update_method_module = None

    def _get_blueprints(self, dest_uri):
        result = []
        bp_fqn = f'{self.config.lookup.get("blueprint_directory")}/{self.storage_name.blueprint_name}'
        self.logger.debug(f'Begin _get_blueprints for {dest_uri} from {bp_fqn} file')
        module_fqn = f'{dirname(bp_fqn)}/{self.config.lookup.get("instrument").lower()}.py'
        self.logger.info(f'Load module {module_fqn}')
        self._load_module(module_fqn)
        blueprint = ObsBlueprint(module=self.module)
        if exists(bp_fqn):
            try:
                blueprint.load_from_file(bp_fqn)
            except Exception as e:
                self.logger.error(f'Blueprint load failure: {e}')
                self.logger.debug(traceback.format_exc())
                raise e
            result.append(blueprint)
        else:
            self.logger.warning(
                f'Cannot find blueprint file {bp_fqn}. Check config.yml lookup values for "blueprint_directory" and '
                f'"instrument".'
            )
        self.logger.debug('End _get_blueprints')
        return result

    def _get_parser(self, blueprint, uri):
        self.logger.debug('Begin _get_parser')
        headers = self.storage_name.metadata.get(uri)
        if headers is None or len(headers) == 0 or 'no_wcs' in self.storage_name.blueprint_name:
            parser = BlueprintParser(blueprint, uri)
        else:
            parser = FitsParser(headers, blueprint, uri)
        self.logger.debug(f'Created {parser.__class__.__name__} parser for {uri}.')
        return parser

    def _load_module(self, module):
        """If a user provides code for execution during blueprint configuration, add that code to the execution
        environment of the interpreter here.

        :param module the fully-qualified path name to the source code from a user.
        """
        mname = basename(module)
        if '.' in mname:
            # remove extension from the provided name
            mname = mname.split('.')[0]
        pname = dirname(module)
        sys.path.append(pname)
        try:
            self.module = importlib.import_module(mname)
        except ImportError as e:
            self.logger.warning(f'Could import module {mname!r} in {pname!r}')

    def _load_update_method(self):
        if hasattr(self.module, 'update'):
            self.update_method_module= self.module
        elif hasattr(self.module, 'ObservationUpdater'):
            # for backwards compatibility with caom2repo
            self.update_method_module = getattr(self.module, 'ObservationUpdater')()

    def _loaded_module_visit(self, parser, visit_local):
        result = self.observation
        kwargs = {}
        self._load_update_method()
        if self.update_method_module:
            # TODO make a check that's necessary under both calling conditions here
            self.logger.debug(
                f'Begin plugin execution {self.module.__name__} update method on observation '
                f'{self.observation.observation_id}'
            )
            if isinstance(parser, FitsParser):
                kwargs['headers'] = parser.headers
            kwargs['fqn'] = visit_local
            kwargs['product_id'] = self.storage_name.product_id
            kwargs['uri'] = self.storage_name.file_uri
            kwargs['file_info'] = self.storage_name.file_info.get(self.storage_name.file_uri)
            kwargs['subject'] = self.clients._subject
            try:
                result = self.update_method_module.update(observation=self.observation, **kwargs)
                if result is not None:
                    self.logger.debug(
                        f'Finished executing plugin {self.module.__name__} update '
                        f'method on observation {self.observation.observation_id}'
                    )
            except Exception as e:
                self.logger.error(e)
                tb = traceback.format_exc()
                self.logger.debug(tb)
                raise e
        return result

    def visit(self):
        self.logger.debug('Begin visit')
        try:
            for index, uri in enumerate(self.storage_name.destination_uris):
                self.logger.debug(f'Build observation for {uri}')
                blueprints = self._get_blueprints(uri)
                for blueprint in blueprints:
                    parser = self._get_parser(blueprint, uri)

                    if self.observation is None:
                        if blueprint._get('DerivedObservation.members') is None:
                            self.logger.debug('Build a SimpleObservation')
                            self.observation = SimpleObservation(
                                collection=self.storage_name.collection,
                                observation_id=self.storage_name.obs_id,
                                algorithm=Algorithm('exposure'),
                            )
                        else:
                            self.logger.debug('Build a DerivedObservation')
                            algorithm_name = (
                                'composite'
                                if blueprint._get('Observation.algorithm.name') == 'exposure'
                                else parser._get_from_list('Observation.algorithm.name', 0)
                            )
                            self.observation = DerivedObservation(
                                collection=self.storage_name.collection,
                                observation_id=self.storage_name.obs_id,
                                algorithm=Algorithm(algorithm_name),
                            )
                    parser.augment_observation(
                        observation=self.observation,
                        artifact_uri=uri,
                        product_id=self.storage_name.product_id,
                    )
                    for group_name in self.config.data_read_groups:
                        for plane in self.observation.planes.values():
                            plane.data_read_groups.add(group_name)
                    for group_name in self.config.meta_read_groups:
                        for plane in self.observation.planes.values():
                            plane.meta_read_groups.add(group_name)
                        self.observation.meta_read_groups.add(group_name)

                    result = self._loaded_module_visit(parser, self.storage_name.source_names[index])
                    if result:
                        self.observation = result
                    try:
                        validate(self.observation)
                    except InvalidWCSError as e:
                        self.logger.error(e)
                        tb = traceback.format_exc()
                        self.logger.debug(tb)
                        raise e

        except Exception as e:
            self.logger.debug(traceback.format_exc())
            self.logger.warning(
                f'CAOM2 record creation failed for {self.storage_name.obs_id}'
                f':{self.storage_name.file_name} with {e}'
            )
            self.observation = None

        self.logger.debug('End visit')
        return self.observation


def visit(observation, **kwargs):
    return File2caom2Visitor(observation, **kwargs).visit()
