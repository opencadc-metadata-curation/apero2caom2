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
import os

from mock import Mock

from apero2caom2 import file2caom2_augmentation, main_app, provenance_augmentation
from caom2.diff import get_differences
from caom2pipe.manage_composable import ExecutionReporter2, read_obs_from_file, write_obs_to_file
from apero2caom2.main_app import set_storage_name_from_local_preconditions


def test_main_app_no_blueprint(test_config, tmp_path, test_data_dir, change_test_dir):
    # this tests what happens when there is no blueprint or module file to be found
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    test_config.change_working_directory(tmp_path.as_posix())
    test_config.lookup['instrument'] = 'SOMETHING_ELSE'

    expected_fqn = f'{test_data_dir}/something_else.expected.xml'
    actual_fqn = expected_fqn.replace('expected', 'actual')
    if os.path.exists(actual_fqn):
        os.unlink(actual_fqn)
    observation = None
    test_file_name = f'{test_data_dir}/bp_tests/Template_GL699_tellu_obj_AB.fits.header'
    storage_name = main_app.APEROName(instrument='SOMETHING_ELSE', source_names=[test_file_name])
    set_storage_name_from_local_preconditions(storage_name, test_config.working_directory, logger)
    test_reporter = ExecutionReporter2(test_config)
    kwargs = {
        'storage_name': storage_name,
        'reporter': test_reporter,
        'config': test_config,
        'clients': Mock(),
    }
    observation = file2caom2_augmentation.visit(observation, **kwargs)
    observation = provenance_augmentation.visit(observation, **kwargs)
    assert observation is None, 'no supporting files, this is where it stops'


def test_main_app(test_config, tmp_path, test_data_dir, change_test_dir):
    # this tests what happens when there is a blueprint and a module file for
    # an instrument named "DIFFERENT"
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    test_config.change_working_directory(tmp_path.as_posix())
    test_config.lookup['instrument'] = 'DIFFERENT'
    test_config.lookup['blueprint_directory'] = f'{test_data_dir}/blueprints'

    expected_fqn = f'{test_data_dir}/something_else.expected.xml'
    actual_fqn = expected_fqn.replace('expected', 'actual')
    if os.path.exists(actual_fqn):
        os.unlink(actual_fqn)
    observation = None
    test_file_name = f'{test_data_dir}/bp_tests/Template_GL699_tellu_obj_AB.fits.header'
    storage_name = main_app.APEROName(instrument='DIFFERENT', source_names=[test_file_name])
    set_storage_name_from_local_preconditions(storage_name, test_config.working_directory, logger)
    test_reporter = ExecutionReporter2(test_config)
    kwargs = {
        'storage_name': storage_name,
        'reporter': test_reporter,
        'config': test_config,
        'clients': Mock(),
    }
    observation = file2caom2_augmentation.visit(observation, **kwargs)
    observation = provenance_augmentation.visit(observation, **kwargs)

    if observation is None:
        assert False, f'Did not create observation for DIFFERENT'
    else:
        if os.path.exists(expected_fqn):
            expected = read_obs_from_file(expected_fqn)
            compare_result = get_differences(expected, observation)
            if compare_result is not None:
                write_obs_to_file(observation, actual_fqn)
                compare_text = '\n'.join([r for r in compare_result])
                msg = f'Differences found in observation {expected.observation_id} {actual_fqn}\n{compare_text}'
                raise AssertionError(msg)
        else:
            write_obs_to_file(observation, actual_fqn)
            assert False, 'nothing to compare to for SOMETHING_ELSE'
        # assert False  # cause I want to see logging messages
