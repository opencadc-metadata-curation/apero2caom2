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

from mock import ANY, Mock, patch, PropertyMock

from datetime import datetime

from caom2 import SimpleObservation
from caom2pipe import manage_composable as mc
from apero2caom2 import composable


@patch('caom2pipe.client_composable.ClientCollection')
@patch('caom2pipe.execute_composable.OrganizeExecutesRunnerMeta.do_one')
def test_run(do_one_mock, clients_mock, test_config, tmp_path, change_test_dir):
    do_one_mock.return_value = (0, None)
    test_f_id = 'Template_s1dw_GL699_sc1d_w_file_AB'
    test_f_name = f'{test_f_id}.fits'
    test_config.change_working_directory(tmp_path.as_posix())
    test_config.proxy_file_name = 'test_proxy.fqn'
    test_config.task_types = [mc.TaskType.INGEST]
    test_config.write_to_file(test_config)

    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')
    with open(test_config.work_fqn, 'w') as f:
        f.write(test_f_name)

    try:
        # execution
        test_result = composable._run()
    except Exception as e:
        import traceback
        import logging
        logging.error(traceback.format_exc())
        assert False, e

    assert test_result == 0, 'wrong return value'
    assert do_one_mock.called, 'should have been called'
    args, _ = do_one_mock.call_args
    test_storage = args[0]
    assert isinstance(test_storage, mc.StorageName), type(test_storage)
    assert test_storage.file_name == test_f_name, 'wrong file name'
    assert test_storage.source_names[0] == test_f_name, 'wrong fname on disk'


@patch('apero2caom2.provenance_augmentation.visit')
@patch('apero2caom2.data_source.APEROLocalFilesDataSource._initialize_end_dt')
@patch(
    'apero2caom2.data_source.APEROLocalFilesDataSource.end_dt',
    new_callable=PropertyMock(return_value=datetime(year=2025, month=11, day=1, hour=10, minute=5))
)
@patch('apero2caom2.file2caom2_augmentation.visit')
@patch('apero2caom2.data_source.check_fitsverify')
@patch('apero2caom2.composable.ClientCollection')
def test_run_by_file_store_ingest_modify(
    clients_mock,
    fits_verify_mock,
    visit_mock,
    end_time_mock,
    initialize_end_dt_mock,
    provenance_mock,
    test_data_dir,
    test_config,
    tmp_path,
    change_test_dir,
):
    test_file_uri = 'cadc:APERO/SPIRou/Template_s1dw_GL699_sc1d_w_file_AB.fits'
    visit_mock.side_effect = _mock_visit_wd
    fits_verify_mock.return_value = True

    test_config.change_working_directory(tmp_path.as_posix())
    test_config.data_sources = [f'{test_data_dir}/Template_s1dw_GL699_sc1d_w_file_AB']
    test_config.data_source_extensions = ['.fits.header']
    test_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST, mc.TaskType.MODIFY]
    test_config.use_local_files = True
    test_config.logging_level = 'INFO'
    test_config.interval = 1800
    test_config.write_to_file(test_config)
    clients_mock.return_value.metadata_client.read.side_effect = _mock_repo_read
    clients_mock.return_value.metadata_client.create.side_effect = Mock()
    clients_mock.return_value.metadata_client.update.side_effect = _mock_repo_update
    clients_mock.return_value.data_client.info.side_effect = _mock_get_file_info

    test_state_fqn = f'{tmp_path}/state.yml'
    start_time = datetime(year=2025, month=10, day=30, hour=10, minute=5)
    mc.State.write_bookmark(test_state_fqn, test_config.bookmark, start_time)
    mc.Config.write_to_file(test_config)

    provenance_mock.side_effect = (
        lambda x, working_directory, storage_name, log_file_directory, clients, reporter, config: x
    )

    test_result = composable._run_incremental()
    assert test_result == 0, 'wrong result'
    assert clients_mock.return_value.data_client.put.called, 'put call'
    clients_mock.return_value.data_client.put.assert_called_with(
        test_config.data_sources[0], test_file_uri
    ), 'put call args'
    assert clients_mock.return_value.metadata_client.read.called, 'read call'
    clients_mock.return_value.metadata_client.read.assert_called_with('APERO', 'Template_GL699'), 'read call'
    assert clients_mock.return_value.metadata_client.update.called, 'modify call'
    clients_mock.return_value.metadata_client.update.assert_called_with(ANY), 'modify call'


def _mock_repo_read(collection, obs_id):
    return SimpleObservation(collection=collection, observation_id=obs_id)


def _mock_visit_wd(obs, **kwargs):
    return _mock_repo_read('NEOSSat', 'test_obs')


def _mock_repo_update(ignore1):
    return None


def _mock_get_file_info(uri):
    if '_prev' in uri:
        return {'type': 'image/jpeg'}
    else:
        return {'type': 'application/fits'}


def _mock_http_get(url, local_fqn, verify_session):
    pass
