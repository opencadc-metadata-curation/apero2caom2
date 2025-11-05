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

from apero2caom2 import APEROName


def test_is_valid(test_config):
    assert APEROName(test_config.lookup.get('instrument'), ['ccf_plot_GL699_spirou_offline_udem.png']).is_valid()


def test_storage_name(test_config):
    # don't test obs_id, product_id, or destination uris, because those are set n the
    # set_storage_name_from_local_preconditions method in main_app.py
    test_f_name = 'Template_s1dw_GL699_sc1d_w_file_AB.fits'
    test_uri = f'{test_config.scheme}:{test_config.collection}/{test_config.lookup.get('instrument')}/{test_f_name}'
    for index, entry in enumerate(
        [
            test_f_name,
            test_uri,
            f'https://localhost:8020/{test_f_name}',
            f'vos:goliaths/test/{test_f_name}',
            f'/tmp/{test_f_name}',
        ]
    ):
        test_subject = APEROName(test_config.lookup.get('instrument'), [entry])
        assert test_subject.file_id == test_f_name.replace('.fits', '').replace('.header', ''), f'wrong file id {index}'
        assert test_subject.file_uri == test_uri, f'wrong uri {index}'
        assert test_subject.source_names == [entry], f'wrong source names {index}'


def test_product_id(test_config):
    expected_obs_id = 'GL699'
    test_f_name = 'Template_s1dw_GL699_sc1d_w_file_AB.fits'
    test_uri = f'{test_config.scheme}:{test_config.collection}/{test_f_name}'
    for index, entry in enumerate(
        [
            "ccf_plot_GL699_spirou_offline_udem.png",
            "ccf_plot_GL699_spirou_offline_udem_256.png",
            "debug_effron_plot_GL699_spirou_offline_udem_256.png",
            "debug_version_plot_GL699_spirou_offline_udem_256.png",
            "debug_extsmax_plot_GL699_spirou_offline_udem.png",
            "debug_extsmax_plot_GL699_spirou_offline_udem_256.png",
            "debug_wcav000_plot_GL699_spirou_offline_udem.png",
            "debug_wcav000_plot_GL699_spirou_offline_udem_256.png ",
            "debug_shape_plot_GL699_spirou_offline_udem.png",
            "debug_wfpdrift_plot_GL699_spirou_offline_udem.png",
            "debug_shape_plot_GL699_spirou_offline_udem_256.png",
            "debug_wfpdrift_plot_GL699_spirou_offline_udem_256.png",
            "debug_effron_plot_GL699_spirou_offline_udem.png",
            "debug_version_plot_GL699_spirou_offline_udem.png",
            "lbl_GL699_GL699.rdb",
            "lbl_GL699_GL699_drift.rdb",
            "lbl_plot_GL699_GL699_spirou_offline_udem.png",
            "lbl_plot_GL699_GL699_spirou_offline_udem_256.png",
            "lbl2_GL699_GL699.rdb",
            "lbl2_GL699_GL699_drift.rdb ",
            "spec_plot_GL699_spirou_offline_udem_256.png",
            "spec_plot_GL699_spirou_offline_udem.png",
            "Template_s1dw_GL699_sc1d_w_file_AB.fits",
            "Template_s1dw_GL699_sc1d_w_file_AB.fits.header",
            "Template_GL699_tellu_obj_AB.fits",
            "Template_GL699_tellu_obj_AB.fits.header ",
            "Template_s1dv_GL699_sc1d_v_file_AB.fits ",
            "Template_s1dv_GL699_sc1d_v_file_AB.fits.header",
        ]
    ):
        test_subject = APEROName(test_config.lookup.get('instrument'), [entry])
        if entry.startswith('ccf'):
            assert test_subject.product_id == f'ccf_{expected_obs_id}', f'ccf wrong {entry}'
        elif entry.startswith('debug'):
            assert test_subject.product_id == f'debug_{expected_obs_id}', f'debug wrong {entry}'
        elif entry.startswith('lbl'):
            assert test_subject.product_id == f'lbl_{expected_obs_id}', f'lbl wrong {entry}'
        elif entry.startswith('spec'):
            assert test_subject.product_id == f'spectrum_{expected_obs_id}', f'spectrum wrong {entry}'
        elif '_tellu_' in entry:
            assert test_subject.product_id == f'telluric_{expected_obs_id}', f'telluric wrong {entry}'

