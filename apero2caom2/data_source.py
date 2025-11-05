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

from datetime import datetime, timezone
from caom2utils.data_util import get_local_file_info
from caom2pipe.astro_composable import check_fitsverify
from caom2pipe.data_source_composable import DataSource, LocalFilesDataSourceRunnerMeta, TodoFileDataSourceRunnerMeta
from apero2caom2.main_app import APEROName


class APEROTodoFileDataSource(TodoFileDataSourceRunnerMeta):

    def _find_work(self, entry_path):
        with open(entry_path) as f:
            for line in f:
                temp = line.strip()
                if len(temp) > 0:
                    # ignore empty lines
                    self._logger.debug(f'Adding entry {temp} to work list.')
                    self._work.append(APEROName(
                        instrument=self._config.lookup.get('instrument'), source_names=[temp]
                    ))


class APEROLocalFilesDataSource(LocalFilesDataSourceRunnerMeta):

    def _verify_file(self, fqn):
        """
        Check file content for correctness, by whatever rules the file needs to conform to.
        Over-ride the default implementation because APERO ingests png and rdb files as well as
        fits.

        :param fqn: str fully-qualified file name
        :return: True if the file passes the check, False otherwise
        """
        if '.fits' in fqn:
            return check_fitsverify(fqn)
        else:
            return True

    def default_filter(self, dir_entry):
        """
        :param dir_entry: os.DirEntry
        """
        self._logger.debug(f'Begin default_filter with {dir_entry}')
        work_with_file = True
        if DataSource.default_filter(self, dir_entry):
            if dir_entry.name.startswith('.'):
                # skip dot files
                work_with_file = False
            else:
                self._temp_storage_name = APEROName(
                    instrument=self._config.lookup.get('instrument'), source_names=[dir_entry.path]
                )
                local_file_info = get_local_file_info(dir_entry.path)
                index = 0
                self._temp_storage_name.set_file_info(index, local_file_info)
                if '.hdf5' in dir_entry.name or '.h5' in dir_entry.name:
                    # no hdf5 validation
                    pass
                elif self._verify_file(dir_entry.path):
                    # only work with files that pass the FITS verification
                    if self._cleanup_when_storing:
                        if self._store_modified_files_only:
                            # only transfer files with a different MD5 checksum
                            work_with_file = self._is_remote_different(index, self._temp_storage_name)
                            if not work_with_file:
                                self._logger.warning(
                                    f'{dir_entry.path} has the same md5sum at CADC. Not transferring.'
                                )
                                # KW - 23-06-21
                                # if the file already exists, with the same checksum, at CADC, Kanoa says move it to the
                                # 'succeeded' directory.
                                self._skipped_files += 1
                                self._move_action(dir_entry.path, self._cleanup_success_directory)
                                self._reporter.capture_success(
                                    self._temp_storage_name.obs_id,
                                    self._temp_storage_name.file_name,
                                    datetime.now(tz=timezone.utc).timestamp(),
                                )
                                self._temp_storage_name = None
                    else:
                        work_with_file = True
                else:
                    self._rejected_files += 1
                    if self._cleanup_when_storing:
                        self._logger.warning(
                            f'Rejecting {dir_entry.path}. Moving to {self._cleanup_failure_directory}'
                        )
                        self._move_action(dir_entry.path, self._cleanup_failure_directory)
                    self._reporter.capture_failure(
                        self._temp_storage_name, BaseException('_verify_file errors'), '_verify_file errors'
                    )
                    work_with_file = False
                    self._temp_storage_name = None
        else:
            work_with_file = False
        self._logger.debug(f'Done default_filter says work_with_file is {work_with_file} for {dir_entry.path}')
        return work_with_file
