import unittest
import mock

import tempfile
import os

from delivery.models.project import RunfolderProject
from delivery.services.delivery_service import DeliveryService

from delivery.services.mover_service import MoverDeliveryService
from delivery.services.staging_service import StagingService
from delivery.services.runfolder_service import RunfolderService

from delivery.repositories.delivery_sources_repository import DatabaseBasedDeliverySourcesRepository
from delivery.repositories.project_repository import GeneralProjectRepository


class TestDeliveryService(unittest.TestCase):

    def setUp(self):
        #TODO Set this up
        mover_delivery_service = mock.create_autospec(MoverDeliveryService)
        self.staging_service = mock.create_autospec(StagingService)
        delivery_sources_repo = mock.create_autospec(DatabaseBasedDeliverySourcesRepository)
        general_project_repo = mock.create_autospec(GeneralProjectRepository)
        runfolder_service = mock.create_autospec(RunfolderService)
        self.project_links_dir = tempfile.mkdtemp()

        self.delivery_service = DeliveryService(mover_service=mover_delivery_service,
                                                staging_service=self.staging_service,
                                                delivery_sources_repo=delivery_sources_repo,
                                                general_project_repo=general_project_repo,
                                                runfolder_service=runfolder_service,
                                                project_links_directory=self.project_links_dir)

    def test__create_links_area_for_project_runfolders(self):
        with tempfile.TemporaryDirectory() as tmpdirname:

            self.staging_service.project_links_directory = tmpdirname

            projects = [RunfolderProject(name="ABC_123",
                                         path="/foo/160930_ST-E00216_0112_BH37CWALXX/Projects/ABC_123",
                                         runfolder_path="/foo/160930_ST-E00216_0112_BH37CWALXX",
                                         runfolder_name="160930_ST-E00216_0112_BH37CWALXX"),
                        RunfolderProject(name="ABC_123",
                                         path="/foo/160930_ST-E00216_0111_BH37CWALXX/Projects/ABC_123",
                                         runfolder_path="/foo/160930_ST-E00216_0111_BH37CWALXX/",
                                         runfolder_name="160930_ST-E00216_0111_BH37CWALXX")]

            project_link_area = self.delivery_service._create_links_area_for_project_runfolders("ABC_123", projects)

            project_linking_area_base = os.path.join(self.delivery_service.project_links_directory, "ABC_123")
            self.assertEqual(project_link_area,
                             project_linking_area_base)

            self.assertTrue(
                os.path.islink(
                    os.path.join(
                        project_linking_area_base,
                        "160930_ST-E00216_0112_BH37CWALXX")))

            self.assertTrue(
                os.path.islink(
                    os.path.join(
                        project_linking_area_base,
                        "160930_ST-E00216_0111_BH37CWALXX")))

# TODO Move these tests to run on new delivery service
#    # - Be able to stage a existing runfolder
#    def test_stage_runfolder(self):
#        runfolder1 = FAKE_RUNFOLDERS[0]
#
#        self.mock_runfolder_repo.get_runfolder.return_value = runfolder1
#        mock_staging_repo = self.MockStagingRepo()
#
#        self.staging_service.staging_repo = mock_staging_repo
#
#        result = self.staging_service.stage_runfolder(
#            runfolder_id=runfolder1.name, projects_to_stage=[])
#
#        expected = {'DEF_456': 2, 'ABC_123': 1}
#        self.assertDictEqual(result, expected)
#
#        # - Reject stating a runfolder if the given projects is not available
#        with self.assertRaises(ProjectNotFoundException):
#            self.staging_service.stage_runfolder(runfolder_id='foo_runfolder', projects_to_stage=['foo'])

#    # - Reject staging a runfolder which does not exist runfolder
#    def test_stage_runfolder_does_not_exist(self):
#        with self.assertRaises(RunfolderNotFoundException):
#
#            self.mock_runfolder_repo.get_runfolder.return_value = None
#            self.staging_service.stage_runfolder(runfolder_id='foo_runfolder', projects_to_stage=[])

#    # - Stage a 'general' directory if it exists
#    def test_stage_directory(self):
#        mock_staging_repo = self.MockStagingRepo()
#
#        self.staging_service.staging_repo = mock_staging_repo
#
#        self.mock_general_project_repo.get_projects.return_value = [GeneralProject(name='foo', path='/bar/foo'),
#                                                                    GeneralProject(name='bar', path='/bar/foo')]
#
#        expected = {'foo': 1}
#        result = self.staging_service.stage_directory('foo')
#        self.assertDictEqual(expected, result)
#
#    # - Reject staging a directory that does not exist...
#    def test_stage_directory_does_not_exist(self):
#        with self.assertRaises(ProjectNotFoundException):
#            self.mock_general_project_repo.get_projects.return_value = []
#            self.staging_service.stage_directory('foo')

#    def test_stage_runfolders_for_project(self):
#        self.assertTrue(False)

if __name__ == '__main__':
    unittest.main()
