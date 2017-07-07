import unittest
import mock

import tempfile
import os

from delivery.exceptions import ProjectAlreadyDeliveredException

from delivery.models.project import RunfolderProject, GeneralProject
from delivery.models.db_models import StagingOrder, StagingStatus, DeliverySource
from delivery.services.delivery_service import DeliveryService

from delivery.services.mover_service import MoverDeliveryService
from delivery.services.staging_service import StagingService
from delivery.services.runfolder_service import RunfolderService

from delivery.repositories.delivery_sources_repository import DatabaseBasedDeliverySourcesRepository
from delivery.repositories.project_repository import GeneralProjectRepository

class TestDeliveryService(unittest.TestCase):

    runfolder_projects = [RunfolderProject(name="ABC_123",
                                           path="/foo/160930_ST-E00216_0112_BH37CWALXX/Projects/ABC_123",
                                           runfolder_path="/foo/160930_ST-E00216_0112_BH37CWALXX",
                                           runfolder_name="160930_ST-E00216_0112_BH37CWALXX"),
                          RunfolderProject(name="ABC_123",
                                           path="/foo/160930_ST-E00216_0111_BH37CWALXX/Projects/ABC_123",
                                           runfolder_path="/foo/160930_ST-E00216_0111_BH37CWALXX/",
                                           runfolder_name="160930_ST-E00216_0111_BH37CWALXX")]

    general_project = GeneralProject(name="ABC_123", path="/foo/bar/ABC_123")

    def _compose_delivery_service(self,
                                  mover_delivery_service=mock.create_autospec(MoverDeliveryService),
                                  staging_service=mock.create_autospec(StagingService),
                                  delivery_sources_repo=mock.create_autospec(DatabaseBasedDeliverySourcesRepository),
                                  general_project_repo=mock.create_autospec(GeneralProjectRepository),
                                  runfolder_service=mock.create_autospec(RunfolderService),
                                  project_links_dir=mock.MagicMock()):
        mover_delivery_service = mover_delivery_service
        self.staging_service = staging_service
        delivery_sources_repo = delivery_sources_repo
        general_project_repo = general_project_repo
        runfolder_service = runfolder_service
        self.project_links_dir = project_links_dir

        self.delivery_service = DeliveryService(mover_service=mover_delivery_service,
                                                staging_service=self.staging_service,
                                                delivery_sources_repo=delivery_sources_repo,
                                                general_project_repo=general_project_repo,
                                                runfolder_service=runfolder_service,
                                                project_links_directory=self.project_links_dir)

    def setUp(self):
        self._compose_delivery_service()

    def test__create_links_area_for_project_runfolders(self):
        with tempfile.TemporaryDirectory() as tmpdirname:

            self.delivery_service.project_links_directory = tmpdirname

            batch_nbr = 1337
            project_link_area = self.delivery_service._create_links_area_for_project_runfolders("ABC_123",
                                                                                                self.runfolder_projects,
                                                                                                batch_nbr)

            project_linking_area_base = os.path.join(self.delivery_service.project_links_directory,
                                                     "ABC_123",
                                                     str(batch_nbr))
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

    def test_deliver_arbitrary_directory_project(self):

        staging_service_mock = mock.create_autospec(StagingService)
        staging_service_mock.create_new_stage_order.return_value = \
            StagingOrder(id=1,
                         source=self.general_project.path,
                         status=StagingStatus.pending,
                         staging_target='/foo/bar',
                         size=1024
                         )

        general_project_repo_mock = mock.create_autospec(GeneralProjectRepository)
        general_project_repo_mock.get_project.return_value = self.general_project

        delivery_sources_repo_mock = mock.create_autospec(DatabaseBasedDeliverySourcesRepository)
        delivery_sources_repo_mock.source_exists.return_value = False
        delivery_sources_repo_mock.create_source.return_value = DeliverySource(project_name="ABC_123",
                                                                               source_name=self.general_project.name,
                                                                               path=self.general_project.path)

        self._compose_delivery_service(general_project_repo=general_project_repo_mock,
                                       delivery_sources_repo=delivery_sources_repo_mock,
                                       staging_service=staging_service_mock)

        result = self.delivery_service.deliver_arbitrary_directory_project("ABC_123")
        self.assertTrue(result["ABC_123"] == 1)

    def test_deliver_arbitrary_directory_project_force(self):

        staging_service_mock = mock.create_autospec(StagingService)
        staging_service_mock.create_new_stage_order.return_value = \
            StagingOrder(id=1,
                         source=self.general_project.path,
                         status=StagingStatus.pending,
                         staging_target='/foo/bar',
                         size=1024
                         )

        general_project_repo_mock = mock.create_autospec(GeneralProjectRepository)
        general_project_repo_mock.get_project.return_value = self.general_project

        delivery_sources_repo_mock = mock.create_autospec(DatabaseBasedDeliverySourcesRepository)
        delivery_sources_repo_mock.source_exists.return_value = True
        delivery_sources_repo_mock.create_source.return_value = DeliverySource(project_name="ABC_123",
                                                                               source_name=self.general_project.name,
                                                                               path=self.general_project.path)

        self._compose_delivery_service(general_project_repo=general_project_repo_mock,
                                       delivery_sources_repo=delivery_sources_repo_mock,
                                       staging_service=staging_service_mock)

        with self.assertRaises(ProjectAlreadyDeliveredException):
            self.delivery_service.deliver_arbitrary_directory_project("ABC_123", force_delivery=False)

        result = self.delivery_service.deliver_arbitrary_directory_project("ABC_123", force_delivery=True)
        self.assertTrue(result["ABC_123"] == 1)



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
