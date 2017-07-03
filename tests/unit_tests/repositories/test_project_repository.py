
import unittest
from mock import MagicMock

from delivery.models.project import GeneralProject, RunfolderProject
from delivery.repositories.project_repository import RunfolderProjectRepository, GeneralProjectRepository
from delivery.services.file_system_service import FileSystemService

from tests.test_utils import FAKE_RUNFOLDERS


class TestRunfolderProjectRepository(unittest.TestCase):

    runfolder_respository = MagicMock()
    runfolder_respository.get_runfolders.return_value = FAKE_RUNFOLDERS
    repo = RunfolderProjectRepository(runfolder_repository=runfolder_respository)

    def test_get_projects(self):
        actual_projects = list(self.repo.get_projects())
        self.assertTrue(len(actual_projects) == 4)

    def test_get_project(self):
        project_name = "ABC_123"
        expected_projects = [RunfolderProject(name="ABC_123",
                                              runfolder_path="/foo/160930_ST-E00216_0111_BH37CWALXX",
                                              path="/foo/160930_ST-E00216_0111_BH37CWALXX/Projects/ABC_123",
                                              runfolder_name="160930_ST-E00216_0111_BH37CWALXX"),
                             RunfolderProject(name="ABC_123",
                                              runfolder_path="/foo/160930_ST-E00216_0112_BH37CWALXX",
                                              path="/foo/160930_ST-E00216_0112_BH37CWALXX/Projects/ABC_123",
                                              runfolder_name="160930_ST-E00216_0112_BH37CWALXX")]

        actual_projects = list(self.repo.get_project(project_name))

        self.assertEqual(len(actual_projects), 2)
        self.assertEqual(actual_projects, expected_projects)


class TestGeneralProjectRepository(unittest.TestCase):

    class FakeFileSystemService(FileSystemService):

        @staticmethod
        def list_directories(base_path):
            return ['/foo/bar', '/bar/foo']

    def test_get_projects(self):
        fake_filesystem_service = self.FakeFileSystemService()
        repo = GeneralProjectRepository(root_directory='foo', filesystem_service=fake_filesystem_service)

        expected = [GeneralProject(name='bar', path='/foo/bar'),
                    GeneralProject(name='foo', path='/bar/foo')]

        actual = repo.get_projects()
        self.assertEqual(list(actual), expected)
