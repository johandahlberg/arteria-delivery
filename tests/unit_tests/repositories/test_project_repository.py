
import unittest
from mock import MagicMock

from delivery.models.project import GeneralProject, RunfolderProject
from delivery.repositories.project_repository import GeneralProjectRepository
from delivery.services.file_system_service import FileSystemService

from tests.test_utils import FAKE_RUNFOLDERS


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
