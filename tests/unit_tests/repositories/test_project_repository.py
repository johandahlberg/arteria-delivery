
import unittest
from mock import MagicMock

from delivery.repositories.project_repository import RunfolderProjectRepository

from tests.test_utils import FAKE_RUNFOLDERS


class TestProjectRepository(unittest.TestCase):

    runfolder_respository = MagicMock()
    runfolder_respository.get_runfolders.return_value = FAKE_RUNFOLDERS
    repo = RunfolderProjectRepository(runfolder_repository=runfolder_respository)

    def test_get_projects(self):
        actual_projects = list(self.repo.get_projects())
        self.assertTrue(len(actual_projects) == 4)
