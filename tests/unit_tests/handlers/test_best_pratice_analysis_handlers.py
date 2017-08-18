
import json
from mock import MagicMock


from tornado.testing import *
from tornado.web import Application

from delivery.app import routes
from delivery.services.best_practice_analysis_service import BestPracticeAnalysisService
from delivery.models.project import GeneralProject
from delivery.exceptions import ProjectNotFoundException

from tests.test_utils import DummyConfig


class TestBestPracticeAnalysisHandlers(AsyncHTTPTestCase):

    API_BASE = "/api/1.0"

    general_project_repo = MagicMock()
    this_file = os.path.abspath(__file__)
    project_dir = os.path.join(this_file, "../../../resources/projects/DEF_123")
    general_project_repo.get_project.return_value = GeneralProject(name="DEF_123",
                                                                   path=project_dir)
    best_practice_analysis_service = BestPracticeAnalysisService(general_project_repo)

    def get_app(self):
        return Application(
            routes(
                config=DummyConfig(),
                runfolder_repo=MagicMock(),
                best_practice_analysis_service=self.best_practice_analysis_service))

    def test_get_samples(self):
        response = self.fetch(self.API_BASE + "/project/DEF_123/best_practice_samples")
        self.assertEqual(response.code, 200)
        response_json = json.loads(response.body)
        self.assertListEqual(sorted(response_json["samples"]), sorted(["s1", "s2", "s3"]))

    def test_get_samples_unknown_project(self):
        self.general_project_repo.get_project.side_effect = ProjectNotFoundException()
        response = self.fetch(self.API_BASE + "/project/foo/best_practice_samples")
        self.assertEqual(response.code, 404)

    def test_get_samples_slashes_in_project_name(self):
        response = self.fetch(self.API_BASE + "/project/DEF_123/notvalid/best_practice_samples")
        self.assertEqual(response.code, 404)
