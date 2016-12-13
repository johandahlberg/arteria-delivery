
from delivery.handlers import *
from delivery.handlers.utility_handlers import ArteriaDeliveryBaseHandler
from delivery.repositories.project_repository import RunfolderProjectRepository


class ProjectBaseHandler(ArteriaDeliveryBaseHandler):
    """
    Base class for handlers concerned with presenting projects
    """

    def initialize(self, **kwargs):
        self.runfolder_repo = kwargs["runfolder_repo"]
        self.project_repo = RunfolderProjectRepository(
            runfolder_repository=self.runfolder_repo)
        super(ProjectBaseHandler, self).initialize(kwargs)


class ProjectHandler(ProjectBaseHandler):
    """
    Handler class for managing projects
    """

    def get(self):
        """
        Returns all projects as json on the following format:
        {
           "projects": [
                {
                    "path": "/path/to/160930_ST-E00216_0111_BH37CWALXX/Projects/ABC_123",
                    "name": "ABC_123",
                    "runfolder_path": "/path/to/160930_ST-E00216_0111_BH37CWALXX"
                }
            ]
        }
        """
        projects = list(self.project_repo.get_projects())
        self.write_list_of_models_as_json(projects, key="projects")


class ProjectsForRunfolderHandler(ProjectBaseHandler):
    """
    Manage projects for a specific runfolder
    """

    def get(self, runfolder_name):
        """
        Returns all projects for the specified runfolder on format:
        {
           "projects": [
                {
                    "path": "/path/to/160930_ST-E00216_0111_BH37CWALXX/Projects/ABC_123",
                    "name": "ABC_123",
                    "runfolder_path": "/path/to/160930_ST-E00216_0111_BH37CWALXX"
                }
            ]
        }
        """
        runfolder = self.runfolder_repo.get_runfolder(runfolder_name)
        if runfolder:
            projects = runfolder.projects
            self.write_list_of_models_as_json(projects, key="projects")
        else:
            self.send_error(status_code=NOT_FOUND)
