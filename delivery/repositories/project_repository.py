
import os

from delivery.services.file_system_service import FileSystemService
from delivery.models.project import GeneralProject

class RunfolderProjectRepository(object):
    """
    Repository for materializing project instances
    """

    def __init__(self, runfolder_repository):
        """
        Instantiate a new repository
        :param runfolder_repository: a `FileSystemBasedRunfolderRepository` or something the implements the
        `get_runfolders` method
        """
        self.runfolder_repository = runfolder_repository

    def get_projects(self):
        """
        Pick up all projects
        :return: a generator of project instances
        """
        for runfolder in self.runfolder_repository.get_runfolders():
            for project in runfolder.projects:
                yield project


class GeneralProjectRepository(object):
    """
    Repository for a general project. For this purpose a project is represented by any director in
    root directory defined by the configuration.
    """

    def __init__(self, root_directory, filesystem_service=FileSystemService()):
        """
        TODO
        :param root_directory:
        :param filesystem_service:
        """
        self.root_directory = root_directory
        self.filesystem_service = filesystem_service

    def get_projects(self):
        """
        TODO
        :return:
        """
        for directory in self.filesystem_service.list_directories(self.root_directory):
            abs_path = self.filesystem_service.abspath(directory)
            yield GeneralProject(name=self.filesystem_service.basename(abs_path),
                                 path=abs_path)
