
import logging
import os
import re

from delivery.models.runfolder import Runfolder
from delivery.models.project import RunfolderProject
from delivery.services.file_system_service import FileSystemService

log = logging.getLogger(__name__)


class FileSystemBasedRunfolderRepository(object):
    """
    Uses the file system as a source of truth for information about what runfolders are available.
    """

    def __init__(self, base_path, file_system_service=FileSystemService()):
        """
        Instantiate a new FileSystemBasedRunfolderRepository
        :param base_path: the directory where runfolders are stored
        :param file_system_service: a service which can access the file system.
        """
        self._base_path = base_path
        self.file_system_service = file_system_service

    def _get_runfolders(self):
        # TODO Filter based on expression for runfolders...
        runfolder_expression = r"^\d+_"

        directories = self.file_system_service.find_runfolder_directories(self._base_path)
        for directory in directories:
            if re.match(runfolder_expression, os.path.basename(directory)):

                name = os.path.basename(directory)
                path = os.path.join(self._base_path, directory)

                projects_base_dir = os.path.join(path, "Projects")
                project_directories = self.file_system_service.find_project_directories(
                    projects_base_dir)

                runfolder = Runfolder(name=name, path=path, projects=None)

                def project_from_dir(d):
                    return RunfolderProject(
                        name=os.path.basename(d),
                        path=os.path.join(projects_base_dir, d),
                        runfolder_path=path)

                # There are scenarios where there are no project directories in the runfolder,
                # i.e. when fastq files have not yet been divided into projects
                if project_directories:
                    runfolder.projects = list(map(
                        project_from_dir, project_directories))

                yield runfolder

    def get_runfolders(self):
        """
        Get all runfolders
        :return: a generator of known runfolders
        """
        return self._get_runfolders()

    def get_runfolder(self, runfolder):
        """
        Get a Runfolder object matching the specified name
        :param runfolder: to look for
        :return: the matching runfolder, or None if no match
        :raises: a AssertionError if more than one runfolder was found
                matching the given name.
        """
        runfolders = self.get_runfolders()
        matching_name = list([r for r in runfolders if r.name == runfolder])
        if len(matching_name) > 1:
            raise AssertionError("Found more than 1 runfolder matching: ".format(r))
        if len(matching_name) > 0 and matching_name[0]:
            return matching_name[0]
        else:
            return None
