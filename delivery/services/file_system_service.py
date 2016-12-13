
import os

import logging

log = logging.getLogger(__name__)


class FileSystemService(object):
    """
    File system service, used for accessing the file system in a way that can
    easily be mocked out in testing.
    """

    @staticmethod
    def list_directories(base_path):
        """
        List all directories
        :param base_path: base path to list directories in.
        :return: a generator of paths to directories
        """

        log.debug("Listing dirs in: {}".format(os.path.abspath(base_path)))
        for my_dir in os.listdir(base_path):
            dir_abs_path = os.path.abspath(os.path.join(base_path, my_dir))

            if os.path.isdir(dir_abs_path):
                log.debug("Found dir: {}".format(dir_abs_path))
                yield dir_abs_path

    def find_project_directories(self, projects_base_dir):
        """
        Find project directories
        :param projects_base_dir: directory to list
        :return: a generator of paths to project directories
        """
        return self.list_directories(projects_base_dir)

    def find_runfolder_directories(self, base_path):
        """
        Find runfolder directories
        :param base_path: directory to list
        :return: a generator or paths to runfolder directories
        """
        return self.list_directories(base_path)

    @staticmethod
    def isdir(path):
        """
        Shadows os.path.isdir
        :param path: to check
        :return: boolean if path is dir or not
        """
        return os.path.isdir(path)

    @staticmethod
    def isfile(path):
        """
        Shadows os.path.isfile
        :param path: to check
        :return: boolean if path is file or not
        """
        return os.path.isfile(path)

    @staticmethod
    def basename(path):
        """
        Shadows os.path.basename
        :param path: to get base name for
        :return: base name of file as per os.path.basename
        """
        return os.path.basename(path)

    @staticmethod
    def abspath(path):
        """
        Shaods os.path.abspath
        :param path: to get abspath for
        :return: abs path to file/dir as per os.path.abspath
        """
        return os.path.abspath(path)
