

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
