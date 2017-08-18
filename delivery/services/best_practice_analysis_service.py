

from delivery.services.file_system_service import FileSystemService


class BestPracticeAnalysisService(object):

    def __init__(self, general_project_repo):
        self.general_project_repo = general_project_repo
        self.file_system_service = FileSystemService()

    def get_samples(self, project_name):
        project = self.general_project_repo.get_project(project_name=project_name)
        for directory in self.file_system_service.list_directories(project.path):
            if self.file_system_service.isfile("{}.lst".format(directory)) and \
                    self.file_system_service.isfile("{}.md5".format(directory)):
                yield self.file_system_service.basename(directory)

