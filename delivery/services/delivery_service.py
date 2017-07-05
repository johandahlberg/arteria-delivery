
import os
import logging

from delivery.exceptions import ProjectAlreadyDeliveredException, RunfolderNotFoundException, ProjectNotFoundException

from delivery.models.db_models import StagingStatus

log = logging.getLogger(__name__)

class RunfolderService(object):
    # TODO Break out into separate file

    def __init__(self, runfolder_repo):
        self.runfolder_repo = runfolder_repo

    def find_runfolder(self, runfolder_id):
        runfolder = self.runfolder_repo.get_runfolder(runfolder_id)

        if not runfolder:
            raise RunfolderNotFoundException(
                "Couldn't find runfolder matching: {}".format(runfolder_id))
        else:
            return runfolder

    def _validate_project_lists(self, projects_on_runfolder, projects_to_stage):
        projects_to_stage_set = set(projects_to_stage)
        projects_on_runfolder_set = set(projects_on_runfolder)
        return projects_to_stage_set.issubset(projects_on_runfolder_set)

    def find_projects_on_runfolder(self, runfolder_name, only_these_projects=None):
        runfolder = self.find_runfolder(runfolder_name)

        names_of_project_on_runfolder = list(map(lambda x: x.name, runfolder.projects))

        # If no projects have been specified, get all projects
        if not only_these_projects:
            projects_to_return = names_of_project_on_runfolder

        log.debug("Projects to stage: {}".format(projects_to_return))

        if not self._validate_project_lists(names_of_project_on_runfolder, projects_to_return):
            raise ProjectNotFoundException("Projects to stage: {} do not match projects on runfolder: {}".
                                           format(projects_to_return, names_of_project_on_runfolder))

        for project in runfolder.projects:
            if project.name in projects_to_return:
                yield project


class DeliveryService(object):

    def __init__(self,
                 delivery_sources_repo,
                 general_project_repo,
                 runfolder_repo,
                 staging_service,
                 mover_service):
        self.delivery_sources_repo = delivery_sources_repo
        self.staging_service = staging_service
        self.mover_service = mover_service
        self.general_project_repo = general_project_repo
        self.runfolder_repo = runfolder_repo
        self.runfolder_service = RunfolderService(runfolder_repo)

    def _validate_and_stage_source(self, source, force_delivery, path):
        #      check what status it has?
        source_exists = self.delivery_sources_repo.source_exists(source)

        # If such a Delivery source exists, only proceed if
        # override is activated
        if source_exists and not force_delivery:
            raise ProjectAlreadyDeliveredException(
                "Project {} has already been delivered.".format(source.project_name))
        elif source_exists and force_delivery:
            self.delivery_sources_repo.update_path_of_source(source, new_path=path)
        else:
            self.delivery_sources_repo.add_source(source)

        # Start staging
        stage_order = self.staging_service.create_new_stage_order(path=source.path)
        self.staging_service.stage_order(stage_order)
        return stage_order

    def deliver_single_runfolder(self, runfolder_name, only_these_projects, force_delivery):

        projects = self.runfolder_service.find_projects_on_runfolder(runfolder_name, only_these_projects)

        projects_and_stage_order_ids = {}
        for project in projects:
            source = self.delivery_sources_repo.create_source(project_name=project.name,
                                                              source_name="{}/{}".format(runfolder_name,
                                                                                         project.name),
                                                              path=project.path)
            stage_order = self._validate_and_stage_source(source, force_delivery, project.path)
            projects_and_stage_order_ids[project.name] = stage_order.id

        return projects_and_stage_order_ids

    def deliver_all_runfolders_for_project(self, project_name, mode):
        pass

    def deliver_arbitrary_directory_project(self, project_name, project_alias=None, force_delivery=False):

        if not project_alias:
            project_alias = project_name

        # Construct DeliverySource for the project
        project = self.general_project_repo.get_project(project_alias)
        # Check if such a DeliverySource already exists.

        source = self.delivery_sources_repo.create_source(project_name=project_name,
                                                          source_name=os.path.basename(project.path),
                                                          path=project.path)

        stage_order = self._validate_and_stage_source(source, force_delivery, project.path)
        return {source.project_name: stage_order.id}

    def check_staging_status(self, staging_id):
        stage_order = self.staging_service.get_stage_order_by_id(staging_id)
        return stage_order

    def kill_process_of_stage_order(self, staging_id):
        was_killed = self.staging_service.kill_process_of_stage_order(staging_id)
        return was_killed
