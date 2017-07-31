
import os
import logging

from delivery.services.file_system_service import FileSystemService
from delivery.exceptions import ProjectAlreadyDeliveredException, RunfolderNotFoundException, ProjectNotFoundException
from delivery.models.delivery_modes import DeliveryMode

from delivery.models.db_models import StagingStatus

log = logging.getLogger(__name__)


class DeliveryService(object):

    def __init__(self,
                 delivery_sources_repo,
                 general_project_repo,
                 runfolder_service,
                 staging_service,
                 mover_service,
                 project_links_directory,
                 file_system_service=FileSystemService()):
        self.delivery_sources_repo = delivery_sources_repo
        self.staging_service = staging_service
        self.mover_service = mover_service
        self.general_project_repo = general_project_repo
        self.runfolder_service = runfolder_service
        self.project_links_directory = project_links_directory
        self.file_system_service = file_system_service

    def _validate_source(self, source, force_delivery, path):
        source_exists = self.delivery_sources_repo.source_exists(source)

        # If such a Delivery source exists, only proceed if
        # override is activated
        if source_exists and not force_delivery:
            raise ProjectAlreadyDeliveredException(
                "Project source {} has already been delivered.".format(source))
        elif source_exists and force_delivery:
            self.delivery_sources_repo.update_path_of_source(source, new_path=path)
        else:
            self.delivery_sources_repo.add_source(source)

    def _validate_and_stage_source(self, source, force_delivery, path):
        self._validate_source(source, force_delivery, path)
        # Start staging
        stage_order = self.staging_service.create_new_stage_order(path=source.path)
        self.staging_service.stage_order(stage_order)
        return stage_order

    def _start_staging_projects(self, projects, force_delivery):
        projects_and_stage_order_ids = {}
        for project in projects:
            source = self.delivery_sources_repo.create_source(project_name=project.name,
                                                              source_name="{}/{}".format(project.runfolder_name,
                                                                                         project.name),
                                                              path=project.path)
            stage_order = self._validate_and_stage_source(source, force_delivery, project.path)
            projects_and_stage_order_ids[project.name] = stage_order.id

        return projects_and_stage_order_ids

    def _create_links_area_for_project_runfolders(self, project_name, projects, batch_nbr):
        """
        Creates a directory in which it creates links to all runfolders for the projects
        given. This is useful so that we can then rsync that directory to
        the staging area.
        :param project_name: name of the project
        :param projects: runfolders with the specified project on them
        :param batch_nbr: which batch of deliveries of this project this corresponds to
        :return: the path to the dir created
        """

        project_dir = os.path.join(self.project_links_directory, project_name, str(batch_nbr))
        try:
            self.file_system_service.makedirs(project_dir)
        except FileExistsError as e:
            log.warning("Project dir: {} already exists".format(project_dir))

        for project in projects:
            try:
                link_name = os.path.join(project_dir, project.runfolder_name)
                self.file_system_service.symlink(project.path, link_name)
            except FileExistsError:
                log.warning("Project link: {} already exists".format(project_dir))
                continue

        return self.file_system_service.abspath(project_dir)

    def deliver_single_runfolder(self, runfolder_name, only_these_projects, force_delivery):
        projects = list(self.runfolder_service.find_projects_on_runfolder(runfolder_name, only_these_projects))
        return self._start_staging_projects(projects, force_delivery)

    def _get_projects_for_delivery(self, projects, mode):
        # First create sources for all the projects, depending on mode
        # The make a separate delivery source for the actually stages folder...
        for project in projects:
            source = self.delivery_sources_repo.create_source(project_name=project.name,
                                                              source_name="{}/{}".format(project.runfolder_name,
                                                                                         project.name),
                                                              path=project.path)
            try:
                self._validate_source(source, force_delivery=False, path=project.path)
                self.delivery_sources_repo.add_source(source)
            except ProjectAlreadyDeliveredException as e:
                if mode == DeliveryMode.CLEAN:
                    raise e
                elif mode == DeliveryMode.BATCH:
                    continue
                elif mode == DeliveryMode.FORCE:
                    self.delivery_sources_repo.update_path_of_source(source, project.path)
                    yield project
                else:
                    raise NotImplementedError("This is not a valid state, delivery mode needs to be CLEAN/"
                                              "BATCH/FORCE.")

    def deliver_all_runfolders_for_project(self, project_name, mode):
        projects = list(self.runfolder_service.find_runfolders_for_project(project_name))

        if len(projects) < 1:
            raise ProjectNotFoundException("Could not find any Project "
                                           "folders for project name: {}".format(project_name))

        batch_nbr = self.delivery_sources_repo.find_highest_batch_nbr(project_name)
        if not batch_nbr:
            batch_nbr = 1

        projects_to_deliver = self._get_projects_for_delivery(projects, mode)
        links_directory = self._create_links_area_for_project_runfolders(project_name, projects_to_deliver, batch_nbr)

        source = self.delivery_sources_repo.create_source(project_name=project_name,
                                                          source_name="{}/batch{}".format(project_name, batch_nbr),
                                                          path=links_directory,
                                                          batch_nbr=batch_nbr)

        stage_order = self._validate_and_stage_source(source, force_delivery=False, path=source.path)
        return {source.project_name: stage_order.id}

    def deliver_arbitrary_directory_project(self, project_name, dir_name=None, force_delivery=False):

        if not dir_name:
            dir_name = project_name

        # Construct DeliverySource for the project
        project = self.general_project_repo.get_project(dir_name)
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
