
import os

from delivery.exceptions import ProjectAlreadyDeliveredException

from delivery.models.db_models import StagingStatus


class DeliveryService(object):

    def __init__(self, delivery_sources_repo, general_project_repo, staging_service, mover_service):
        self.delivery_sources_repo = delivery_sources_repo
        self.staging_service = staging_service
        self.mover_service = mover_service
        self.general_project_repo = general_project_repo

    def deliver_single_runfolder(self, runfolder_name):
        pass

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

        # TODO Is it enough to check if it exists? Or do we also need to
        #      check what status it has?
        source_exists = self.delivery_sources_repo.source_exists(source)

        # If such a Delivery source exists, only proceed if
        # override is activated
        if source_exists and not force_delivery:
            raise ProjectAlreadyDeliveredException("Project {} has already been delivered.".format(project_name))

        self.delivery_sources_repo.add_source(source)

        # Start staging
        stage_order = self.staging_service.create_new_stage_order(path=source.path)
        self.staging_service.stage_order(stage_order)

        return {source.project_name: stage_order.id}

    def check_staging_status(self, staging_uid):
        pass

