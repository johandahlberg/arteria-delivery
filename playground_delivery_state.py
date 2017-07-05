
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine

import os

from delivery.models.db_models import SQLAlchemyBase, DeliverySource, StagingOrder,\
    StagingStatus, DeliveryOrder, DeliveryStatus

from delivery.repositories.delivery_sources_repository import DatabaseBasedDeliverySourcesRepository
from delivery.repositories.runfolder_repository import FileSystemBasedRunfolderRepository
from delivery.repositories.project_repository import RunfolderProjectRepository, GeneralProjectRepository

from delivery.repositories.staging_repository import DatabaseBasedStagingRepository
from delivery.repositories.deliveries_repository import DatabaseBasedDeliveriesRepository

from delivery.services.mover_service import MoverDeliveryService
from delivery.services.staging_service import StagingService
from delivery.services.external_program_service import ExternalProgramService
from delivery.services.delivery_service import DeliveryService

from delivery.exceptions import ProjectNotFoundException, TooManyProjectsFound


engine = create_engine('sqlite:///:memory:', echo=False)

session_factory = scoped_session(sessionmaker())
session_factory.configure(bind=engine)
session = session_factory()

SQLAlchemyBase.metadata.create_all(engine)


delivery_source_repo = DatabaseBasedDeliverySourcesRepository(session_factory)

source1 = delivery_source_repo.\
    create_source(project_name="ABC_123", source_name="ABC_123_batch1", path="/foo/bar/ABC_123_batch1")
source2 = delivery_source_repo.create_source(
    project_name="ABC_123", source_name="ABC_123_batch2", path="/foo/bar/ABC_123_batch2")

staging_order1 = StagingOrder(id=1,
                              source="/foo/bar/ABC_123_batch1",
                              status=StagingStatus.staging_successful,
                              staging_target="/foo/staging/ABC_123_batch1",
                              size=100,
                              pid=123)

delivery_order1 = DeliveryOrder(id=1,
                                delivery_source="/foo/staging/ABC_123_batch1",
                                delivery_project="somedelivery123",
                                mover_pid=111,
                                mover_delivery_id="moversid1",
                                delivery_status=DeliveryStatus.delivery_successful,
                                staging_order_id=1)

delivery_source_repo.add_source(source1)
delivery_source_repo.add_source(source2)

session.add(staging_order1)
session.add(delivery_order1)
session.commit()



staging_dir = '/tmp/staging'
runfolder_dir = '/tmp/runfolder'
project_links_directory = '/tmp/proj_links/'
runfolder_repo = FileSystemBasedRunfolderRepository(runfolder_dir)

general_project_dir = '/home/MOLMED/johda411/workspace/arteria/arteria-delivery/tests/resources/projects'
general_project_repo = GeneralProjectRepository(root_directory=general_project_dir)
runfolder_project_repo = RunfolderProjectRepository(runfolder_repository=runfolder_repo)

external_program_service = ExternalProgramService()

session_factory = scoped_session(sessionmaker())
session_factory.configure(bind=engine)

staging_repo = DatabaseBasedStagingRepository(session_factory=session_factory)

staging_service = StagingService(external_program_service=external_program_service,
                                 runfolder_repo=runfolder_repo,
                                 project_dir_repo=general_project_repo,
                                 staging_repo=staging_repo,
                                 staging_dir=staging_dir,
                                 project_links_directory=project_links_directory,
                                 runfolder_project_repo=runfolder_project_repo,
                                 session_factory=session_factory)

delivery_repo = DatabaseBasedDeliveriesRepository(session_factory=session_factory)

path_to_mover = '/foo/bar/mover'
delivery_service = MoverDeliveryService(external_program_service=external_program_service,
                                        staging_service=staging_service,
                                        delivery_repo=delivery_repo,
                                        session_factory=session_factory,
                                        path_to_mover=path_to_mover)

delivery_service = DeliveryService(mover_service=delivery_service,
                                   staging_service=staging_service,
                                   delivery_sources_repo=delivery_source_repo,
                                   general_project_repo=general_project_repo)

delivery_service.deliver_arbitrary_directory_project(project_name="ABC_123",
                                                     project_alias="ABC_123",
                                                     force_delivery=False)

