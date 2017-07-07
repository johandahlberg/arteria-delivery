
from sqlalchemy import exists
from sqlalchemy.sql.expression import func

from delivery.models.db_models import DeliverySource, StagingOrder, StagingStatus, DeliveryOrder, DeliveryStatus

class DatabaseBasedDeliverySourcesRepository(object):
    """
    TODO
    """

    def __init__(self, session_factory):
        """
        Instantiate a new DatabaseBasedDeliveryProjectsRepository
        :param session_factory: a factory method that can create a new sqlalchemy Session object.
        """
        self.session = session_factory()

    def get_projects(self):
        for project in self.session.query(DeliverySource).distinct(DeliverySource.project_name).all():
            yield project

    def get_sources(self):
        return self.session.query(DeliverySource).all()

    @staticmethod
    def create_source(project_name, source_name, path, batch_nbr=None):
        return DeliverySource(project_name=project_name,
                              source_name=source_name,
                              path=path,
                              batch=batch_nbr)

    def add_source(self, source):
        self.session.add(source)
        self.session.commit()

    def get_source(self, project_name, source_name):
        return self.session.query(DeliverySource).\
            filter(DeliverySource.project_name == project_name).\
            filter(DeliverySource.source_name == source_name).scalar()

    def update_path_of_source(self, source, new_path):
        source.path = new_path
        self.session.commit()

    def source_exists(self, source):
        does_exist = self.session.query(exists().
                                        where(DeliverySource.project_name == source.project_name).
                                        where(DeliverySource.source_name == source.source_name))
        return does_exist.scalar()

    def find_highest_batch_nbr(self, project_name):
        return self.session.\
            query(func.max(DeliverySource.batch)).\
            filter(DeliverySource.project_name == project_name).\
            scalar()