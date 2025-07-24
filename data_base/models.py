from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime

from data_base.connection import Base


class Parameters(Base):
    __tablename__ = 'parameters'

    id = Column(Integer, primary_key=True, autoincrement=True)
    offer_id = Column(String, nullable=False)
    seo_term = Column(String, nullable=False)
    region_id = Column(String, nullable=False)
    filter_attribute = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)

    def __repr__(self):
        return (f"<Parameter(id={self.id}, offer_id='{self.offer_id}', "
                f"seo_term='{self.seo_term}', region_id='{self.region_id}')>")

