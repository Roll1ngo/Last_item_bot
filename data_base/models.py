from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Index

from data_base.base import Base



class OffersParameters(Base):
    __tablename__ = 'offers_parameters'

    id = Column(Integer, primary_key=True, autoincrement=True)
    offer_id = Column(String, nullable=False)
    seo_term = Column(String, nullable=False)
    region_id = Column(String, nullable=False)
    filter_attribute = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)

    __table_args__ = (
        Index('ix_parameters_offer_id', 'offer_id'),
    )

    def __repr__(self):
        return (f"<Parameter(id={self.id}, offer_id='{self.offer_id}', "
                f"seo_term='{self.seo_term}', region_id='{self.region_id}')>")

