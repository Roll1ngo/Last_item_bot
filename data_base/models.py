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


class AuthParameters(Base):
    __tablename__ = 'auth_parameters'
    id = Column(String, primary_key=True, default="main_auth_record") # Використовуємо фіксований ID для єдиного запису
    access_token = Column(String, nullable=True)
    refresh_token = Column(String, nullable=True)
    active_device_token = Column(String, nullable=True)
    long_lived_token = Column(String, nullable=True)
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<AuthParameters(id='{self.id}', access_token='{self.access_token[:10]}...')>"