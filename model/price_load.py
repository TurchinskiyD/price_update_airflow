
from sqlalchemy import Column, VARCHAR, Float
import sys
sys.path.append("..")
from model.base import Base


class Price(Base):
    __tablename__ = 'prod_update'
    product_id= Column(VARCHAR(50), nullable=False, unique=True, primary_key=True)
    status = Column(VARCHAR(20), nullable=False)
    current_price = Column(Float, nullable=False)
    product_name = Column(VARCHAR(250))

