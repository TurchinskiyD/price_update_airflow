from sqlalchemy import Column, INTEGER, VARCHAR
import sys
sys.path.append("..")
from model.base import Base


class Sale(Base):
    __tablename__ = 'prod_sale'
    product_id= Column(VARCHAR(50), nullable=False, unique=True, primary_key=True)
    sale = Column(INTEGER, default = 0)