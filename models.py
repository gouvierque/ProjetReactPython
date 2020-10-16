
"""
model.py : database row <-> objet python
"""
from sqlalchemy import Column, Integer, String, SmallInteger, Date, ForeignKey, Table
from sqlalchemy.orm import relationship
    #, ForeignKey
#from sqlalchemy.orm import relationship

from database import Base

#table associative

association_table= Table('play', Base.metadata, Column('id_movie',Integer,ForeignKey('movies.id')),Column('id_actor',Integer,ForeignKey('stars.id')))

class Movie(Base):
    __tablename__ = "movies"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(length=250), nullable=False)
    year = Column(Integer, nullable=False)
    duration = Column(Integer, nullable=True)
    id_director = Column(Integer, ForeignKey('stars.id'), nullable=True)
    director = relationship('Star')
    actors = relationship('Star', secondary=association_table)


class Star(Base):
    __tablename__ = "stars"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(length=150), nullable=False)
    birthdate = Column(Date, nullable=True)
