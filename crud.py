"""
file crud.py
manage CRUD and adapt model data from db to schema data to api rest
"""

from typing import List, Optional, Tuple

from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import desc, extract, between
from sqlalchemy import func
from fastapi.logger import logger
import models, schemas


def get_movie(db: Session, movie_id: int):
    # read from the database (get method read from cache)
    # return object read or None if not found
    db_movie = db.query(models.Movie).filter(models.Movie.id == movie_id).first()
    logger.error("Movie retrieved from DB: {} ; director: {}".format(
              db_movie.title,
              db_movie.director.name if db_movie.director is not None else "no director"))
    return db_movie;

def get_movies(db: Session, skip: int = 0, limit: int = 10000):
    return db.query(models.Movie).offset(skip).limit(limit).all()


def get_star(db: Session, star_id: int):
    # read from the database (get method read from cache)
    # return object read or None if not found
    return db.query(models.Star).filter(models.Star.id == star_id).first()
    #return db.query(models.Star).get(1)
    #return schemas.Star(id=1, name="Fred")

def get_stars(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Star).offset(skip).limit(limit).all()

def _get_stars_by_predicate(*predicate, db: Session):
    return db.query(models.Star).filter(*predicate)

def get_stars_by_birthyear(db: Session, year: int):
    return _get_stars_by_predicate(extract('year', models.Star.birthdate) == year, db=db).order_by(models.Star.name).all()


def create_movie(db: Session, movie: schemas.MovieCreate):
    db_movie = models.Movie(title=movie.title, year=movie.year, duration=movie.duration)
    db.add(db_movie)
    db.commit()
    db.refresh(db_movie)
    return db_movie


def update_movie(db: Session, movie: schemas.Movie):
    db_movie = db.query(models.Movie).filter(models.Movie.id == movie.id).first()
    if db_movie is not None:
        db_movie.title = movie.title
        db_movie.year = movie.year
        db_movie.duration = movie.duration
        db.commit()
    if db_movie is None :
        return None
    return db_movie


def delete_movie(db: Session, movie_id: int):
     db_movie = db.query(models.Movie).filter(models.Movie.id == movie_id).first()
     if db_movie is not None:
         db.delete(db_movie)
         db.commit()
     if db_movie is None :
         return None
     return db_movie


def create_star(db: Session, star: schemas.StarCreate):
    db_star = models.Star(name=star.name, birthdate=star.birthdate)
    db.add(db_star)
    db.commit()
    db.refresh(db_star)
    return db_star


def update_star(db: Session, star: schemas.Star):
    db_star = db.query(models.Star).filter(models.Star.id == star.id).first()
    if db_star is not None:
        db_star.name = star.name
        db_star.birthdate = star.birthdate
        db.commit()
    if db_star is None :
        return None
    return db_star


def delete_star(db: Session, star_id: int):
     db_star = db.query(models.Star).filter(models.Star.id == star_id).first()
     if db_star is not None:
         db.delete(db_star)
         db.commit()
     if db_star is None :
         return None
     return db_star


def get_movies_by_title(db: Session, title: str):
    return db.query(models.Movie).filter(models.Movie.title.like(f'%{title}%')).order_by(models.Movie.year).all()


def get_movies_by_range_year(db: Session, year_min: Optional[int] = None, year_max: Optional[int] = None):
    if year_min is None and year_max is None:
        return None
    elif year_min is None:
        return db.query(models.Movie).filter(models.Movie.year <= year_max).all()
    elif year_max is None:
        return db.query(models.Movie).filter(models.Movie.year >= year_min).all()
    else:
        return db.query(models.Movie).filter(models.Movie.year >= year_min,models.Movie.year <= year_max).all()


def get_movies_by_director_endname(db: Session, endname: str):
    return db.query(models.Movie).join(models.Movie.director).filter(models.Star.name.like(f'%{endname}')).order_by(desc(models.Movie.year)).all()

def get_director_by_movie(db: Session, movie_id: int):
    return db.query(models.Movie).filter(models.Movie.id==movie_id).join(models.Movie.director).all()


def get_movies_by_actor_endname(db: Session, endname: str):
    return db.query(models.Movie).join(models.Movie.actors).filter(models.Star.name.like(f'%{endname}')).order_by(desc(models.Movie.year)).all()

def get_actors_by_movie_id(db: Session, movie_id:int):
    return db.query(models.Star).join(models.Movie.actors).filter(models.Movie.id == movie_id).order_by(desc(models.Movie.year)).all()

def update_movie_director(db: Session, movie_id: int, director_id: int):
    db_movie = get_movie(db=db, movie_id=movie_id)
    db_star = get_star(db=db,star_id=director_id)
    if db_movie is None or db_star is None:
        return None
    db_movie.director = db_star
    # commit transaction : update SQL
    db.commit()
    return db_movie

def update_movie_actors(db: Session, movie_id: int, star_ids: List[int]):
    db_movie = get_movie(db=db, movie_id=movie_id)
    db_actors = db.query(models.Star).filter(models.Star.id.in_(star_ids)).all()
    if db_movie is None or db_actors is None:
        return None
    db_movie.actors = db_actors
    db.commit()
    db.refresh(db_movie)
    return db_movie

def add_movie_actor(db: Session, movie_id: int, star_id: int):
    db_movie=get_movie(db=db, movie_id=movie_id)
    db_star=get_star(db=db, star_id=star_id)
    if db_movie is None or db_star is None:
        return None
    db_movie.actors.append(db_star)
    db.commit()
    return db_movie

def update_movie_actor(db: Session, movie_id: int, star_id: int):
    db_movie = get_movie(db=db, movie_id=movie_id)
    db_star = get_star(db=db,star_id=star_id)
    if db_movie is None or db_star is None:
        return None
    db_movie.actors = db_star
    # commit transaction : update SQL
    db.commit()
    return db_movie


def get_movies_stats_by_year_dictionnaire(db: Session):
    query = db.query(models.Movie.year,func.count().label('movie_count'), func.max(models.Movie.duration).label('max_duration'),func.min((models.Movie.duration).label('min_duration')),func.avg((models.Movie.duration).label('avg_duration')))  \
            .group_by(models.Movie.year).models.Movie.yearorder_by(models.Movie.year).all()
    return [{'year':y,'movie_count':mc,'min_duration':mid,'max_duration':mxd,'avg_duration':ad} for y,mc,mid,mxd,ad in query]

def get_stats_movie_by_director(db: Session, min_count: int=10):
    return db.query(models.Star, func.count(models.Movie.id).label("movie_count")).join(models.Movie.director).group_by(models.Star).having(func.count(models.Movie.id)>= min_count).order_by(desc("movie_count")).all()

def get_stats_movie_by_actor(db: Session, min_count: int=10):
    query = db.query(models.Star.name, func.count(models.Movie.id).label("movie_count"),func.min(models.Movie.year).label('first_movie_date'),func.max(models.Movie.year).label('last_movie_date')).join(models.Movie.actors).group_by(models.Star).having(func.count(models.Movie.id)>= min_count) \
    .order_by(desc("movie_count")).all()
    return [{'star':s, 'movie_count':mc,'first_movie_date':fmd, 'last_movie_date': smd} for s,mc,fmd,smd in query]
