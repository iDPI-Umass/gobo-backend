import logging
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud

Post = tables.Post
Link = tables.Link
Source = tables.Source


add, get, update, remove, query, find, pull = itemgetter(
    "add", "get", "update", "remove", "query", "find", "pull"
)(define_crud(Post))


def safe_add(data):
    with Session() as session:
        statement = select(Post)
        statement = statement.where(Post.url == data["url"])
        statement = statement.limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = Post.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            return row.to_dict()

def upsert(data):
    with Session() as session:
        statement = select(Post)
        statement = statement.where(Post.url == data["url"])
        statement = statement.limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = Post.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()


    source_ids = set()
    for post in posts:
        id = post.get("source_id")
        if id != None:
            source_ids.add(id)
    
    sources = models.source.pluck(list(source_ids))

def view_identity_feed(data):
    with Session() as session:
        statement = select(Link) \
            .where(Link.origin_type == "identity") \
            .where(Link.origin_id == data["identity_id"]) \
            .where(Link.target_type == "post") \
            .where(Link.name == "identity-feed")

        if data.get("start") != None:
            statement = statement.where(Link.secondary < data["start"])
                    
        statement = statement.order_by(Link.secondary.desc()) \
            .limit(data["per_page"])

        rows = session.scalars(statement).all()


        feed = []
        next_token = rows[-1].secondary
        posts = []
        sources = []
        seen_posts = set()
        seen_sources = set()

        for row in rows:
            id = row.target_id
            feed.append(id)
            seen_posts.add(id)

        statement = select(Link) \
            .where(Link.origin_type == "post") \
            .where(Link.origin_id.in_(feed)) \
            .where(Link.target_type == "post") \
            .where(Link.name == "shares")

        rows = session.scalars(statement).all()

        for row in rows:
            seen_posts.add(row.target_id)

        statement = select(Post).where(Post.id.in_(list(seen_posts)))
        rows = session.scalars(statement).all()
        for row in rows:
            posts.append(row.to_dict()) 

      

        for post in posts:
            seen_sources.add(post["source_id"])

        statement = select(Source).where(Source.id.in_(list(seen_sources)))
        rows = session.scalars(statement).all()
        for row in rows:
            sources.append(row.to_dict())    



        return {
            "feed": feed,
            "posts": posts,
            "sources": sources,
            "next": next_token
        }