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


def upsert(data):
    with Session() as session:
        if data.get("base_url") is None or data.get("platform_id") is None:
            raise Exception("upsert requires post have base_url and platform_id")

        statement = select(Post) \
            .where(Post.base_url == data["base_url"]) \
            .where(Post.platform_id == data["platform_id"]) \
            .limit(1)

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
        posts = []
        shares = []
        sources = []
        seen_posts = set()
        seen_sources = set()

        # The next_token tells the client how to try to get the next page.
        # If we've just pulled an empty page, the client can try again later
        # with the same next_token. Depending how secondary is calculated,
        # there might be something there. 
        if len(rows) == 0:
            next_token = data.get("start")
        else:
            next_token = rows[-1].secondary


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
            shares.append([row.origin_id, row.target_id])
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


        output = {
            "feed": feed,
            "posts": posts,
            "shares": shares,
            "sources": sources,
        }

        if next_token is not None:
            output["next"] = next_token

        return output


def view_post_graph(id):
    with Session() as session:
        feed = []
        posts = []
        shares = []
        sources = []
        seen_posts = set()
        seen_sources = set()

        feed.append(id)
        seen_posts.add(id)

        statement = select(Link) \
            .where(Link.origin_type == "post") \
            .where(Link.origin_id.in_(feed)) \
            .where(Link.target_type == "post") \
            .where(Link.name == "shares")

        rows = session.scalars(statement).all()

        for row in rows:
            shares.append([row.origin_id, row.target_id])
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
            "shares": shares,
            "sources": sources,
        }