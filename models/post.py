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

        if row is None:
            row = Post.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()


def safe_add(data):
    with Session() as session:
        if data.get("base_url") is None or data.get("platform_id") is None:
            raise Exception("upsert requires post have base_url and platform_id")

        statement = select(Post) \
            .where(Post.base_url == data["base_url"]) \
            .where(Post.platform_id == data["platform_id"]) \
            .limit(1)

        row = session.scalars(statement).first()

        if row is None:
            row = Post.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            return row.to_dict()


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
        replies = []
        sources = []
        seen_posts = set()
        seen_sources = set()
        roots = set()

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

        # Secondary Shares
        statement = select(Link) \
            .where(Link.origin_type == "post") \
            .where(Link.origin_id.in_(feed)) \
            .where(Link.target_type == "post") \
            .where(Link.name == "shares")

        rows = session.scalars(statement).all()

        for row in rows:
            shares.append([row.origin_id, row.target_id])
            seen_posts.add(row.target_id)
            roots.add(row.target_id)

        # Tertiary Shares
        statement = select(Link) \
            .where(Link.origin_type == "post") \
            .where(Link.origin_id.in_(list(roots))) \
            .where(Link.target_type == "post") \
            .where(Link.name == "shares")

        rows = session.scalars(statement).all()

        for row in rows:
            shares.append([row.origin_id, row.target_id])
            seen_posts.add(row.target_id)


        # Now we have basis to look for all reply secondary posts
        for id in feed:
            roots.add(id)

        statement = select(Link) \
            .where(Link.origin_type == "post") \
            .where(Link.origin_id.in_(list(roots))) \
            .where(Link.target_type == "post") \
            .where(Link.name == "replies")

        rows = session.scalars(statement).all()

        for row in rows:
            replies.append([row.origin_id, row.target_id])
            seen_posts.add(row.target_id)


        # Now we have all primary, secondary, and tertiary posts. Fetch all.
        statement = select(Post).where(Post.id.in_(list(seen_posts)))
        rows = session.scalars(statement).all()
        for row in rows:
            posts.append(row.to_dict()) 

      
        # And with all full posts, we can see their source IDs. Pull all sources.
        for post in posts:
            seen_sources.add(post["source_id"])

        statement = select(Source).where(Source.id.in_(list(seen_sources)))
        rows = session.scalars(statement).all()
        for row in rows:
            sources.append(row.to_dict())    


        # Package the response body.
        output = {
            "feed": feed,
            "posts": posts,
            "shares": shares,
            "replies": replies,
            "sources": sources
        }

        if next_token is not None:
            output["next"] = next_token

        return output


def view_post_graph(id):
    with Session() as session:
        feed = []
        posts = []
        replies = []
        shares = []
        sources = []
        seen_posts = set()
        seen_sources = set()

        feed.append(id)
        seen_posts.add(id)


        # Get the possible reply
        statement = select(Link) \
            .where(Link.origin_type == "post") \
            .where(Link.origin_id.in_(feed)) \
            .where(Link.target_type == "post") \
            .where(Link.name == "replies")

        rows = session.scalars(statement).all()
        for row in rows:
            replies.append([row.origin_id, row.target_id])
            seen_posts.add(row.target_id)


        # Get first order shares
        statement = select(Link) \
            .where(Link.origin_type == "post") \
            .where(Link.origin_id.in_(list(seen_posts))) \
            .where(Link.target_type == "post") \
            .where(Link.name == "shares")

        rows = session.scalars(statement).all()
        ids = []
        for row in rows:
            shares.append([row.origin_id, row.target_id])
            seen_posts.add(row.target_id)
            ids.append(row.target_id)
        
        # Second order shares
        statement = select(Link) \
            .where(Link.origin_type == "post") \
            .where(Link.origin_id.in_(ids)) \
            .where(Link.target_type == "post") \
            .where(Link.name == "shares")
        
        rows = session.scalars(statement).all()
        for row in rows:
            shares.append([row.origin_id, row.target_id])
            seen_posts.add(row.target_id)

        
        # Fetch all posts based on the IDs we've collected.
        statement = select(Post).where(Post.id.in_(list(seen_posts)))
        rows = session.scalars(statement).all()
        for row in rows:
            posts.append(row.to_dict()) 

      

        # And based on the source listed in each post, grab the source records.
        for post in posts:
            seen_sources.add(post["source_id"])

        statement = select(Source).where(Source.id.in_(list(seen_sources)))
        rows = session.scalars(statement).all()
        for row in rows:
            sources.append(row.to_dict())    


        # Collate it all for a response graph.
        return {
            "feed": feed,
            "posts": posts,
            "shares": shares,
            "replies": replies,
            "sources": sources,
        }