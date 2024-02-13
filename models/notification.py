import logging
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud

Notification = tables.Notification
Link = tables.Link
Post = tables.Post
Source = tables.Source
PostEdge = tables.PostEdge

add, get, update, remove, query, find, pluck, pull, scan = itemgetter(
    "add", "get", "update", "remove", "query", "find", "pluck", "pull", "scan"
)(define_crud(Notification))


def upsert(data):
    with Session() as session:
        if data.get("base_url") is None or data.get("platform_id") is None:
            raise Exception("upsert requires notification to have base_url and platform_id")

        statement = select(Notification) \
            .where(Notification.base_url == data["base_url"]) \
            .where(Notification.platform_id == data["platform_id"]) \
            .limit(1)

        row = session.scalars(statement).first()

        if row == None:
            row = Notification.write(data)
            session.add(row)
            session.commit()
            return row.to_dict()
        else:
            row.update(data)
            session.commit()
            return row.to_dict()


def view_identity_feed(data):
    with Session() as session:
        feed = []
        notifications = []
        post_edges = []
        posts = []
        shares = []
        sources = []
        seen_posts = set()
        seen_sources = set()


        # Start by pulling a page from this identity's mention feed.
        statement = select(Link) \
            .where(Link.origin_type == "identity") \
            .where(Link.origin_id == data["identity_id"]) \
            .where(Link.target_type == "notification") \
            .where(Link.name == data["view"])

        if data.get("start") is not None:
            statement = statement.where(Link.secondary < data["start"])
                    
        statement = statement.order_by(Link.secondary.desc()) \
            .limit(data["per_page"])

        rows = session.scalars(statement).all()
        for row in rows:
            feed.append(row.target_id)



        # The next_token tells the client how to try to get the next page.
        # If we've just pulled an empty page, the client can try again later
        # with the same next_token. Depending how secondary is calculated,
        # there might be something there. 
        if len(rows) == 0:
            next_token = data.get("start")
        else:
            next_token = rows[-1].secondary
        


        # There are currently no meta-notifications, so we have all neccessary
        # references and can fetch them now.
        statement = select(Notification) \
            .where(Notification.id.in_(list(feed)))
        
        rows = session.scalars(statement).all()
        for row in rows:
            notifications.append(row.to_dict()) 



        # Locate posts associated with these notifications.
        statement = select(Link) \
            .where(Link.origin_type == "notification") \
            .where(Link.origin_id.in_(feed)) \
            .where(Link.target_type == "post") \
            .where(Link.name == "notifies")

        rows = session.scalars(statement).all()
        for row in rows:
            seen_posts.add(row.target_id)



        # Share Secondary Posts
        statement = select(Link) \
            .where(Link.origin_type == "post") \
            .where(Link.origin_id.in_(list(seen_posts))) \
            .where(Link.target_type == "post") \
            .where(Link.name == "shares")

        rows = session.scalars(statement).all()
        secondaries = set()
        for row in rows:
            shares.append([row.origin_id, row.target_id])
            seen_posts.add(row.target_id)
            secondaries.add(row.target_id)



        # For all the posts we've seen so far, they're eligible for social
        # graph operations in the HX. Lookup their relevant edges.
        statement = select(PostEdge) \
            .where(PostEdge.identity_id == data["identity_id"]) \
            .where(PostEdge.post_id.in_(list(seen_posts)))
            
        rows = session.scalars(statement).all()
        for row in rows:
            post_edges.append([ row.post_id, row.name ])


        # Share Tertiary Posts
        statement = select(Link) \
            .where(Link.origin_type == "post") \
            .where(Link.origin_id.in_(list(secondaries))) \
            .where(Link.target_type == "post") \
            .where(Link.name == "shares")

        rows = session.scalars(statement).all()
        for row in rows:
            shares.append([row.origin_id, row.target_id])
            seen_posts.add(row.target_id)


      
        # Now we have all primary, secondary, and tertiary posts. Fetch all.
        statement = select(Post).where(Post.id.in_(list(seen_posts)))
        rows = session.scalars(statement).all()
        for row in rows:
            posts.append(row.to_dict()) 

      

        # And with all full notifications and posts, we can see references to
        # their associated sources. Fetch all sources now.
        for post in posts:
            seen_sources.add(post["source_id"])
        for notification in notifications:
            seen_sources.add(notification["source_id"])

        statement = select(Source).where(Source.id.in_(list(seen_sources)))
        rows = session.scalars(statement).all()
        for row in rows:
            sources.append(row.to_dict())    



        # Package the response body.
        output = {
            "feed": feed,
            "notifications": notifications,
            "posts": posts,
            "shares": shares,
            "sources": sources,
            "post_edges": post_edges
        }

        if next_token is not None:
            output["next"] = next_token

        return output