import logging
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud

Post = tables.Post
Link = tables.Link
Source = tables.Source
PostEdge = tables.PostEdge


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
        feed = []
        post_edges = []
        posts = []
        shares = []
        replies = []
        sources = []
        seen_posts = set()
        seen_sources = set()


        # Start by pulling a page from this identity's feed.
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
        for row in rows:
            id = row.target_id
            feed.append(id)
            seen_posts.add(id)



        # The next_token tells the client how to try to get the next page.
        # If we've just pulled an empty page, the client can try again later
        # with the same next_token. Depending how secondary is calculated,
        # there might be something there. 
        if len(rows) == 0:
            next_token = data.get("start")
        else:
            next_token = rows[-1].secondary



        # Reply secondary posts go next because they might have share edges.
        statement = select(Link) \
            .where(Link.origin_type == "post") \
            .where(Link.origin_id.in_(feed)) \
            .where(Link.target_type == "post") \
            .where(Link.name == "replies")

        rows = session.scalars(statement).all()
        for row in rows:
            replies.append([row.origin_id, row.target_id])
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
            .where(PostEdge.post_id.in_(list(seen_posts))) \
            
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
            "post_edges": post_edges,
            "posts": posts,
            "shares": shares,
            "replies": replies,
            "sources": sources
        }

        if next_token is not None:
            output["next"] = next_token

        return output


def view_post_graph(data):
    with Session() as session:
        feed = []
        post_edges = []
        posts = []
        replies = []
        shares = []
        sources = []
        seen_posts = set()
        seen_sources = set()

        feed.append(data["id"])
        seen_posts.add(data["id"])


        # Possible Reply Secondary
        statement = select(Link) \
            .where(Link.origin_type == "post") \
            .where(Link.origin_id.in_(feed)) \
            .where(Link.target_type == "post") \
            .where(Link.name == "replies") \
            .limit(1)

        row = session.scalars(statement).first()
        if row is not None:
            replies.append([row.origin_id, row.target_id])
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
            .where(PostEdge.post_id.in_(list(seen_posts))) \
            
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

      

        # And with all full posts, we can see their source IDs. Pull all sources.
        for post in posts:
            seen_sources.add(post["source_id"])

        statement = select(Source).where(Source.id.in_(list(seen_sources)))
        rows = session.scalars(statement).all()
        for row in rows:
            sources.append(row.to_dict())    


        # Collate it all for a response graph.
        return {
            "feed": feed,
            "post_edges": post_edges,
            "posts": posts,
            "shares": shares,
            "replies": replies,
            "sources": sources,
        }