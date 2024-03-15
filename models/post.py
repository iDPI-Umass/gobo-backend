import logging
from operator import itemgetter
from sqlalchemy import select
from db.base import Session
from db import tables
from .helpers import define_crud
from . import reference

Post = tables.Post
Link = tables.Link
Source = tables.Source
PostEdge = tables.PostEdge


add, get, update, remove, query, find, pull, scan = itemgetter(
    "add", "get", "update", "remove", "query", "find", "pull", "scan"
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

# Based on https://stackoverflow.com/a/1015405
def bind(instance, f, as_name=None):
    if as_name is None:
        as_name = f.__name__
    bound_method = f.__get__(instance, instance.__class__)
    setattr(instance, as_name, bound_method)
    return bound_method




class FeedBuilder():
    def __init__(self, session):
        self.session = session

        self.feed = []
        self.post_edges = []
        self.posts = []
        self.shares = []
        self.threads = []
        self.sources = []
        self.seen_posts = set()
        self.seen_sources = set()
        self.secondaries = set()

    def get_threads(self):
        statement = select(Link) \
            .where(Link.origin_type == "post") \
            .where(Link.origin_id.in_(self.feed)) \
            .where(Link.target_type == "post") \
            .where(Link.name == "threads") \
            .order_by(Link.secondary.asc())

        rows = self.session.scalars(statement).all()
        for row in rows:
            self.threads.append([row.origin_id, row.target_id])
            self.seen_posts.add(row.target_id)

    def get_secondary(self):
        statement = select(Link) \
            .where(Link.origin_type == "post") \
            .where(Link.origin_id.in_(list(self.seen_posts))) \
            .where(Link.target_type == "post") \
            .where(Link.name == "shares")

        rows = self.session.scalars(statement).all()
        for row in rows:
            self.shares.append([row.origin_id, row.target_id])
            self.seen_posts.add(row.target_id)
            self.secondaries.add(row.target_id)

    # For all the posts we've seen so far, they're eligible for social
    # graph operations in the HX. Lookup their relevant edges.
    def get_action_edges(self, identity_id):
        statement = select(PostEdge) \
            .where(PostEdge.identity_id == identity_id) \
            .where(PostEdge.post_id.in_(list(self.seen_posts)))
            
        rows = self.session.scalars(statement).all()
        for row in rows:
            self.post_edges.append([ row.post_id, row.name ])

    def get_tertiary(self):
        statement = select(Link) \
            .where(Link.origin_type == "post") \
            .where(Link.origin_id.in_(list(self.secondaries))) \
            .where(Link.target_type == "post") \
            .where(Link.name == "shares")

        rows = self.session.scalars(statement).all()
        for row in rows:
            self.shares.append([row.origin_id, row.target_id])
            self.seen_posts.add(row.target_id)

    # Now we have all primary, secondary, and tertiary posts. Fetch all.
    def pull_posts(self):    
        statement = select(Post) \
            .where(Post.id.in_(list(self.seen_posts)))
        
        rows = self.session.scalars(statement).all()
        for row in rows:
            self.posts.append(row.to_dict())

    # And with all full posts, we can see their source IDs. Pull all sources.
    def pull_sources(self):
        for post in self.posts:
            self.seen_sources.add(post["source_id"])

        statement = select(Source) \
            .where(Source.id.in_(list(self.seen_sources)))
        
        rows = self.session.scalars(statement).all()
        for row in rows:
            self.sources.append(row.to_dict())

    # Now that we have all the records, it's time to think about resources.
    # Especially when it comes to edge descriptions within this graph,
    # we have *references* to resources that should be legible as
    # Web resources amenable to composition and other REST properties.
    def address(self):
        for index, id in enumerate(self.feed):
            self.feed[index] = reference.post(id)

        for edge in self.post_edges:
            edge[0] = reference.post(edge[0])

        for share in self.shares:
            share[0] = reference.post(share[0])
            share[1] = reference.post(share[1])

        for thread in self.threads:
            thread[0] = reference.post(thread[0])
            thread[1] = reference.post(thread[1])

    # TODO: The references approach below still needs to be completed.
    def hide_posts(self, target_ids):
        targets = set()
        for id in target_ids:
            targets.add(reference.post(id))

        # Hidden "feed" posts should only come up in direct fetches.
        for index, ref in enumerate(self.feed):
            if ref in targets:
                self.feed[index] = reference.hidden_post()

        # Fully prune actions for hidden posts.
        post_edges = []
        for edge in self.post_edges:
            if edge[0] not in targets:
                post_edges.append(edge)     
        self.post_edges = post_edges
        
        # Fully prune underlying resource for hidden posts.
        posts = []
        for post in self.posts:
            if post["id"] not in target_ids:
                posts.append(post)
        self.posts = posts

        # Hide references to hidden posts in share edges.
        shares = []
        for share in self.shares:
            if share[0] in targets:
                continue
            if share[1] in targets:
                share[1] = reference.hidden_post()
            shares.append(share)
        self.shares = shares

        # Hide references to hidden posts in thread edges.
        threads = []
        for thread in self.threads:
            if thread[0] in targets:
                continue
            if thread[1] in targets:
                thread[1] = reference.hidden_post()
            threads.append(thread)
        self.threads = threads


        # ...
        current_sources = set()
        for post in self.posts:
            current_sources.add(post["source_id"])
        sources = []
        for source in self.sources:
            if source["id"] in current_sources:
                sources.append(source)
        self.sources = sources
  

    def prune_posts(self, targets):
        # Scalar prune. Should only come up in direct fetches.
        feed = []
        for id in self.feed:
            if id in targets:
                continue
            feed.append(id)
        self.feed = feed

        # Scalar prune
        post_edges = []
        for edge in self.post_edges:
            if edge[0] in targets:
                continue
            post_edges.append(edge)     
        self.post_edges = post_edges
        
        # Scalar prune
        posts = []
        for post in self.posts:
            if post["id"] in targets:
                continue
            posts.append(post)
        self.posts = posts

        # Pruning either side of the share edge drops the edge.
        shares = []
        for share in self.shares:
            if share[0] in targets:
                continue
            if share[1] in targets:
                continue
            shares.append(share)
        self.shares = shares

        # Pruning a post drops all edges involving the ancestors.
        # Threads are sorted in ascending time order, so reverse to get children first.
        threads = []
        drop_set = set()
        self.threads.reverse()
        for thread in self.threads:
            if thread[0] in targets:
                continue
            if thread[0] in drop_set:
                continue
            if thread[1] in targets:
                drop_set.add(thread[0])
                continue
            threads.append(thread)
        threads.reverse()
        self.threads = threads


        # We've pruned N posts, now we need to cleanup orphaned sources.
        current = set()
        for post in self.posts:
            current.add(post["source_id"])
        sources = []
        for source in self.sources:
            if source["id"] not in current:
                continue
            sources.append(source)
        self.sources = sources



    def filter_followers_only(self, identity_id):
        sources = set()
        for post in self.posts:
            if post.get("visibility") == "followers only":
                sources.add(post["source_id"])

        # Bail if there are no posts with reduced visibility.
        if len(sources) == 0:
            return

        # Look for follow relationships with the given sources.
        statement = select(Link) \
          .where(Link.origin_type == "identity") \
          .where(Link.origin_id == identity_id) \
          .where(Link.target_type == "source") \
          .where(Link.target_id.in_(list(sources)))
        
        rows = self.session.scalars(statement).all()

        # Bail if the viwer is following all the given sources.
        if len(rows) == len(sources):
            return
        
        # Locate the posts that must be pruned from the graph.
        for row in rows:
            sources.remove(row.target_id)

        targets = set()
        for post in self.posts:
            if post.get("visibility") == "followers only" and post["source_id"] in sources:
                targets.add(post["id"])
        
        # Prune
        self.prune_posts(targets)
                


def view_identity_feed(data):
    def get_primary(self):
        statement = select(Link) \
            .where(Link.origin_type == "identity") \
            .where(Link.origin_id == data["identity_id"]) \
            .where(Link.target_type == "post") \
            .where(Link.name == "identity-feed")

        if data.get("start") != None:
            statement = statement.where(Link.secondary < data["start"])
                    
        statement = statement.order_by(Link.secondary.desc()) \
            .limit(data["per_page"])

        rows = self.session.scalars(statement).all()
        for row in rows:
            self.feed.append(row.target_id)
            self.seen_posts.add(row.target_id)

        # The next_token tells the client how to try to get the next page.
        # If we've just pulled an empty page, the client can try again later
        # with the same next_token. Depending how secondary is calculated,
        # there might be something there. 
        if len(rows) == 0:
            self.next_token = data.get("start")
        else:
            self.next_token = rows[-1].secondary



    with Session() as session:
        builder = FeedBuilder(session)
        bind(builder, get_primary)

        builder.get_primary()
        builder.get_threads()
        builder.get_secondary()
        builder.get_action_edges(data["identity_id"])
        builder.get_tertiary()
        builder.pull_posts()
        builder.pull_sources()
        builder.filter_followers_only(data["identity_id"])

        # Package the response body.
        output = {
            "feed": builder.feed,
            "post_edges": builder.post_edges,
            "posts": builder.posts,
            "shares": builder.shares,
            "threads": builder.threads,
            "sources": builder.sources
        }

        if builder.next_token is not None:
            output["next"] = builder.next_token

        return output


def view_post_graph(data):
    def get_primary(self):
        self.feed.append(data["id"])
        self.seen_posts.add(data["id"])


    with Session() as session:
        builder = FeedBuilder(session)
        bind(builder, get_primary)

        builder.get_primary()
        builder.get_threads()
        builder.get_secondary()
        builder.get_action_edges(data["identity_id"])
        builder.get_tertiary()
        builder.pull_posts()
        builder.pull_sources()
        builder.filter_followers_only(data["identity_id"])


        # Collate it all for a response graph.
        return {
            "feed": builder.feed,
            "post_edges": builder.post_edges,
            "posts": builder.posts,
            "shares": builder.shares,
            "threads": builder.threads,
            "sources": builder.sources,
        }