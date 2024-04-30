import logging
import os
from .gobo_linkedin import GoboLinkedin
from datetime import datetime, timedelta
import joy
import models

class SessionFrame():
    def __init__(self, identity, session):
        self.identity = identity
        self.session = session
    
    @staticmethod    
    def from_bundle(identity, bundle):
        if identity is None:
            raise Exception("raw identity dictionary passed to SessionFrame constructor is None")
        if bundle is None:
            raise Exception("raw bundle dictionary passed to SessionFrame constructor is None")

        access_token = bundle["tokens"]["access_token"]
        delta = timedelta(seconds = bundle["tokens"]["expires_in"])
        expires = joy.time.nowdate() + delta
        access_expires = joy.time.convert("date", "iso", expires)

        # TODO: Do the same for refresh tokens once we're given that priviledge.

        return {
            "person_id": identity["person_id"],
            "identity_id": identity["id"],
            "platform_id": identity["platform_id"],
            "access_token": access_token,
            "access_expires": access_expires
        }
    
    def is_stale(self):
        return self.identity["stale"] == True

    def access_expired(self):
        if self.session is None:
            return True
        timestamp = self.session.get("access_expires")
        if timestamp is None:
            return True
        # Make sure there's at least 10 minutes of access left.
        expires = datetime.fromisoformat(timestamp)
        delta = expires - joy.time.nowdate()
        return delta < timedelta(minutes = 10)


class Linkedin():
    BASE_URL = GoboLinkedin.BASE_URL

    def __init__(self, identity):
        self.identity = identity

    @staticmethod
    def make_login_url(context):
        return GoboLinkedin.make_login_url(context)
    
    @staticmethod
    def exchange_code(code):
        tokens = GoboLinkedin.exchange_code(code)
        user = GoboLinkedin.get_userinfo(tokens["access_token"])
        return {
            "tokens": tokens,
            "user": user
        }
         

    # TODO: This will get more complicated with refresh tokens.
    def login(self):
        identity_id = self.identity["id"]
        session = models.linkedin_session.find({
            "identity_id": identity_id
        })
        if session is None:
            raise Exception(f"unable to find session matching identity {identity_id}")
        
        frame = SessionFrame(self.identity, session)
        if frame.is_stale():
            raise Exception("this session is stale and cannot be used.")
        if frame.access_expired():
            session["stale"] = True
            models.linkedin_session.upsert(session)
            raise Exception("this session is stale and cannot be used.")
        
        self.me = session["access_token"]
        self.client = GoboLinkedin()
        self.invalid = False
        return self.client.login(self.me)

    def get_profile(self):
        return self.client.get_userinfo(self.me)    

    def map_profile(self, data):
        profile = data["profile"]
        identity = data["identity"]

        identity["profile_image"] = profile["picture"]
        identity["username"] = profile["name"]
        return identity


    def create_post(self):
        pass