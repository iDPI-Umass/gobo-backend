import logging
import time
from os import environ
from datetime import timedelta
import joy
import models
from .mastodon import Mastodon, Status, build_status


class LocalTimelineAccount():
    def __init__(self):
        self.id = "local timeline"
        self.url = None
        self.username = None
        self.name = None
        self.icon_url = None
        self.platform = "smalltown"


class Smalltown(Mastodon):
    
    # This needs to exist because of category requirements in the flow composition.
    # However, the result will always be a list with one item, the virtual source
    # representing the Smalltown server's local timeline.
    def list_sources(self):
        return {"accounts": [LocalTimelineAccount()]}

    def get_post_graph(self, source, last_retrieved = None, is_shallow = False):
        logging.info("calling replacement post graph method")
        isDone = False
        oldest_limit = joy.time.convert("date", "iso", 
            joy.time.nowdate() - timedelta(days=int(environ.get("MAXIMUM_RETENTION_DAYS")))
        )
        if is_shallow == True:
            default_limit = 40
        else:
            default_limit = 200
        max_id = None

        statuses = []
        partials = []
        accounts = []

        count = 1
        while True:
            if isDone == True:
                break

            logging.info(f"Smalltown Fetch: {source['base_url']} {max_id}")
            items = self.client.timeline(
                timeline = "local",
                local = True,
                max_id = max_id,
                limit = 40
            )
            time.sleep(1)

            if len(items) == 0:
                break

            max_id = str(items[-1].id)

            if last_retrieved == None:
                for item in items:
                    status = build_status(item)
                    if status is None:
                        continue
                    
                    count += 1
                    if status.published < oldest_limit:
                        isDone = True
                        break
                    if count < default_limit:
                        statuses.append(status)
                    else:
                        isDone = True
                        break
            else:
                for item in items:
                    status = build_status(item)
                    if status is None:
                        continue
                    
                    if status.published < oldest_limit:
                        isDone = True
                        break
                    if status.published > last_retrieved:
                        statuses.append(status)
                    else:
                        isDone = True
                        break


        seen_statuses = set()
        reply_ids = set()
        for status in statuses:
            seen_statuses.add(status.id)
        
        for status in statuses: 
            reblog = status.reblog
            if reblog is not None and reblog.id not in seen_statuses:
                seen_statuses.add(reblog.id)
                partials.append(reblog)

        for status in statuses:
            reply = status.reply
            if reply is not None and reply not in seen_statuses:
                seen_statuses.add(reply)
                reply_ids.add(reply) 



        registered = models.post.pull([
            models.helpers.where("base_url", source["base_url"]),
            models.helpers.where("platform_id", list(reply_ids), "in")
        ])

        for item in registered:
            reply_ids.remove(item["platform_id"])

        for id in reply_ids:
            try:
                logging.info(f"Mastodon: fetching reply {id}")
                status = Status(self.client.status(id))
                # We need to stop traversing the graph so we don't pull in lots of replies.
                # By definition, Mastodon does not allow replies to boosted statuses.
                status.reply = None
                partials.append(status)
            except Exception as e:
                logging.warning(f"failed to fetch status {id} {e}")
            time.sleep(1)



        seen_accounts = set()
        for status in statuses:
            account = status.account
            if account.id not in seen_accounts:
                seen_accounts.add(account.id)
                accounts.append(account)
        for status in partials:
            account = status.account
            if account.id not in seen_accounts:
                seen_accounts.add(account.id)
                accounts.append(account)



        return {
            "statuses": statuses,
            "partials": partials,
            "accounts": accounts,
            "is_list": True
        }