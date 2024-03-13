import logging
import time
from os import environ
from datetime import timedelta
import joy
import models
from .mastodon import Mastodon, Status, build_status


class LocalTimelineAccount():
    def __init__(self):
        self.id = "local-timeline"
        self.url = "https://community.publicinfrastructure.org"
        self.username = "gobo:local-timeline"
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
        for status in statuses:
            seen_statuses.add(status.id)
        
        for status in statuses: 
            reblog = status.reblog
            if reblog is not None and reblog.id not in seen_statuses:
                seen_statuses.add(reblog.id)
                partials.append(reblog)

        for status in statuses:
            reply = status.reply
            if reply is not None:
                try:
                    logging.info(f"Smalltown: fetching context for {status.id}")
                    context = self.client.status_context(status.id)
                    status.thread = []
                    for item in context.ancestors:
                        ancestor = build_status(item)
                        if ancestor is None:
                            continue
                        status.thread.append(ancestor.id)
                        if ancestor.id not in seen_statuses:
                            seen_statuses.add(ancestor.id)
                            partials.append(ancestor)
            
                except Exception as e:
                    logging.warning(f"failed to fetch context {status.id} {e}")


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