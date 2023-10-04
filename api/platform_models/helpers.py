import logging
import joy
import models
from clients import Mastodon
import http_errors


def get_mastodon_credentials(base_url):
    client = models.mastodon_client.find({"base_url": base_url})
    if client == None:
        try:
            _client = Mastodon.register_client(base_url)
        except Exception as e:
             logging.warning(e, exc_info=True)
             raise http_errors.bad_request(
                f"unable to reach Mastodon server at {base_url} " +
                "Please confirm the URL is correct and try again."
              )

        client = models.mastodon_client.add(_client)

    return client