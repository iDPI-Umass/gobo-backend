from urllib.parse import urlparse
import markdown
import mimetypes
import joy
from datetime import timedelta


md = markdown.Markdown(
    safe_mode=True,
    extensions=["mdx_linkify"]
)

def guess_mime(url):
    content_type, encoding = mimetypes.guess_type(url, strict=False)
    return content_type

def partition(items, size):
    for i in range(0, len(items), size):
        yield items[i : i+size]

def get_base_url(url):
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.hostname}"

def two_weeks_ago():
    return joy.time.convert(
        start = "date",
        end = "iso",
        value = joy.time.nowdate() - timedelta(days = 14)
    )