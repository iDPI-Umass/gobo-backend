from urllib.parse import urlparse
import markdown
import mimetypes


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