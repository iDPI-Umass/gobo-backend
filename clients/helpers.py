import markdown
import mimetypes


md = markdown.Markdown(
    safe_mode=True,
    extensions=["mdx_linkify"]
)

def guess_mime(url):
    content_type, encoding = mimetypes.guess_type(url, strict=False)
    return content_type