import json
from src.doctree import parse_html, DocTree

def test_parse():
    html = """
    <html>
        <body>
            <div><p>Hello</p></div>
            <div><p>World</p></div>
        </body>
    </html>
    """
    html_str = parse_html(html)
    html_tree = DocTree.from_dict(json.loads(html_str))
    assert any([txt == "Hello" for txt,section in html_tree.walk()])
    assert any([txt == "World" for txt,section in html_tree.walk()])