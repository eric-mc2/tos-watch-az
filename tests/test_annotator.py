from src.annotator import annotate_doc
from src.doctree import parse_html

def test_parse():
    html = """
    <html>
        <body>
            <div><p>Hello</p></div>
            <div><p>World</p></div>
        </body>
    </html>
    """
    tree = parse_html(html)
    notes = annotate_doc('hi','hi','hi',tree)
    assert any([x.text == "Hello" for x in notes])
    assert any([x.text == "World" for x in notes])