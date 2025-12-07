from src.doctree import parse_html

def walk_html(html, flat):
    if not html.strip().startswith("<html>"):
        html = f"<html><body>{html}</body></html>"
    return [x[0] for x in parse_html(html).walk(flat=flat)]

def test_parse():
    html = """
        <div><p>Hello</p></div>
        <div><p>World</p></div>
    """
    lines = walk_html(html, flat=False)
    assert lines[0] == "Hello"
    assert lines[1] == "World"
    

def test_h1234():
    html = """
        <h1>H1</h1>
        <h2>H2</h2>
        <h3>H3</h3>
        <h4>H4</h4>
    """
    lines = walk_html(html, flat=False)
    assert lines[0] == "H1"
    assert lines[1] == "H1.\nH2"
    assert lines[2] == "H1.\nH2.\nH3"
    assert lines[3] == "H1.\nH2.\nH3.\nH4"

def test_h1234_flat():
    html = """
        <h1>H1</h1>
        <h2>H2</h2>
        <h3>H3</h3>
        <h4>H4</h4>
    """
    lines = walk_html(html, flat=True)
    assert lines[0] == "H1"
    assert lines[1] == "H2"
    assert lines[2] == "H3"
    assert lines[3] == "H4"


def test_up_down_h_tags():
    html = """
            <h1>H1</h1>
            <h2>H2</h2>
            <h3>H3</h3>
            <h4>H4</h4>
            <h4>H4</h4>
            <h2>H2</h2>
            <h2>H2</h2>
            <h3>H3</h3>
            <h5>H5</h5>
            <h1>H1</h1>
            """
    result = walk_html(html, flat=False)
    assert len(result) == 10
    assert result[0] == "H1"
    assert result[1] == "H1.\nH2"
    assert result[2] == "H1.\nH2.\nH3"
    assert result[3] == "H1.\nH2.\nH3.\nH4"
    assert result[4] == "H1.\nH2.\nH3.\nH4"
    assert result[5] == "H1.\nH2"
    assert result[6] == "H1.\nH2"
    assert result[7] == "H1.\nH2.\nH3"
    assert result[8] == "H1.\nH2.\nH3.\nH5"
    assert result[9] == "H1"

def test_up_down_h_tags_flat():
    html = """
            <h1>H1</h1>
            <h2>H2</h2>
            <h3>H3</h3>
            <h4>H4</h4>
            <h4>H4</h4>
            <h2>H2</h2>
            <h2>H2</h2>
            <h3>H3</h3>
            <h5>H5</h5>
            <h1>H1</h1>
            """
    result = walk_html(html, flat=True)
    assert len(result) == 10
    assert result[0] == "H1"
    assert result[1] == "H2"
    assert result[2] == "H3"
    assert result[3] == "H4"
    assert result[4] == "H4"
    assert result[5] == "H2"
    assert result[6] == "H2"
    assert result[7] == "H3"
    assert result[8] == "H5"
    assert result[9] == "H1"


def test_div_sibling():
    html = """<div>This is a test document.</div>
            <div>This is a sibling.</div>"""
    result = walk_html(html, flat=False)
    assert len(result) == 2
    assert result[0] == "This is a test document."
    assert result[1] == "This is a sibling."
    result = walk_html(html, flat=True)
    assert len(result) == 2
    assert result[0] == "This is a test document."
    assert result[1] == "This is a sibling."

def test_empty_div():
    html = """<div>
                <div>This is a test document.</div>
                <div>This is a sibling.</div>
              </div>"""
    result = walk_html(html, flat=True)
    assert len(result) == 2
    assert result[0] == "This is a test document."
    assert result[1] == "This is a sibling."
    result = walk_html(html, flat=False)
    assert len(result) == 2
    assert result[0] == "This is a test document."
    assert result[1] == "This is a sibling."

def test_p_sibling():
    html = """
            <p>This is a test document.</p>
            <p>It has multiple paragraphs.</p>
            """
    result = walk_html(html, flat=False)
    assert len(result) == 2
    assert result[0] == "This is a test document."
    assert result[1] == "It has multiple paragraphs."
    result = walk_html(html, flat=True)
    assert len(result) == 2
    assert result[0] == "This is a test document."
    assert result[1] == "It has multiple paragraphs."

def test_h_and_p_tags():
    html = """
            <h1>This is a test document.</h1>
            <p>It has other paragraphs.</p>
            """
    result = walk_html(html, flat=False)
    assert len(result) == 2
    assert result[0] == "This is a test document."
    assert result[1] == "This is a test document.\nIt has other paragraphs."
    result = walk_html(html, flat=True)
    assert len(result) == 2
    assert result[0] == "This is a test document."
    assert result[1] == "It has other paragraphs."

def test_nested_div():
    html = """
            <div>Immediate text<div>Nested text</div></div>
            """
    result = walk_html(html, flat=False)
    assert len(result) == 2
    assert result[0] == "Immediate text"
    assert result[1] == "Immediate text.\nNested text"
    result = walk_html(html, flat=True)
    assert len(result) == 2
    assert result[0] == "Immediate text"
    assert result[1] == "Nested text"

def test_table():
    html = """
            <div>firstly</div>
            <table>
                <thead>Header</thead>
                <tbody>
                <tr>First line</tr>
                <tr>Second line</tr>
                <tr>Third line<div>Nested stuff</div>And more</tr>
                </tbody>
            </table>
            <div>lastly</div>
            """
    result = walk_html(html, flat=False)
    assert len(result) == 8
    assert result[0] == "firstly"
    assert result[1] == "Header"
    assert result[2] == "First line"
    assert result[3] == "Second line"
    assert result[4] == "Third line"
    assert result[5] == "Third line.\nNested stuff"
    assert result[6] == "Third line.\nNested stuff.\nAnd more"
    assert result[7] == "lastly"
    result = walk_html(html, flat=True)
    assert len(result) == 8
    assert result[0] == "firstly"
    assert result[1] == "Header"
    assert result[2] == "First line"
    assert result[3] == "Second line"
    assert result[4] == "Third line"
    assert result[5] == "Nested stuff"
    assert result[6] == "And more"
    assert result[7] == "lastly"

def test_empty_tags():
    html = """
            <div></div>
            <p></p>
            <h2></h2>
            """
    result = walk_html(html, flat=False)
    assert result == []
    result = walk_html(html, flat=True)
    assert result == []

def test_nested_lists():
    html = """
            <ul>
                <li>Item 1
                    <ul>
                        <li>Subitem 1</li>
                        <li>Subitem 2</li>
                    </ul>
                </li>
                <li>Item 2</li>
            </ul>
            """
    result = walk_html(html, flat=False)
    assert "Item 1" in result[0]
    assert "Subitem 1" in result[0] # Not recursing into LI's
    assert "Subitem 2" in result[0]
    assert result[1] == "Item 2"
    result = walk_html(html, flat=True)
    assert "Item 1" in result[0]
    assert "Subitem 1" in result[0] # Not recursing into LI's
    assert "Subitem 2" in result[0]
    assert result[1] == "Item 2"