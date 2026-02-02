from src.transforms.doctree import parse_html
from src.transforms.annotator import annotate_doc, _entropy_pooling


def test_flattens_nested_structure_in_dfs_order():
    """Verify that nested elements are flattened in depth-first order."""
    html_content = """
    <html>
        <body>
            <h1>Section Title</h1>
            <p>First paragraph.</p>
            <section>
                <h2>Subsection</h2>
                <p>Nested paragraph.</p>
            </section>
        </body>
    </html>
    """
    doctree = parse_html(html_content)
    chunks = annotate_doc("test_company", "test_policy", "2024-01-01", repr(doctree))
    
    texts = [chunk.text for chunk in chunks]
    
    assert len(texts) > 0
    assert "Section Title" in texts[0]
    
    # Verify DFS: parent content appears before nested content
    section_idx = next(i for i, t in enumerate(texts) if "Section Title" in t)
    first_para_idx = next(i for i, t in enumerate(texts) if "First paragraph" in t)
    subsection_idx = next(i for i, t in enumerate(texts) if "Subsection" in t)
    nested_idx = next(i for i, t in enumerate(texts) if "Nested paragraph" in t)
    
    assert section_idx < first_para_idx < subsection_idx < nested_idx


def test_combines_short_items_using_entropy():
    """Verify that short, low-entropy items like headings and bullets are combined."""
    html_content = """
    <html>
        <body>
            <h1>Title</h1>
            <ul>
                <li>Item one</li>
                <li>Item two</li>
                <li>Item three</li>
            </ul>
            <p>This is a longer paragraph with more content and entropy that provides substantial information.</p>
        </body>
    </html>
    """
    doctree = parse_html(html_content)
    chunks = annotate_doc("test_company", "test_policy", "2024-01-01", repr(doctree))
    
    # Apply entropy pooling to combine low-entropy items
    pooled_chunks = _entropy_pooling(chunks)
    texts = [chunk.text for chunk in pooled_chunks]
    
    # The longer paragraph should be preserved
    assert any("longer paragraph" in t for t in texts)
    
    # Should have fewer items than original (title + 3 bullets + paragraph = 5)
    # Entropy pooling should combine the short items
    assert len(pooled_chunks) < len(chunks)


def test_preserves_substantial_paragraphs():
    """Verify that substantial paragraphs with high entropy remain separate."""
    html_content = """
    <html>
        <body>
            <p>This is a substantial paragraph with enough content and entropy to stand alone. 
            It contains multiple sentences and meaningful diverse vocabulary.</p>
            
            <p>Another paragraph with sufficient content and entropy to remain separate.
            This also has multiple sentences with varied vocabulary.</p>
        </body>
    </html>
    """
    doctree = parse_html(html_content)
    chunks = annotate_doc("test_company", "test_policy", "2024-01-01", repr(doctree))
    pooled_chunks = _entropy_pooling(chunks)
    
    texts = [chunk.text for chunk in pooled_chunks]
    
    # Both substantial paragraphs should be present
    assert any("substantial paragraph with enough content" in t for t in texts)
    assert any("Another paragraph with sufficient content" in t for t in texts)
    assert len(pooled_chunks) == len(chunks)

def test_handles_empty_document():
    """Verify graceful handling of empty documents."""
    html_content = "<html><body></body></html>"
    doctree = parse_html(html_content)
    chunks = annotate_doc("test_company", "test_policy", "2024-01-01", repr(doctree))
    
    assert isinstance(chunks, list)
    assert len(chunks) == 0


def test_annotates_with_chunk_metadata():
    """Verify that chunks include proper metadata."""
    html_content = """
    <html>
        <body>
            <h1>Title</h1>
            <p>A paragraph.</p>
        </body>
    </html>
    """
    doctree = parse_html(html_content)
    chunks = annotate_doc("acme", "privacy", "2024-01-01", repr(doctree))
    
    assert all(chunk.company == "acme" for chunk in chunks)
    assert all(chunk.policy == "privacy" for chunk in chunks)
    assert all(chunk.version_ts == "2024-01-01" for chunk in chunks)
    assert all(isinstance(chunk.chunk_idx, int) for chunk in chunks)
