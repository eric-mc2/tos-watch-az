import numpy as np
from collections import Counter
import logging
import json
from src.log_utils import setup_logger
from src.doctree import DocTree
from src.docchunk import DocChunk


logger = setup_logger(__name__, logging.INFO)

def annotate_doc(company: str, policy: str, timestamp: str, tree:str) -> list[DocChunk]:
    """Read and parse text file."""
    doctree = DocTree.from_dict(json.loads(tree))
    chunks = []
    for (text, section) in doctree.walk():
        chunk = DocChunk(company=company,
                        policy=policy,
                        version_ts=timestamp,
                        chunk_idx=section.read_idx,
                        text=text)
        chunks.append(chunk)
    return chunks

def _entropy_pooling(corpus: list[DocChunk]) -> list[DocChunk]:
    counter = 0
    threshold = 3
    corpus_iter = iter(corpus)
    pooled = None
    chunks = []
    try:
        pooled = corpus_iter.__next__()
    except StopIteration:
        return chunks
    for next_doc in corpus_iter:
        if (next_doc.company != pooled.company or 
            next_doc.policy != pooled.policy or 
            _paragraph_entropy(pooled.text) > threshold):
            chunks.append(pooled)
            counter += 1
            pooled = next_doc
            pooled.idx = counter
        else:
            sep = "\n" if pooled.text.endswith(".") else ".\n"
            pooled.text += sep + next_doc.text
    chunks.append(pooled)
    return chunks

def _paragraph_entropy(text, tokenizer=str.split):
    tokens = tokenizer(text)
    counts = np.array(list(Counter(tokens).values()))
    probs = counts / counts.sum()
    return -np.sum(probs * np.log2(probs + 1e-12))  # add epsilon to avoid log(0)

def _check_doc(company: str, policy: str, ts: str, lines:list[str]):
    n_sentences = len(lines)
    sentence_lengths = [len(line) for line in lines]
    p50 = int(np.percentile(sentence_lengths, 50)) if sentence_lengths else 0
    p90 = int(np.percentile(sentence_lengths, 90)) if sentence_lengths else 0
    logger.debug(f"Doc {company}/{policy}{ts} yielded {n_sentences} sentences. "
                f"Token quantiles: 50%: {p50}, 90%: {p90}")

def _test_length(chunks: list[DocChunk]):
    for chunk in chunks:
        if len(chunk.text) > 8192: # max transformer tokens:
            logger.warning("%s/%s[%d] sentence length exceeded",
                        chunk.company, chunk.policy, chunk.chunk_idx)

def _test_entropy(texts: list[str]):
    entropies = np.array([_paragraph_entropy(t) for t in texts])
    logger.debug("Sentence-level word entropies:")
    p5 = np.percentile(entropies,5) if texts else 0
    p25 = np.percentile(entropies,25) if texts else 0
    p50 = np.percentile(entropies,50) if texts else 0
    p75 = np.percentile(entropies,75) if texts else 0
    p95 = np.percentile(entropies,95) if texts else 0
    logger.debug("Q5: {:.4e} ".format(p5 ))
    logger.debug("Q25: {:.4e}".format(p25))
    logger.debug("Q50: {:.4e}".format(p50))
    logger.debug("Q75: {:.4e}".format(p75))
    logger.debug("Q95: {:.4e}".format(p95))

def main(company: str, policy: str, timestamp: str, tree: str) -> str:
    chunks = annotate_doc(company, policy, timestamp, tree)
    chunks = _entropy_pooling(chunks)
    texts = [x.text for x in chunks]
    
    _check_doc(company, policy, timestamp, texts)
    _test_length(chunks)
    _test_entropy(texts)

    return json.dumps([str(chunk) for chunk in chunks], indent=2)
