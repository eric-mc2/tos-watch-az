from collections import defaultdict
from typing import Iterator, Self
from bs4.element import PageElement
from enum import Enum
from bs4 import BeautifulSoup
import json
from typing import Optional
import re
from bleach import clean as bleach_clean

class SemLevel(Enum):
    CHILD = -1
    SIBLING = 0
    FIXED_1 = 1
    FIXED_2 = 2
    FIXED_3 = 3
    FIXED_4 = 4
    FIXED_5 = 5
    FIXED_6 = 6

FIXED = [SemLevel.FIXED_1,SemLevel.FIXED_2,SemLevel.FIXED_3,
         SemLevel.FIXED_4,SemLevel.FIXED_5,SemLevel.FIXED_6]

SemTag = defaultdict(lambda: SemLevel.CHILD,
    root = SemLevel.SIBLING,
    div = SemLevel.SIBLING,
    table = SemLevel.SIBLING,
    section = SemLevel.SIBLING,
    h1 = SemLevel.FIXED_1,
    h2 = SemLevel.FIXED_2,
    h3 = SemLevel.FIXED_3,
    h4 = SemLevel.FIXED_4,
    h5 = SemLevel.FIXED_5,
    h6 = SemLevel.FIXED_6,
)

RE_WHITESPACE = re.compile(r"(\s)+", re.UNICODE)

class DocTree:
    """Represents HTML text chunks as a tree. Allows reading in 'flat' mode 
    ie. just each next chunk, or in 'context' mode, prepending the root-to-leaf text."""
    def __init__(self, text: str, tag: str) -> None:
        self.text = text
        self.level = SemTag[tag]
        self.tag = tag
        self.children: list[Self] = []
        self.parent: Optional[Self] = None
        self.sibling_idx = 0
    
    @classmethod
    def from_dict(cls, d: dict) -> Self:
        root = cls(d['text'], d['tag'])
        for c in d['children']:
            node = cls.from_dict(c)
            node.parent = root
            root._add_child(node)
        return root
    
    def as_dict(self, full=True):
        d = dict(text = self.text,
                 read_idx = self.read_idx,
                 tag = self.tag,
                 children = [c.as_dict(full=full) for c in self.children])
        if self.parent is None:
            d |= dict(size = self.size,
                     depth = self.depth,)
        if not full:
            del d['tag']
            del d['read_idx']
        return d
    
    def save(self, file_path: str):
        with open(file_path, "w") as f:
            json.dump(self.as_dict(full=True), f, indent=2)

    @classmethod
    def load(cls, file_path: str):
        with open(file_path, "r") as f:
            data = json.load(f)
        return cls.from_dict(data)
    
    def __str__(self):
        return json.dumps(self.as_dict(full=False), indent=2, sort_keys=False)
    
    def __repr__(self):
        return json.dumps(self.as_dict(full=True), indent=2, sort_keys=False)
    
    @property
    def size(self):
        return 1 + sum((c.size for c in self.children))
    
    @property
    def depth(self):
        return self._depth(0)
    
    def _depth(self, previous=0):
        current = previous + 1
        if self.children:
            return max((c._depth(current) for c in self.children))
        else:
            return current
        
    @property
    def read_idx(self):
        if self.parent is None:
            return -1 + (not self.skip_read)
        else:
            skips = sum([c.skip_read for c in self.parent.children[:self.sibling_idx]])
            reads = self.sibling_idx - skips
            return self.parent.read_idx + reads + (not self.skip_read)
            
    @property
    def skip_read(self):
        return not self.text or self.text.isspace()
    
    def _add_child(self, node: Self):
        node.parent = self
        node.sibling_idx = len(self.children)
        self.children.append(node)

    def insert(self, node: Self):
        if node.level == SemLevel.CHILD:
            # New blocks are always children of current node
            for branch in reversed(self.children):
                if branch.level != SemLevel.CHILD:
                    branch._add_child(node)
                    break
            if not node.parent:
                self._add_child(node)
        elif node.level == SemLevel.SIBLING:
            # New blocks are always children of current node
            self._add_child(node)
        elif node.level in FIXED:
            # Traverse up parents to find insertion point
            for branch in reversed(self.children):
                if (branch.level != SemLevel.CHILD 
                    and branch.level in FIXED
                    and branch.level.value < node.level.value):
                    branch.insert(node)
                    break
            if not node.parent and self.level.value > node.level.value: # and node.level > 0:
                if self.parent is not None:
                    self.parent.insert(node)
                else:
                    raise RuntimeError(f"Dropping node {node}")
            elif not node.parent:
                self._add_child(node)
        
    def walk(self, context="") -> Iterator[tuple[str, Self]]:
        def separate(ctx: str) -> str:    
            if not ctx or ctx[-1].isspace():
                return ""
            elif ctx.endswith("."):
                return "\n"
            else:
                return ".\n"
        sep = "" if not self.text else separate(context)
        if not self.skip_read:
            yield (context + sep + self.text, self)
        for child in self.children:
            yield from child.walk(context + sep + self.text)
            append = child.tag == "text" #and child.level != SemLevel.CHILD
            context += child.text if append else ""
            sep = "" if not child.text else separate(context)

    def find(self, read_idx: int) -> Optional[Self]:
        if self.read_idx == read_idx:
            return self
        for child in self.children:
            found = child.find(read_idx)
            if found is not None:
                return found
        return None

def parse_html(content: str) -> str:
    html = BeautifulSoup(content, "html.parser")
    root = DocTree("", "root")
    root = _parse_doctree(html, root)
    return root.__repr__()

def _parse_doctree(html: PageElement, root: DocTree) -> DocTree:
    """Split the html text into chunks per high-level document structure,
        also respecting semantic structure."""
    RECURSE_TAGS = ['html','body','main','article','section','div','c-wiz','ul','ol','table','tbody','tr','td']
    for elem in html.children:
        tag = elem.name if elem.name else "text"
        if tag in RECURSE_TAGS:
            leaf = DocTree("", tag)
            root.insert(leaf)
            _parse_doctree(elem, leaf)
        elif tag in ['text','script','footer']:
            continue
        elif elem.text and not elem.text.isspace():
            text = bleach_clean(elem.text)
            text = RE_WHITESPACE.sub(" ", text).strip()
            leaf = DocTree(text, tag)
            root.insert(leaf)
    return root
