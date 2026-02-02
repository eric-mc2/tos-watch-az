from dataclasses import dataclass
from itertools import accumulate

from src.services.differ import DiffDoc

@dataclass
class PromptChunker:
    token_limit: int
    headroom: float = 0.8

    def chunk_prompt(self, diffs) -> list[DiffDoc]:
        limit = self.token_limit * self.headroom

        # XXX: Edge case: need to handle any or many single diffs being larger than limit.
        # split_diffs = []
        # for i,d in enumerate(doc.diffs):
        #     if len(d.before) + len(d.after) < limit:
        #         split_diffs.append(d)
        #     else:

        # XXX: approx! slightly under-counts characters!
        # XXX: character count <> token count! over-estimates by a factor!
        chunk_sizes = [len(d.before) + len(d.after) for d in diffs]

        page_nums = [size // limit for size in accumulate(chunk_sizes)]
        pages = []
        for i,d in enumerate(diffs):
            if i == 0 or page_nums[i-1] != page_nums[i]:
                pages.append(DiffDoc(diffs=[d]))
            else:
                pages[-1].diffs.append(d)
        return pages