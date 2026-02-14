from dataclasses import dataclass
from itertools import accumulate
from math import ceil

from src.adapters.llm.protocol import Message
from src.services.llm import LLMService
from src.transforms.differ import DiffDoc, DiffSection
from src.transforms.chunker import WordChunker

@dataclass
class PromptChunker:
    llm: LLMService
    token_limit: int
    headroom: float = 0.8

    def chunk_prompt(self, system: str, history: list[Message], diffs: DiffDoc) -> list[DiffDoc]:
        limit = self.token_limit * self.headroom
        chunker = WordChunker(limit)

        # First break DiffDoc's list of diffs into smaller DiffDocs with smaller lists.
        page_nums = [size // limit for size in accumulate(map(len(diffs.diffs)))]
        pages = []
        for i,d in enumerate(diffs.diffs):
            if i == 0 or page_nums[i-1] != page_nums[i]:
                pages.append(DiffDoc(diffs=[d]))
            else:
                pages[-1].diffs.append(d)

        # Check each DiffDoc and break up very large diffs into smaller sections.
        chunked_diffs = []
        for page in pages:
            if len(page.diffs) > 1:
                # If multiple diffs are in here its guaranteed to be under the limit
                chunked_diffs.append(page)
            else:
                # It's a single item and it's probably too big.
                diff_text = page.diffs[0].model_dump_json()
                messages = history + [Message("user", diff_text)]
                text_len = sum((len(x.content) for x in messages))
                token_len = self.llm.adapter.count_tokens(system, messages)
                txts = chunker.process(diff_text, token_len, text_len)
                # XXX: This is WRONG in this context. Because we need aligned diffs.
                #       It would be better to summarize / condense these chunks,
                #       or use them as RAG.
                for txt in txts:
                    chunked_diffs.append(DiffDoc(diffs=[DiffSection(index=0, before="", after=txt)]))


        return pages