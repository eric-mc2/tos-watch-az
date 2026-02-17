from typing import List, Protocol, Any, Callable, Iterator, TypeVar, Generic, Optional

T = TypeVar('T')


class Buffer(Generic[T]):
    """A generic buffer class for accumulating items."""

    def __init__(
        self,
        capacity: int,
        combine: Callable[[T, T], T],
        length: Callable[[T], int],
        empty: T,
        combine_cost: int = 0  # delimiter overhead for non-empty additions 
    ):
        self._items: Optional[T] = None
        self._size = 0
        self.capacity = capacity
        self.combine = combine
        self.combine_cost = combine_cost
        self.length = length
        self.empty = empty
        self.is_open = True
        if self.capacity <= 0:
            raise ValueError("Capacity must be positive")

    @property
    def size(self) -> int:
        return self._size

    @property
    def is_empty(self) -> bool:
        return self._items is None

    @property
    def pressure(self) -> float:
        return self.size / self.capacity

    @property
    def content(self) -> T:
        return self.empty if self._items is None else self._items

    def close(self):
        self.is_open = False

    def can_add(self, item: T) -> bool:
        if not self.is_open:
            return False
        if self.is_empty:
            new_size = self.length(item)
        else:
            new_size = self._size + self.combine_cost + self.length(item)
        return new_size <= self.capacity

    def add(self, item: T, force: bool = False) -> bool:
        if not force and not self.can_add(item):
            self.close()
            return False
        if self.is_empty or self._items is None:
            self._items = item
            self._size += self.length(item)
        else:
            self._items = self.combine(self._items, item)
            self._size += self.length(item) + self.combine_cost
        return True

class GenericWindower(Generic[T]):
    """Utility to segment a sequence of items into overlapping chunks."""

    def __init__(
        self,
        capacity: int,
        combine: Callable[[T, T], T],
        length: Callable[[T], int],
        empty: T,
        overlap: float = 0.05,
        combine_cost: int = 0
    ):
        self.slots: List[Buffer[T]] = []
        self.overlap = overlap
        self.capacity = capacity
        self.combine = combine
        self.combine_cost = combine_cost
        self.length = length
        self.empty = empty
        if capacity <= 0:
            raise ValueError("Capacity must be positive")
        if overlap < 0 or overlap >= 1:
            raise ValueError("Overlap must be in range (0,1).")

    def _make_buffer(self) -> Buffer[T]:
        return Buffer(self.capacity, self.combine, self.length, self.empty, self.combine_cost)

    def add(self, item: T, force: bool = False) -> int:
        added = 0
        for slot in self.slots:
            added += slot.add(item, force)
        if added == 0:
            slot = self._make_buffer()
            self.slots.append(slot)
            if slot.can_add(item):
                # Add and leave room for other stuff.
                slot.add(item, force)
                added = 1
            else:
                # We don't want to drop data, so put it in its own buffer
                # and allow downstream processing to break it up.
                slot.add(item, True)
                slot.close()
                # XXX: The return value is weird encoding of success and failure states.
                added = 0
        elif added == 1 and 1 - self.slots[-1].pressure < self.overlap:
            slot = self._make_buffer()
            self.slots.append(slot)
            return added + slot.add(item, force)
        assert added <= 2, "Added too many slots"
        return added

    def append(self, buf: Buffer[T]):
        self.slots.append(buf)

    @property
    def contents(self) -> List[T]:
        return [x.content for x in self.slots]


# Convenience factory for string windowing (preserves original behavior)
def string_windower(capacity: int, delimiter: str, overlap: float = 0.05) -> GenericWindower[str]:
    return GenericWindower(
        capacity=capacity,
        combine=lambda a, b: a + delimiter + b if a else b,
        length=len,
        empty="",
        overlap=overlap,
        combine_cost=len(delimiter)
    )


# Example: List windower for chunking sequences
def list_windower(capacity: int, item_length_fn: Callable[[Any], int], overlap: float = 0.05) -> GenericWindower[List[Any]]:
    return GenericWindower(
        capacity=capacity,
        combine=lambda a, b: a + b,
        length=lambda lst: sum(item_length_fn(x) for x in lst),
        empty=[],
        overlap=overlap,
        combine_cost=0
    )


def chunk_string(text: str, token_limit: int, text_len: int, token_len: int, overlap: float = 0.05):
    # String chunking (like before)
    char_limit = int(token_limit * text_len / token_len)
    outer_windower = string_windower(capacity=char_limit, delimiter="\n", overlap=overlap)
    for line in text.split("\n"):
        if not outer_windower.add(line):
            inner_windower = string_windower(capacity=char_limit, delimiter=" ", overlap=overlap)
            for word in line.split(" "):
                inner_windower.add(word, force=True)
            for chunk in inner_windower.slots:
                chunk.close()
                outer_windower.append(chunk)
    return outer_windower.contents


def chunk_list(documents, token_limit: int, text_len: int, token_len: int, overlap: float = 0.05, item_length_fn: Callable[[Any], int] = len):
    # List chunking (e.g., for documents with metadata)
    char_limit = int(token_limit * text_len / token_len)
    outer_windower = list_windower(capacity=char_limit, item_length_fn=item_length_fn, overlap=overlap)
    for doc in documents:
        outer_windower.add([doc])
    return outer_windower.contents