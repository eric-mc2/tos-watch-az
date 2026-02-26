from enum import Enum, auto, IntEnum
from typing import List, Protocol, Any, Callable, Iterator, TypeVar, Generic, Optional

T = TypeVar('T')

class AddResult(IntEnum):
    NOT_ADDED = auto()
    SINGLE_ADDED = auto()
    MULTI_ADDED = auto()
    FORCE_ADDED = auto()

    def __or__(self, value):
        if not isinstance(value, AddResult):
            raise TypeError(f"Expected {AddResult}, got {type(value)}")
        if self == AddResult.SINGLE_ADDED and value == AddResult.SINGLE_ADDED:
            return AddResult.MULTI_ADDED
        else:
            return max(self, value)

class Buffer(Generic[T]):
    """A generic buffer class for accumulating items."""

    def __init__(
        self,
        capacity: int,
        combine: Callable[[T, T], T],
        length: Callable[[T], int],
        empty: T,
        combine_cost: int = 0,  # delimiter overhead for non-empty additions
        overhead: T = None
    ):
        self._items: Optional[T] = None
        self._size = 0
        self.capacity = capacity
        self.combine = combine
        self.combine_cost = combine_cost
        self.length = length
        self.empty = empty
        self.overhead = overhead
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
            overhead_size = self.length(self.overhead) if self.overhead is not None else 0
            new_size = self.length(item) + overhead_size
        else:
            new_size = self._size + self.combine_cost + self.length(item)
        return new_size <= self.capacity

    def add(self, item: T, force: bool = False) -> AddResult:
        if not force and not self.can_add(item):
            self.close()
            return AddResult.NOT_ADDED
        forced = not self.can_add(item)
        if self.is_empty or self._items is None:
            self._items = self.overhead + item if self.overhead else item  # don't use combine
            self._size += self.length(item)
        else:
            self._items = self.combine(self._items, item)
            self._size += self.length(item) + self.combine_cost
        return AddResult.FORCE_ADDED if forced else AddResult.SINGLE_ADDED


class GenericWindower(Generic[T]):
    """Utility to segment a sequence of items into overlapping chunks."""

    def __init__(
        self,
        capacity: int,
        combine: Callable[[T, T], T],
        length: Callable[[T], int],
        empty: T,
        overlap: float = 0.05,
        combine_cost: int = 0,
        overhead: T = None
    ):
        self.slots: List[Buffer[T]] = []
        self.overlap = overlap
        self.capacity = capacity
        self.combine = combine
        self.combine_cost = combine_cost
        self.length_fn = length
        self.empty = empty
        self.overhead = overhead
        if capacity <= 0:
            raise ValueError("Capacity must be positive")
        if overlap < 0 or overlap >= 1:
            raise ValueError("Overlap must be in range (0,1).")

    def _make_buffer(self) -> Buffer[T]:
        return Buffer(self.capacity, self.combine, self.length_fn, self.empty, self.combine_cost, self.overhead)

    def add(self, item: T, force: bool = False) -> AddResult:
        added = AddResult.NOT_ADDED
        for slot in self.slots:
            added |= slot.add(item, force)
        if added == AddResult.NOT_ADDED:
            slot = self._make_buffer()
            self.slots.append(slot)
            if slot.can_add(item):
                # Add and leave room for other stuff.
                slot.add(item, force)
                added = AddResult.SINGLE_ADDED
            else:
                # We don't want to drop data, so put it in its own buffer
                # and allow downstream processing to break it up.
                slot.add(item, True)
                slot.close()
                added = AddResult.FORCE_ADDED
        elif added == AddResult.SINGLE_ADDED and 1 - self.slots[-1].pressure < self.overlap:
            slot = self._make_buffer()
            self.slots.append(slot)
            added = slot.add(item, force)
        return added

    def append(self, buf: Buffer[T]):
        self.slots.append(buf)

    @property
    def contents(self) -> List[T]:
        return [x.content for x in self.slots]

    def pop(self) -> T:
        if len(self.slots) == 0:
            return self.empty
        buf = self.slots.pop()
        return buf.content


# Convenience factory for string windowing (preserves original behavior)
def string_windower(capacity: int, delimiter: str, overlap: float = 0.05, overhead: str = "") -> GenericWindower[str]:
    return GenericWindower(
        capacity=capacity,
        combine=lambda a, b: a + delimiter + b if a else b,
        length=len,
        empty="",
        overlap=overlap,
        combine_cost=len(delimiter),
        overhead=overhead
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


def chunk_string(text: str, char_limit: int, overlap: float = 0.05, overhead: str = "") -> List[str]:
    # String chunking (like before)
    outer_windower = string_windower(capacity=char_limit, delimiter="\n", overlap=overlap, overhead=overhead)
    for line in text.split("\n"):
        added = outer_windower.add(line)
        if added == AddResult.FORCE_ADDED:
            line = outer_windower.pop()
            line = line.removeprefix(overhead) # if overhead has spaces, remove so we don't double it.
        if added == AddResult.NOT_ADDED or added == AddResult.FORCE_ADDED:
            inner_windower = string_windower(capacity=char_limit, delimiter=" ", overlap=overlap, overhead=overhead)
            for word in line.split(" "):
                inner_windower.add(word)
            for slot in inner_windower.slots:
                slot.close()
                outer_windower.append(slot)
    return outer_windower.contents


def chunk_list(documents, char_limit: int, overlap: float = 0.05, item_length_fn: Callable[[Any], int] = len):
    # List chunking (e.g., for documents with metadata)
    outer_windower = list_windower(capacity=char_limit, item_length_fn=item_length_fn, overlap=overlap)
    for doc in documents:
        outer_windower.add([doc])
    return outer_windower.contents