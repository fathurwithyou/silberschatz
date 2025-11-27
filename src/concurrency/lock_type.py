from enum import Enum


class LockType(Enum):
    SHARED = "S"
    EXCLUSIVE = "X"

    def __str__(self):
        return self.value
