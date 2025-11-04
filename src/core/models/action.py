from enum import Enum


class Action(Enum):
    READ = "read"
    WRITE = "write"

    def __str__(self):
        return self.value
