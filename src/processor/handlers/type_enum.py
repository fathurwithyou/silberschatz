from enum import Enum

class QueryTypeEnum(Enum):
    DDL = "ddl"
    DML = "dml"
    TCL = "tcl"
    
    def __str__(self):
        return self.value
    