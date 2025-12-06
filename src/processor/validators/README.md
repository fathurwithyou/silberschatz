# SQL Grammar Rules

## Core Statement Types

```bnf
<statement> ::= <select_statement> | <insert_statement> | <update_statement> |
               <delete_statement> | <create_statement> | <drop_statement> |
               <begin_statement> | <commit_statement> | <abort_statement>
```

## SELECT Statement

```bnf
<select_statement> ::= SELECT <select_list> FROM <from_clause>
                      [ <where_clause> ] [ <order_by_clause> ] [ <limit_clause> ]

<select_list> ::= '*' | <expression> [ AS IDENTIFIER ] { ',' <expression> [ AS IDENTIFIER ] }

<from_clause> ::= <join_expression> { ',' <join_expression> }

<join_expression> ::= <table_reference> { <join_clause> }

<table_reference> ::= IDENTIFIER [ [ AS ] IDENTIFIER ]

<join_clause> ::= [ INNER | LEFT [ OUTER ] | RIGHT [ OUTER ] | FULL OUTER ]
                 JOIN <table_reference> [ ON <expression> ]

<where_clause> ::= WHERE <expression>

<order_by_clause> ::= ORDER BY <expression> [ ASC | DESC ] { ',' <expression> [ ASC | DESC ] }

<limit_clause> ::= LIMIT NUMBER_LITERAL
```

## INSERT Statement

```bnf
<insert_statement> ::= INSERT INTO IDENTIFIER [ '(' <column_list> ')' ]
                      VALUES '(' <value_list> ')'

<column_list> ::= IDENTIFIER { ',' IDENTIFIER }

<value_list> ::= <factor> { ',' <factor> }
```

## UPDATE Statement

```bnf
<update_statement> ::= UPDATE IDENTIFIER SET <assignment_list> [ <where_clause> ]

<assignment_list> ::= <assignment> { ',' <assignment> }

<assignment> ::= IDENTIFIER '=' <expression>
```

## DELETE Statement

```bnf
<delete_statement> ::= DELETE FROM IDENTIFIER [ <where_clause> ]
```

## DDL Statements

```bnf
<create_statement> ::= CREATE TABLE IDENTIFIER '(' <column_definition_list> ')'

<column_definition_list> ::= <column_definition> { ',' <column_definition> }

<column_definition> ::= IDENTIFIER <data_type> [<column_constraints>]

<data_type> ::= INTEGER | VARCHAR '(' NUMBER ')' | CHAR '(' NUMBER ')' | FLOAT | INT

<column_constraints> ::= {<constraint>}

<constraint> ::= PRIMARY KEY | NOT NULL | NULL | REFERENCES <table_ref> [<fk_actions>]

<table_ref> ::= IDENTIFIER '(' IDENTIFIER ')'

<fk_actions> ::= [ON DELETE <action>] [ON UPDATE <action>]

<action> ::= CASCADE | RESTRICT | SET NULL | NO ACTION

<drop_statement> ::= DROP TABLE IDENTIFIER
```

## Transaction Control

```bnf
<begin_statement> ::= BEGIN TRANSACTION

<commit_statement> ::= COMMIT

<abort_statement> ::= ABORT
```

## Expressions and Terms

```bnf
<expression> ::= <term> { ( AND | OR ) <term> }

<term> ::= [ NOT ] <factor> [ <comparison_operator> <factor> ]

<factor> ::= IDENTIFIER [ '.' IDENTIFIER ] | STRING_LITERAL | NUMBER_LITERAL |
            NULL | '(' <expression> ')' | '*'

<comparison_operator> ::= '=' | '!=' | '<' | '>' | '<=' | '>='
```

## Notes

- `[ ]` denotes optional elements
- `{ }` denotes zero or more repetitions
- `|` denotes alternatives
- Terminal symbols are shown in UPPERCASE or quoted literals
