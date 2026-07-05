"""Analytics chatbot: natural-language questions over the lakehouse.

Claude (with tool use) answers business questions by querying the semantic
layer's governed metrics or, for detail questions, running guarded read-only
SQL through the DuckDB lakehouse engine. Grounding comes from the same data
contracts that generate the data dictionary.
"""
