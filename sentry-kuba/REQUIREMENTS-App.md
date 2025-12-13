Requirements for Dune tools

1. Use kuba.App.scala for adding all tools functionality
1.1 Config must have "dune.api.key" parameter
2. Add 'dune-fetch' command to fetch all Query results from Dune API
2.1 Command must have 1 parameter: queryId
3. Dune fetch must first request query results size to understand how many pages (dune limit page size to 1000 rows)
4. In the loop fetch query results by pages size of 1000 in csv format
5. Write results in stream fashion to "--output" param file (default is output.csv) (append mode)
6. When finished, returnd number of lines written
