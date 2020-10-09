Query Benchmark Tool
====
Tool to run multiple simultaneous queries on PGSQL


Installation
--

Please install Python 3.6+ and psycopg2

psycopg2 can most easily be installed with:

   pip install psycopg2-binary

inside a virtualenv or globally.


Running
--

Please run `python profiler.py --help` for detailed usage information.

```
$ python query_tool.py --help
usage: query_tool.py [-h] [-a] num_workers connection_string input_file

Profile cpu usage queries against a Timescale database. Input must be a csv
file with columns hostname, start_time, end_time

positional arguments:
  num_workers           Number of parallel query workers to start
  connection_string     Connection string to postgres db (specify db name,
                        username, etc.
  input_file            File to configure queries

optional arguments:
  -h, --help            show this help message and exit
  -a, --additional-stats
                        Show additional stats
```

Example usage:

    python query_tool.py -a 16 'dbname=homework' params.csv
    Warning: query error on input:  {'hostname': 'hostname', 'start_time': 'start_time', 'end_time': 'end_time'}
    Warning: query error on input:  {'hostname': 'hostname', 'start_time': 'start_time', 'end_time': 'end_time'}
    Warning: query error on input:  {'hostname': 'hostname', 'start_time': 'start_time', 'end_time': 'end_time'}
    Warning: query error on input:  {'hostname': 'hostname', 'start_time': 'start_time', 'end_time': 'end_time'}
    Total runtime (seconds) 0.829479455947876
    There were 4 query errors
    Successfully ran 1000  queries
    Query runtime statistics (ms)
    ----
    Avg	Median	Min	Max	Total
    3.68	3.45	2.24	28.29	3684.91


    
