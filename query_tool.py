#!/usr/bin/python3
# query_tool.py
# Tool to run multiple simultaneous queries on PGSQL
# author: David Iserovich

import argparse
import statistics
import sys
import time
import textwrap
from multiprocessing import Process, Queue

import psycopg2

CSV_COLS = ['hostname', 'start_time', 'end_time']

QUERY = """
    SELECT
        time_bucket('1 minute', ts) AS minute,
        max(usage),
        min(usage)
    FROM cpu_usage
    WHERE ts>%s and ts<%s and host=%s
    GROUP BY minute
"""

def query_worker(q_in, q_out, conn_string):
    with psycopg2.connect(conn_string) as conn:

        # Warm up the connection
        with conn.cursor() as cur:
            cur.execute("""SELECT 1""")
            cur.fetchall()

        while True:
            item = q_in.get()
            if item == "end":
                return

            try:
                time, _ = run_query(conn, item)
                q_out.put(time)
            except:
                print('Warning: query error on input: ', item, file=sys.stderr)
                q_out.put('err')

                # Roll back the transaction to prevent errors from
                # affecting subsequent queries
                conn.rollback()


def run_query(conn, params):
    with conn.cursor() as cur:
        start = time.time()
        cur.execute(QUERY, (
            params['start_time'], params['end_time'], params['hostname']
        ))

        result = cur.fetchall()

        end = time.time()
        return end - start, result

def parse_input_file(f):
    # Read the column names
    # Clean up the column names a bit to handle whitespace/case
    cols = [c.strip().lower() for c in f.readline().split(',')]

    for col in CSV_COLS:
        if col not in cols:
            print('Error: Expected columns', ', '.join(CSV_COLS), ' in csv first line', file=sys.stderr)
            exit(1)

    for line in f.readlines():
        # Read each line into a dict by column name
        result = {cols[i]: val.strip() for i, val in enumerate(line.split(',')[:len(cols)])}
        yield result

def main():
    parser = argparse.ArgumentParser(description=textwrap.dedent("""
        Profile cpu usage queries against a Timescale database. Input must be a csv file with columns hostname, start_time, end_time
    """))
    parser.add_argument('num_workers', type=int, help='Number of parallel query workers to start')
    parser.add_argument('connection_string', help='Connection string to postgres db (specify db name, username, etc.')
    parser.add_argument('input_file', help='File to configure queries')
    parser.add_argument('-a', '--additional-stats', action='store_true', help='Show additional stats')
    args = parser.parse_args()

    # Output queue. In order to facilitate calculating the median of all queries run,
    # this contains query time results from all the workers, instead of
    # having each worker calculate statistics on its own stream and
    # aggregating them
    q_out = Queue()

    workers = []

    start = time.time()
    for i in range(args.num_workers):
        q_in = Queue()
        p = Process(target=query_worker, args=(q_in, q_out, args.connection_string))
        p.start()
        workers.append({'proc': p, 'queue': q_in})

    with open(args.input_file, 'r') as f:
        for line in parse_input_file(f):
            worker_idx = hash(line['hostname']) % len(workers)
            #print(worker_idx)
            worker_q = workers[worker_idx]['queue']
            worker_q.put(line)

    for worker in workers:
        # Block until all workers have completed their requests
        worker['queue'].put('end')
        worker['proc'].join()

    times = []
    errors = 0
    while not q_out.empty():
        item = q_out.get_nowait()
        if item == 'err':
            errors += 1
        else:
            times.append(item)

    end = time.time()

    if args.additional_stats:
        print('Total runtime (seconds)', end - start)

    if errors:
        print("There were", errors, "query errors", file=sys.stderr)

    print('Successfully ran', len(times), ' queries')
    print('Query runtime statistics (ms)\n----')
    print('\t'.join(['Avg', 'Median', 'Min', 'Max', 'Total']))
    print('\t'.join([
        f'{x*1000:.2f}' for x in (
            statistics.mean(times),
            statistics.median(times),
            min(times),
            max(times),
            sum(times)
        )
    ]))


if __name__ == "__main__":
    main()
