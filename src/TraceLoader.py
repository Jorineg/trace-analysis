from LineageItemGrammar import trace_record
import pyparsing as pp

import pathlib
import pandas as pd
from ItemLoader import insert_parsed_row
from LinageTraceDatabase import LineageTraceDatabase
import time


def parse_linage_rows(filename):
    with open(filename, "r", encoding="utf-8") as lineage_trace_file:
        l = 0
        for line_num, line in enumerate(lineage_trace_file):
            l = line_num
            try:
                parsed_data = trace_record.parse_string(line)
            except pp.ParseException as e:
                print("Parsed line " + repr(line))
                raise Exception(
                    "Invalid input on line " + str(line_num) + ": " + str(e)
                )
            yield parsed_data.as_dict()


def load_trace(path_to_file, database):
    # print("Loading trace from " + str(path_to_file))
    last_modified = pd.to_datetime(
        pathlib.Path(path_to_file).stat().st_mtime, unit="s"
    ).tz_localize("UTC")

    new_id = len(database.trace_buffer)

    trace_item = {
        "id": new_id,
        "date": last_modified,
        "file": path_to_file,
        "total_execution_time": pd.Timedelta(0),
        "name": pathlib.Path(path_to_file).name,
        "description": "",
    }

    database.trace_buffer.append(trace_item)

    database.current_dedup_patch = None
    for idx, parsed_data in enumerate(list(parse_linage_rows(path_to_file))):
        # print(parsed_data)
        try:
            insert_parsed_row(parsed_data, new_id, database)
        except KeyError as e:
            print(f"Error on line {idx+1} of file '{trace_item['name']}': {e}")
            print(parsed_data)
            raise
    return database


def load_directory(path_to_dir, database=None):
    """
    Loads a directory containing lineage traces into Database object.

    Parameters
    ----------
    path_to_dir : str
        str to directory containing traces.
        Can also contain other files and subdirectorys.
    database : LineageTraceDatabase, default=None
        Already loaded database to add new traces to.

    Returns
    -------
    LineageTraceDatabase
        Database object containing information of all loaded traces as pandas dataframes.

    """
    if database is None:
        database = LineageTraceDatabase()

    # suffixes = [".lineage", ".dedup"]
    suffixes = [".lineage"]
    trace_files = [
        path_to_file
        for path_to_file in pathlib.Path(path_to_dir).rglob("*")
        if path_to_file.suffix in suffixes
    ]

    for path_to_file in trace_files:
        load_trace(path_to_file, database)

    # print("building dataframes from buffers")
    database.to_pandas()

    return database
