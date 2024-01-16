import sys
from pprint import pprint

sys.path.append("./src")

from TraceLoader import load_directory
from QueryInterface import QueryInterface
import os
import pandas as pd


def load_database():
    database = load_directory("./src/tests/traces")
    return database


db = load_database()


def test_loaded_all_traces():
    assert len(db.trace) == 2


def test_trace_names():
    only_trace1 = db.trace.loc[db.trace.name == "test1.lineage"]
    only_trace2 = db.trace.loc[db.trace.name == "test2.lineage"]
    assert len(only_trace1) == 1
    assert len(only_trace2) == 1


def test_trace_date():
    # set modification date of file to now
    os.utime("./src/tests/traces/test1.lineage", None)
    db = load_database()
    trace1 = db.trace.loc[db.trace.name == "test1.lineage"]
    assert (
        pd.Timedelta(seconds=-1)
        < (trace1.date[0] - pd.Timestamp.now("UTC"))
        < pd.Timedelta(seconds=1)
    )


def test_trace_item_length():
    assert len(db.trace_item) == 18


def test_unique_index():
    assert len(db.instruction) == 8
    assert len(db.dedup) == 2
    assert len(db.creation) == 3
    assert len(db.literal) == 2
    assert len(db.createvar_creation) == 1
    assert len(db.seq_creation) == 1
    assert len(db.rand_creation) == 1
    assert len(db.lineage) == 21


def test_instrucion():
    inst = db.instruction.iloc[0]
    assert inst["op_code"] == "rightIndex"
    assert inst["special_value_bits"] == 1


def test_creation():
    creation = db.creation.loc[db.trace_item.iloc[0]["value_hash"]]
    assert creation["execution_type"] == "CP"
    assert creation["creation_method"] == "createvar"
    assert pd.isna(creation["dedup_in"])


def test_createvar_creation():
    create_var = db.createvar_creation.loc[db.trace_item.iloc[0]["value_hash"]]
    assert create_var["function"] == "pREADxxx"
    assert (
        create_var["file_name"]
        == "target/testTemp/functions/lineage/FullReusePerfTest/in/X"
    )
    assert create_var["file_overwrite"] == False
    assert create_var["data_type"] == "MATRIX"
    assert create_var["format"] == "text"
    other_params = [
        {"value": "2000"},
        {"value": "128"},
        {"value": "-1"},
        {"value": "-1"},
        {"value": "copy"},
    ]
    assert create_var["other_params"] == other_params


def test_rand_creation():
    rand = db.rand_creation.loc[db.trace_item.iloc[2]["value_hash"]]
    assert rand["pdf"] == "uniform"
    other_params = [
        {"value": "6400", "data_type": "SCALAR", "value_type": "INT64", "flag": "true"},
        {"value": "784", "data_type": "SCALAR", "value_type": "INT64", "flag": "true"},
        {"value": "1000"},
        {"value": "0"},
        {"value": "20"},
        {"value": "1.0"},
        {"value": "42"},
        {"value": "1.0"},
        {"value": "8"},
        {"value": "xxx·MATRIX·FP64"},
    ]
    assert rand["other_params"] == other_params


def test_seq_creation():
    seq = db.seq_creation.loc[db.trace_item.iloc[1]["value_hash"]]
    other_params = [
        {"value": "10"},
        {"value": "1"},
        {"value": "1000"},
        {"value": "1", "data_type": "SCALAR", "value_type": "INT64", "flag": "true"},
        {"value": "10", "data_type": "SCALAR", "value_type": "INT64", "flag": "true"},
        {"value": "1", "data_type": "SCALAR", "value_type": "INT64", "flag": "true"},
        {"value": "xxx·MATRIX·FP64"},
    ]
    assert seq["other_params"] == other_params


def test_literal():
    literal = db.literal.loc[db.trace_item.iloc[3]["value_hash"]]
    assert literal["value"] == "1"
    assert literal["data_type"] == "SCALAR"
    assert literal["value_type"] == "INT64"
    assert literal["flag"] == True


def test_lineage():
    lineage = db.lineage.reset_index().join(
        db.trace_item.reset_index().set_index("value_hash"),
        on="is_input_for_value_hash",
    )
    lineage = lineage.loc[lineage["trace_id"] == 0][
        ["value_hash", "is_input_for_value_hash"]
    ]
    find_hash = lambda x: db.trace_item.loc[(0, x), "value_hash"]
    lineage_as_ids = [
        (7, 22),
        (7, 4074),
        (7, 10000),
        (12, 22),
        (12, 4074),
        (22, 4074),
        (4074, 10000),
        (10000, 10001),
    ]
    lineage_as_hashes = [(find_hash(x), find_hash(y)) for x, y in lineage_as_ids]
    is_lineage = set(tuple(i.values) for _, i in lineage.iterrows())
    correct_lineage = set(lineage_as_hashes)
    assert is_lineage == correct_lineage


def test_dedup():
    dedup = db.dedup.loc[db.trace_item.iloc[4]["value_hash"]]
    assert dedup["dedup_name"] == "dedup_X_SB515_3"


def test_trace_item():
    items_trace_1 = db.trace_item.reset_index().join(db.trace, on="trace_id")
    items_trace_1 = items_trace_1.loc[items_trace_1.name == "test1.lineage"]
    assert items_trace_1.iloc[0]["id"] == 7
    assert items_trace_1.iloc[0]["type"] == "CREATION"
    assert pd.isna(items_trace_1.iloc[0]["dedup_patch_name"])
    assert items_trace_1.iloc[3]["id"] == 12
    assert items_trace_1.iloc[3]["type"] == "LITERAL"
    assert pd.isna(items_trace_1.iloc[3]["dedup_patch_name"])
    assert items_trace_1.iloc[4]["id"] == 22
    assert items_trace_1.iloc[4]["type"] == "DEDUP"
    assert pd.isna(items_trace_1.iloc[4]["dedup_patch_name"])
    assert items_trace_1.iloc[5]["id"] == 4074
    assert items_trace_1.iloc[5]["type"] == "INSTRUCTION"
    assert pd.isna(items_trace_1.iloc[5]["dedup_patch_name"])

    for _, item in db.trace_item.iterrows():
        assert pd.isna(item["mem_size"])
        assert item["execution_time"] >= pd.Timedelta(seconds=0.01)
        assert item["execution_time"] <= pd.Timedelta(seconds=1)


def test_trace_item_unique_hashes():
    def assert_unique_hashes(x):
        assert len(x.value_hash.unique()) == len(x)
        assert len(x.lineage_hash.unique()) == len(x)
        return 0

    db.trace_item.reset_index().groupby("trace_id").apply(assert_unique_hashes)


qi = QueryInterface(db)


def test_compare_total_operations():
    ops = qi.compare_total_operations()
    print(ops)
    assert len(ops) == 2
    assert ops.loc[0, "count"] == 8
    assert ops.loc[1, "count"] == 10


def test_select_operator():
    df = db.trace_item.join(db.instruction, on="value_hash").join(
        db.op_info, on="op_code"
    )
    assert len(qi.select_operator(df)) == 18
    assert len(qi.select_operator(df, type="CREATION")) == 6
    assert len(qi.select_operator(df, type="LITERAL")) == 2
    assert len(qi.select_operator(df, type="DEDUP")) == 2
    assert len(qi.select_operator(df, type="INSTRUCTION")) == 8
    assert len(qi.select_operator(df, op_code="rightIndex")) == 2
    assert len(qi.select_operator(df, op_code="leftIndex")) == 0
    assert len(qi.select_operator(df, group="Arithmetic")) == 4
    assert len(qi.select_operator(df, cp_type="Binary")) == 5


def test_find_trace_long_operation():
    assert len(qi.find_trace_long_operation(min_time_ms=10)) == 2
    assert len(qi.find_trace_long_operation(min_time_ms=1000)) == 0
    ops = qi.find_trace_long_operation(min_time_ms=500)
    for _, op in ops.iterrows():
        assert op["execution_time"] >= pd.Timedelta(milliseconds=500)


def test_compare_instruction_count():
    assert len(qi.compare_instruction_count()) == 2
    assert qi.compare_instruction_count().loc[0, "item_count"] == 8
    assert qi.compare_instruction_count().loc[1, "item_count"] == 10
    assert qi.compare_instruction_count(type="INSTRUCTION").loc[0, "item_count"] == 3
    assert qi.compare_instruction_count(type="INSTRUCTION").loc[1, "item_count"] == 5


def test_list_execution_types():
    df = qi.list_execution_types()
    assert len(df) == 2
    for idx, row in df.iterrows():
        total_time = db.instruction.join(
            db.trace_item.reset_index().set_index("value_hash")
        )
        total_time = total_time.loc[total_time["trace_id"] == idx][
            "execution_time"
        ].sum()

        row = pd.to_timedelta(row, unit="ms")
        assert row.sum() == total_time


def test_compare_traces_by_id():
    assert qi.compare_traces_by_id(0, 0) == None
    assert qi.compare_traces_by_id(0, 1) == 6
    assert qi.compare_traces_by_id(1, 0) == 6
    assert qi.compare_traces_by_id(0, 1, compare_by="value") == 3
    assert qi.compare_traces_by_id(1, 0, compare_by="value") == 3
