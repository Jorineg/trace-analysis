import pandas as pd
import json
import hashlib
import numpy as np


def insert_parsed_row(lineage_row_data, trace_id, database):
    if not "representation" in lineage_row_data:
        if "patch_id" in lineage_row_data:
            database.current_dedup_patch = lineage_row_data["patch_id"]
            return
        if "patch_end" in lineage_row_data:
            database.current_dedup_patch = None
            return
        if lineage_row_data == {}:
            print("Empty row data!")
            raise

    input_items = None
    representation = lineage_row_data["representation"]
    item_type = lineage_row_data["type"]

    if item_type in "ID":
        input_ids = representation["inputs"]
        input_items = [database.trace_item_lookup[input] for input in input_ids]

    value_hash = get_value_hash(lineage_row_data, input_items)
    lineage_hash = get_lineage_hash(lineage_row_data, input_items)

    if item_type in "ID":
        lineage_data = [
            [input_item["value_hash"], value_hash] for input_item in input_items  # type: ignore
        ]
        database.lineage_buffer.extend(lineage_data)

    type_map = {
        "L": "LITERAL",
        "C": "CREATION",
        "I": "INSTRUCTION",
        "D": "DEDUP",
    }

    trace_item = {
        "id": lineage_row_data["id"],
        "trace_id": trace_id,
        "type": type_map[lineage_row_data["type"]],
        "value_hash": value_hash,
        "lineage_hash": lineage_hash,
        "dedup_patch_name": database.current_dedup_patch,
        "mem_size": pd.NA,
        "execution_time": pd.Timedelta(np.random.randint(10, 1000), unit="ms"),
    }

    database.trace_item_buffer.append(trace_item)
    database.trace_item_lookup[trace_item["id"]] = trace_item

    if item_type == "I":
        insert_parsed_instruction(representation, value_hash, database)
    elif item_type == "D":
        insert_parsed_dedup(representation, value_hash, database)
    elif item_type == "L":
        insert_parsed_literal(representation, value_hash, database)
    elif item_type == "C":
        insert_parsed_creation(representation, value_hash, database)
    else:
        raise Exception("Invalid item type")


def insert_parsed_instruction(representation, value_hash, database):
    instruction_item = {
        "value_hash": value_hash,
        "execution_type": np.random.choice(
            ["CP", "CP_FILE", "SPARK", "GPU", "FED"], p=[0.9, 0.01, 0.04, 0.05, 0]
        ),
    } | representation
    database.instruction_buffer.append(instruction_item)


def insert_parsed_dedup(representation, value_hash, database):
    representation.pop("inputs")
    database.dedup_buffer.append(representation | {"value_hash": value_hash})


def to_bool(x):
    if x == "true":
        return True
    elif x == "false":
        return False
    else:
        raise Exception("Invalid boolean value")


def insert_parsed_literal(representation, value_hash, database):
    representation["flag"] = to_bool(representation["flag"])
    database.literal_buffer.append(representation | {"value_hash": value_hash})


def insert_parsed_creation(representation, value_hash, database):
    params = representation.pop("params", None)
    database.creation_buffer.append(representation | {"value_hash": value_hash})
    if "creation_method" not in representation:
        return
    if representation["creation_method"] == "rand":
        pdf = {k: v for d in params["other_params"] for k, v in d.items()}["pdf"]
        params["other_params"].remove({"pdf": pdf})
        database.rand_creation_buffer.append(
            params | {"value_hash": value_hash, "pdf": pdf}
        )
    elif representation["creation_method"] == "createvar":
        params["file_overwrite"] = to_bool(params["file_overwrite"])
        database.createvar_creation_buffer.append(params | {"value_hash": value_hash})
    elif representation["creation_method"] == "seq":
        database.seq_creation_buffer.append(params | {"value_hash": value_hash})


def get_value_hash(lineage_row_data, inputs):
    item_type = lineage_row_data["type"]
    representation = lineage_row_data["representation"]
    str_to_hash = ""

    if item_type in ["C", "L"]:
        str_to_hash = json.dumps(representation)
    elif item_type == "I":
        str_to_hash = "".join([input["value_hash"] for input in inputs])
        str_to_hash += representation["op_code"]
        str_to_hash += representation.get("special_value_bits", "")
    elif item_type == "D":
        str_to_hash = "".join([input["value_hash"] for input in inputs])
        str_to_hash += representation["dedup_name"]
    else:
        raise Exception("Invalid item type")

    return hashlib.sha256(str_to_hash.encode()).hexdigest()


def get_lineage_hash(lineage_row_data, inputs):
    item_type = lineage_row_data["type"]
    representation = lineage_row_data["representation"]
    str_to_hash = ""

    if item_type == "L":
        str_to_hash = item_type
    elif item_type == "C":
        str_to_hash = item_type
        str_to_hash += representation["creation_method"]
    elif item_type == "I":
        str_to_hash = "".join([input["lineage_hash"] for input in inputs])
        str_to_hash += representation["op_code"]
        # to do: decide if we want to include special_value_bits in lineage_hash
        # document this decision
    elif item_type == "D":
        str_to_hash = "".join([input["lineage_hash"] for input in inputs])
        str_to_hash += representation["dedup_name"]
    else:
        raise Exception("Invalid item type")

    return hashlib.sha256(str_to_hash.encode()).hexdigest()
