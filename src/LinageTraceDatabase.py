import pandas as pd


class LineageTraceDatabase:
    trace_schema = {
        # index
        "id": "int",
        "date": "datetime64[ns, UTC]",
        "total_execution_time": "timedelta64[ns]",
        "file": "string",
        "name": "string",
        "description": "string",
    }

    instruction_schema = {
        # index
        "value_hash": "string",
        "op_code": "string",
        "special_value_bits": "Int64",
        "execution_type": pd.CategoricalDtype(
            categories=["CP", "CP_FILE", "SPARK", "GPU", "FED"]
        ),
    }

    dedup_schema = {
        # index
        "value_hash": "string",
        "dedup_name": "string",
    }

    creation_schema = {
        # index
        "value_hash": "string",
        "execution_type": pd.CategoricalDtype(
            categories=["CP", "CP_FILE", "SPARK", "GPU", "FED"]
        ),
        "creation_method": pd.CategoricalDtype(
            categories=["rand", "createvar", "seq", "in"]
        ),
        "dedup_in": "Int64",
    }

    rand_creation_schema = {
        # index
        "value_hash": "string",
        # "rows": "int",
        # "cols": "int",
        # "blen": "int",
        # "sparsity": "float",
        # "seed": "int",
        "pdf": pd.CategoricalDtype(categories=["uniform", "normal", "poisson"]),
        "other_params": "object",
        # "k": "int",
    }

    createvar_creation_schema = {
        # index
        "value_hash": "string",
        "function": "string",
        "file_name": "string",
        "file_overwrite": "boolean",
        "data_type": pd.CategoricalDtype(
            categories=["SCALAR", "FRAME", "MATRIX", "LIST"]
        ),
        "format": pd.CategoricalDtype(
            categories=["csv", "libsvm", "hdf5", "text", "other"]
        ),
        "other_params": "object",
    }

    seq_creation_schema = {
        # index
        "value_hash": "string",
        "other_params": "object",
    }

    literal_schema = {
        # index
        "value_hash": "string",
        "value": "string",
        # to do: check if these are the right types
        "data_type": pd.CategoricalDtype(
            categories=["SCALAR", "FRAME", "MATRIX", "LIST"]
        ),
        "value_type": pd.CategoricalDtype(
            categories=["INT64", "FP64", "STRING", "BOOLEAN"]
        ),
        "flag": "boolean",
    }

    lineage_schema = {
        # both index
        "value_hash": "string",
        "is_input_for_value_hash": "string",
    }

    trace_item_schema = {
        # both are index
        "trace_id": "int",
        "id": "int",
        "type": pd.CategoricalDtype(
            categories=["INSTRUCTION", "CREATION", "LITERAL", "DEDUP"]
        ),
        "value_hash": "string",
        "lineage_hash": "string",
        "dedup_patch_name": "string",
        "mem_size": "Int64",
        "execution_time": "timedelta64[ns]",
    }

    op_info_schema = {
        # index
        "op_code": "string",
        "num_inputs": "Int64",
        "group": "string",
        "cp_type": "string",
    }

    def __init__(self):
        self.trace_buffer = []
        self.instruction_buffer = []
        self.dedup_buffer = []
        self.creation_buffer = []
        self.rand_creation_buffer = []
        self.createvar_creation_buffer = []
        self.seq_creation_buffer = []
        self.literal_buffer = []
        self.lineage_buffer = []
        self.trace_item_buffer = []

        self.trace_item_lookup = {}

        self.current_dedup_patch = None

    def to_pandas(self):
        self.trace = (
            pd.DataFrame.from_records(
                self.trace_buffer, columns=self.trace_schema.keys()
            )
            .astype(self.trace_schema)
            .set_index("id")
        )
        self.instruction = (
            pd.DataFrame.from_records(
                self.instruction_buffer, columns=self.instruction_schema.keys()
            )
            .astype(self.instruction_schema)
            .set_index("value_hash")
        )
        self.instruction = self.instruction[~self.instruction.index.duplicated()]
        self.dedup = (
            pd.DataFrame.from_records(
                self.dedup_buffer, columns=self.dedup_schema.keys()
            )
            .astype(self.dedup_schema)
            .set_index("value_hash")
        )
        self.dedup = self.dedup[~self.dedup.index.duplicated()]
        self.creation = (
            pd.DataFrame.from_records(
                self.creation_buffer, columns=self.creation_schema.keys()
            )
            .astype(self.creation_schema)
            .set_index("value_hash")
        )
        self.creation = self.creation[~self.creation.index.duplicated()]
        self.rand_creation = (
            pd.DataFrame.from_records(
                self.rand_creation_buffer, columns=self.rand_creation_schema.keys()
            )
            .astype(self.rand_creation_schema)
            .set_index("value_hash")
        )
        self.rand_creation = self.rand_creation[~self.rand_creation.index.duplicated()]
        self.createvar_creation = (
            pd.DataFrame.from_records(
                self.createvar_creation_buffer,
                columns=self.createvar_creation_schema.keys(),
            )
            .astype(self.createvar_creation_schema)
            .set_index("value_hash")
        )
        self.createvar_creation = self.createvar_creation[
            ~self.createvar_creation.index.duplicated()
        ]
        self.seq_creation = (
            pd.DataFrame.from_records(
                self.seq_creation_buffer, columns=self.seq_creation_schema.keys()
            )
            .astype(self.seq_creation_schema)
            .set_index("value_hash")
        )
        self.seq_creation = self.seq_creation[~self.seq_creation.index.duplicated()]
        self.literal = (
            pd.DataFrame.from_records(
                self.literal_buffer, columns=self.literal_schema.keys()
            )
            .astype(self.literal_schema)
            .set_index("value_hash")
        )
        self.literal = self.literal[~self.literal.index.duplicated()]
        self.lineage = (
            pd.DataFrame.from_records(
                self.lineage_buffer, columns=self.lineage_schema.keys()
            )
            .astype(self.lineage_schema)
            .set_index(["value_hash", "is_input_for_value_hash"])
        )
        self.lineage = self.lineage[~self.lineage.index.duplicated()]
        self.trace_item = (
            pd.DataFrame.from_records(
                self.trace_item_buffer, columns=self.trace_item_schema.keys()
            )
            .astype(self.trace_item_schema)
            .set_index(["trace_id", "id"])
        )
        self.op_info = pd.read_csv(
            "op_info.csv", dtype=self.op_info_schema, sep=";"
        ).set_index("op_code")

        self.__init__()
