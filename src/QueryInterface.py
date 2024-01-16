import pandas as pd


class QueryInterface:
    """
    A class used to Query the database of loaded lineage traces.

    Attributes
    ----------
    database : Database Object
        Object of the database to be queried

    Methods
    -------
    compare_total_operations()
        Compares the total operations in the traces.

    select_operator(df, op_code=None, group=None, cp_type=None)
        Returns a dataframe where row selection is based on op_code, group or cp_type.

    find_trace_long_operation(min_time_ms = 20)
        Finds long operations within the traces with a minimum execution time.

    compare_instruction_count()
        Compare instruction count in traces.

    list_exectution_types()
        List total execution duration of different execution types present in trace items.

    compare_traces_by_id(id_trace1, id_trace2, compare_by="lineage")
        Compare two different traces by id.

    compare_traces_by_date(date1, date2, compare_by="lineage")
        Compare two different traces by their dates.
    """

    def __init__(self, database):
        """
        Parameters
        ----------
        database : Database Object
            The database containing all traces
        """
        self.database = database

    def compare_total_operations(self):
        """
        Compare the total operations per trace by grouping all trace_items based on "trace_id",
        sorting the counts in descending order and then join with the trace table.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with the comparison of total operations per trace.
        """

        return (
            self.database.trace_item.groupby("trace_id")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
            .join(self.database.trace)
            .drop(columns=["name", "description", "total_execution_time"])
            .set_index("trace_id")
        )

    def select_operator(self, df, type=None, op_code=None, group=None, cp_type=None):
        """
        Select rows by op_code, group or cp_type from a DataFrame.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to select rows from.
            Must include columns 'op_code', 'group' and 'cp_type'

        type : str, default=None
            One of 'INSTRUCTION', 'DEDUP', 'LITERAL', 'CREATION'
            Filters by type of item.
            Should not be used with op_code, group or cp_type.

        op_code : int, default=None
            Operator code to be selected.
            Implies type='INSTRUCTION'.
            Should not be used with type.

        group : str, default=None
            Group name to be selected.
            Implies type='INSTRUCTION'.
            Should not be used with type.

        cp_type : str, default=None
            Compute type (?) to be selected.
            Implies type='INSTRUCTION'.
            Should not be used with type.

        Raises
        ------
        RuntimeError
            If multiple operator selectors are provided.

        Returns
        -------
        pandas.DataFrame
            DataFrame with selected operator.
        """
        if type is not None:
            if op_code is not None or group is not None or cp_type is not None:
                raise RuntimeError("multiple operator selectors not possible")
            if type not in ["INSTRUCTION", "DEDUP", "LITERAL", "CREATION"]:
                raise RuntimeError("invalid type")
            df = df[df.type == type]
        if (op_code and group) or (op_code and cp_type) or (group and cp_type):
            raise RuntimeError("multiple operator selectors not possible")
        if op_code is not None:
            df = df[df.op_code == op_code]
        if group is not None:
            df = df[df.group == group]
        if cp_type is not None:
            df = df[df.cp_type == cp_type]
        return df

    def find_trace_long_operation(self, **kwargs):
        """
        Find traces with long operations. A long operation is defined with a time greater than minimum time specified.

        Parameters
        ----------
        min_time_ms : int, default=20
            Minimum time in milliseconds to classify an operation as long.

        type : str, default=None
            One of 'INSTRUCTION', 'DEDUP', 'LITERAL', 'CREATION'
            Filters by type of item.
            Should not be used with op_code, group or cp_type.

        op_code : str, default=None
            select only operations with this op_code. Only one of op_code, group and cp_type shall be set.

        group : str, default=None
            select only operations belonging to this group. Only one of op_code, group and cp_type shall be set.

        cp_type : str, default=None
            select only operations with this cp type. Only one of op_code, group and cp_type shall be set.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing traces that contain at least one long running operation.
        """
        min_time_ms = kwargs.pop("min_time_ms", 20)
        trace_item = (
            self.database.trace_item.join(self.database.trace, on="trace_id")
            .join(self.database.instruction, on="value_hash")
            .join(self.database.op_info, on="op_code")
        )
        trace_item = self.select_operator(trace_item, **kwargs)
        trace_item = trace_item[
            trace_item.execution_time > pd.Timedelta(min_time_ms, unit="ms")
        ]

        def f(x):
            d = {}
            idx_max_time = x["execution_time"].idxmax()
            d["execution_time"] = x["execution_time"][idx_max_time]
            d["id"] = idx_max_time[1]
            d["op_code"] = x["op_code"][idx_max_time]
            return pd.Series(d, index=["id", "op_code", "execution_time"])

        long_running_traces = (
            trace_item[["op_code", "execution_time"]]
            .groupby("trace_id", group_keys=False)
            .apply(f)
        )
        return long_running_traces

    def compare_instruction_count(self, **kwargs):
        """
        Compare instruction count in each trace.

        Parameters
        ----------

        type : str, default=None
            One of 'INSTRUCTION', 'DEDUP', 'LITERAL', 'CREATION'
            Filters by type of item.
            Should not be used with op_code, group or cp_type.

        op_code : str, default=None
            select only operations with this op_code. Only one of op_code, group and cp_type shall be set.

        group : str, default=None
            select only operations belonging to this group. Only one of op_code, group and cp_type shall be set.

        cp_type : str, default=None
            select only operations with this cp type. Only one of op_code, group and cp_type shall be set.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing total item counts for each trace for the specified item type.
        """
        trace_items = (
            self.database.trace_item.join(self.database.trace, on="trace_id")
            .join(self.database.instruction, on="value_hash")
            .join(self.database.op_info, on="op_code")
        )
        trace_items = self.select_operator(trace_items, **kwargs)
        operator_count = trace_items.groupby("trace_id").size().to_frame("item_count")
        return operator_count

    def list_execution_types(self):
        """
        Show total execution time per execution type and trace.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing total time with execution types as columns and traces as index.
        """
        execution_types = (
            self.database.trace_item.join(self.database.trace, on="trace_id")
            .join(self.database.instruction, on="value_hash")[
                ["execution_type", "execution_time"]
            ]
            .groupby(["trace_id", "execution_type"])
            .sum()
            .reset_index()
            .pivot(index="trace_id", columns="execution_type", values="execution_time")
        )
        return execution_types

    def compare_traces_by_id(self, id_trace1, id_trace2, compare_by="lineage"):
        """
        Compare two traces by their ids and return unequal index.

        Parameters
        ----------
        id_trace1, id_trace2 : int
            IDs of traces to be compared.
        compare_by : str, optional, default="lineage"
            Attribute to compare traces by, either 'lineage' or 'value'.

        Raises
        ------
        RuntimeError
            If compare_by is neither 'lineage' nor 'value'.

        Returns
        -------
        int
            The first unequal index (iloc! not label). Returns None if traces are equal.
        """
        trace1 = self.database.trace_item.loc[id_trace1].reset_index()
        trace2 = self.database.trace_item.loc[id_trace2].reset_index()
        if compare_by == "lineage":
            compare_column = "lineage_hash"
        elif compare_by == "value":
            compare_column = "value_hash"
        else:
            raise RuntimeError("compare_by must be either 'lineage' or 'value'")
        min_len = min(len(trace1), len(trace2))
        is_equal = list(
            trace1[compare_column][:min_len] == trace2[compare_column][:min_len]
        )
        is_equal += [False] * (max(len(trace1), len(trace2)) - min_len)
        try:
            first_unequal_index = list(is_equal).index(False)
        except ValueError:
            return None
        return first_unequal_index

    def compare_traces_by_date(self, date1, date2, compare_by="lineage"):
        """
        Compare two traces by their dates and return unequal index.

        Parameters
        ----------
        date1, date2 : str
            Dates of traces to be compared.
        compare_by : str
            Attribute to compare traces by.

        Raises
        ------
        RuntimeError
            If compare_by is neither 'lineage' nor 'value'.
        ValueEroor
            If no trace found with date1 or date2

        Returns
        ------
        int
            The first unequal index. Returns None if traces are equal.
        """

        try:
            id1 = list(self.database.trace.date).index(date1)
            id2 = list(self.database.trace.date).index(date2)
        except ValueError:
            raise RuntimeError("no trace found for date found!")

        return self.compare_traces_by_id(id1, id2, compare_by=compare_by)
