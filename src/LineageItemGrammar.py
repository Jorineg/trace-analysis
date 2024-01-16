from pyparsing import (
    Word,
    Combine,
    printables,
    nums,
    Literal,
    Optional,
    Suppress,
    Group,
    Regex,
)

import pyparsing as pp

pp.ParserElement.enable_packrat()

id = Word(nums)
id_item = "(" + id("id") + ")"
id_as_input = Suppress("(") + id + Suppress(")")

l_item_identifier = "(" + Literal("L")("type") + ")"
c_item_identifier = "(" + Literal("C")("type") + ")"
i_item_identifier = "(" + Literal("I")("type") + ")"
d_item_identifier = "(" + Literal("D")("type") + ")"

op_code = Word(printables)("op_code")

special_value_bits = Word(nums)("special_value_bits")
special_value_bits_item = "[" + special_value_bits + "]"

l_placeholder = Literal("IN#") + Word(nums)("dudup_in")
inputs = id_as_input[1, ...]("inputs")

execution_type = (
    Literal("CP")
    | Literal("CP_FILE")
    | Literal("SPARK")
    | Literal("GPU")
    | Literal("FED")
)("execution_type")

any_value = Word(printables + " ", excludeChars="°·")

l_data_type = Literal("SCALAR") | Literal("FRAME") | Literal("MATRIX") | Literal("LIST")
l_value_type = (
    Literal("INT64") | Literal("FP64") | Literal("STRING") | Literal("BOOLEAN")
)
flag = Literal("true") | Literal("false")
l_content = (
    any_value("value")
    + "·"
    + l_data_type("data_type")
    + "·"
    + l_value_type("value_type")
    + "·"
    + flag("flag")
)

int_value = Word(nums)("value")
float_value = Combine(
    Optional("-")
    + Word(nums)
    + "."
    + Word(nums)
    + Optional((Literal("E") | Literal("e")) + Optional("-") + Word(nums))
)("value")
distribution = Literal("uniform") | Literal("normal") | Literal("poisson")

any_non_circle = Word(printables + "·", excludeChars="°")

c_content_any_param = Group(
    l_content | float_value | int_value | distribution("pdf") | any_non_circle("value")
)

c_content_zero_or_more_params = (Suppress("°") + c_content_any_param)[...](
    "other_params"
)
c_content_rand = (
    execution_type
    + "°"
    + Literal("rand")("creation_method")
    + Group(c_content_zero_or_more_params)("params")
)
c_createvar_format = (
    Literal("csv")
    | Literal("libsvm")
    | Literal("hdf5")
    | Literal("text")
    | any_non_circle
)
c_content_create_var = (
    execution_type
    + "°"
    + Literal("createvar")("creation_method")
    + Group(
        "°"
        + any_non_circle("function")
        + "°"
        + any_non_circle("file_name")
        + "°"
        + (Literal("true") | Literal("false"))("file_overwrite")
        + "°"
        + l_data_type("data_type")
        + "°"
        + c_createvar_format("format")
        + c_content_zero_or_more_params
    )("params")
)
c_content_seq = (
    execution_type
    + "°"
    + Literal("seq")("creation_method")
    + Group(c_content_zero_or_more_params)("params")
)
c_content = l_placeholder | c_content_rand | c_content_create_var | c_content_seq

i_content = op_code + inputs + Optional(special_value_bits_item)

dedup_name = Word(printables)("dedup_name")
d_content = dedup_name + inputs

l_item = l_item_identifier + Group(l_content)("representation")
c_item = c_item_identifier + Group(c_content)("representation")
i_item = i_item_identifier + Group(i_content)("representation")
d_item = d_item_identifier + Group(d_content)("representation")

item = l_item | c_item | i_item | d_item
dedup_patch_start = Literal("patch_") + Word(printables)("patch_id")
patch_end = Regex(r"^\n?$").set_whitespace_chars("")("patch_end")
trace_record = (id_item + item) | dedup_patch_start | patch_end
