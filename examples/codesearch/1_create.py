from poyonga import Groonga

grn = Groonga()
ret = grn.call("table_create", name="Files", flags="TABLE_HASH_KEY", key_type="ShortText")
print(ret.status)
ret = grn.call(
    "table_create",
    name="Bigram",
    key_type="ShortText",
    flags="TABLE_PAT_KEY|KEY_NORMALIZE",
    default_tokenizer="TokenBigramSplitSymbolAlphaDigit",
)
print(ret.status)

ret = grn.call("column_create", table="Files", name="ext", flags="COLUMN_SCALAR", type="ShortText")
print(ret.status)
ret = grn.call("column_create", table="Files", name="name", flags="COLUMN_SCALAR", type="ShortText")
print(ret.status)
ret = grn.call(
    "column_create", table="Files", name="content", flags="COLUMN_SCALAR", type="ShortText"
)
print(ret.status)

ret = grn.call(
    "column_create",
    table="Bigram",
    name="content_index",
    flags="COLUMN_INDEX|WITH_POSITION",
    type="Files",
    source="content",
)
print(ret.status)
