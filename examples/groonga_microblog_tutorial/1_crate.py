from poyonga.client import Groonga
import json

g = Groonga(host="192.168.24.72")


def _call(g, cmd, **kwargs):
    ret = g.call(cmd, **kwargs)
    print ret.status
    print ret.body
    return ret

_call(g, "table_create", name="Users", flags="TABLE_HASH_KEY", key_type="ShortText")
_call(g, "table_create", name="Comments", flags="TABLE_HASH_KEY", key_type="ShortText")
_call(g, "table_create", name="HashTags", flags="TABLE_HASH_KEY", key_type="ShortText")
_call(g, "table_create", name="Bigram", flags="TABLE_PAT_KEY|KEY_NORMALIZE", key_type="ShortText", default_tokenizer="TokenBigram")

_call(g, "column_create", table="Users", name="name", flags="COLUMN_SCALAR", type="ShortText")
_call(g, "column_create", table="Users", name="follower", flags="COLUMN_VECTOR", type="Users")
_call(g, "column_create", table="Users", name="favorites", flags="COLUMN_VECTOR", type="Comments")
_call(g, "column_create", table="Users", name="location", flags="COLUMN_SCALAR", type="WGS84GeoPoint")
_call(g, "column_create", table="Users", name="location_str", flags="COLUMN_SCALAR", type="ShortText")
_call(g, "column_create", table="Users", name="description", flags="COLUMN_SCALAR", type="ShortText")
_call(g, "column_create", table="Users", name="followee", flags="COLUMN_INDEX", type="Users", source="follower")

_call(g, "column_create", table="Comments", name="comment", flags="COLUMN_SCALAR", type="ShortText")
_call(g, "column_create", table="Comments", name="last_modified", flags="COLUMN_SCALAR", type="Time")
_call(g, "column_create", table="Comments", name="replied_to", flags="COLUMN_SCALAR", type="Comments")
_call(g, "column_create", table="Comments", name="replied_users", flags="COLUMN_VECTOR", type="Users")
_call(g, "column_create", table="Comments", name="hash_tags", flags="COLUMN_VECTOR", type="HashTags")
_call(g, "column_create", table="Comments", name="location", flags="COLUMN_SCALAR", type="WGS84GeoPoint")
_call(g, "column_create", table="Comments", name="posted_by", flags="COLUMN_SCALAR", type="Users")
_call(g, "column_create", table="Comments", name="favorited_by", flags="COLUMN_INDEX", type="Users", source="favorites")

_call(g, "column_create", table="HashTags", name="hash_index", flags="COLUMN_INDEX", type="Comments", source="hash_tags")

_call(g, "column_create", table="Bigram", name="users_index", flags="COLUMN_INDEX|WITH_POSITION|WITH_SECTION", type="Users", source="name,location_str,description")
_call(g, "column_create", table="Bigram", name="comment_index", flags="COLUMN_INDEX|WITH_POSITION", type="Comments", source="comment")
