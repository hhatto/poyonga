from poyonga.client import Groonga
import pyarrow as pa


def _call(g, cmd, **kwargs):
    ret = g.call(cmd, **kwargs)
    print(ret.status)
    print(ret.body)
    if cmd == "select":
        for item in ret.items:
            print(item)
        print("=*=" * 30)


def load_and_select(table, data, batch):
    # use Apache Arrow IPC Streaming Format
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    buf = sink.getvalue()
    values = buf.to_pybytes()

    _call(g, "load", table=table, values=values, input_type="apache-arrow")
    _call(g, "select", table=table)


g = Groonga()

users = [
    pa.array(["daijiro", "tasukuchan", "OffGao"]),
    pa.array(["hsiomaneki", "グニャラくん", "OffGao"]),
    pa.array([["tasukuchan"], ["daijiro", "OffGao"], ["tasukuchan", "daijiro"]]),
    pa.array([[], ["daijiro:1", "OffGao:1"], ["tasukuchan:1", "daijiro:1"]]),
    pa.array(["127678039x502643091", "128423343x502929252", "128544408x502801502"]),
    pa.array(["神奈川県", "東京都渋谷区", "東京都中野区"]),
    pa.array(["groonga developer", "エロいおっさん", "がおがお"]),
]
users_batch = pa.record_batch(
    users, names=["_key", "name", "follower", "favorites", "location", "location_str", "description"]
)
load_and_select("Users", users, users_batch)


comments = [
    pa.array(
        [
            "daijiro:1",
            "tasukuchan:1",
            "daijiro:2",
            "tasukuchan:2",
            "tasukuchan:3",
            "tasukuchan:4",
            "OffGao:1",
            "OffGao:2",
        ]
    ),
    pa.array(
        [
            "マイクロブログ作ってみました（甘栗むいちゃいました的な感じで）。",
            "初の書き込み。テストテスト。",
            "@tasukuchan ようこそ!!!",
            "@daijiro ありがとう！",
            "groongaなう #groonga",
            "groonga開発合宿のため羽田空港に来ました！ #groonga #travel",
            "@daijiro @tasukuchan 登録してみましたよー！",
            "中野ブロードウェイなうなう",
        ]
    ),
    pa.array(
        [
            "2010/03/17 12:05:00",
            "2010/03/17 12:00:00",
            "2010/03/17 12:05:00",
            "2010/03/17 13:00:00",
            "2010/03/17 14:00:00",
            "2010/03/17 14:05:00",
            "2010/03/17 15:00:00",
            "2010/03/17 15:05:00",
        ]
    ),
    pa.array([None, None, "tasukuchan:1", "daijiro:2", None, None, None, None]),
    pa.array([None, None, ["tasukuchan"], ["daijiro"], None, None, ["daijiro", "tasukuchan"], None]),
    pa.array([None, None, None, None, ["groonga"], ["groonga", "travel"], None, None]),
    pa.array(
        [
            None,
            None,
            None,
            None,
            "127972422x503117107",
            "127975798x502919856",
            "128551935x502796433",
            "128551935x502796434",
        ]
    ),
    pa.array(["daijiro", "tasukuchan", "daijiro", "tasukuchan", "tasukuchan", "tasukuchan", "OffGao", "OffGao"]),
]
comments_batch = pa.record_batch(
    comments,
    names=["_key", "comment", "last_modified", "replied_to", "replied_users", "hash_tags", "location", "posted_by"],
)
load_and_select("Comments", comments, comments_batch)
