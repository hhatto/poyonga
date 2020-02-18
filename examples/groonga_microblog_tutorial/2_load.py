from poyonga.client import Groonga


def _call(g, cmd, **kwargs):
    ret = g.call(cmd, **kwargs)
    print(ret.status)
    print(ret.body)
    if cmd == "select":
        for item in ret.items:
            print(item)
        print("=*=" * 30)


g = Groonga()

users = """\
[
  {
    "_key": "daijiro",
    "name": "hsiomaneki",
    "follower": ["tasukuchan"],
    "favorites": [],
    "location": "127678039x502643091",
    "location_str": "神奈川県",
    "description": "groonga developer"
  },
  {
    "_key": "tasukuchan",
    "name": "グニャラくん",
    "follower": ["daijiro","OffGao"],
    "favorites": ["daijiro:1","OffGao:1"],
    "location": "128423343x502929252",
    "location_str": "東京都渋谷区",
    "description": "エロいおっさん"
  },
  {
    "_key": "OffGao",
    "name": "OffGao",
    "follower": ["tasukuchan","daijiro"],
    "favorites": ["tasukuchan:1","daijiro:1"],
    "location": "128544408x502801502",
    "location_str": "東京都中野区",
    "description": "がおがお"
  }
]
"""


_call(g, "load", table="Users", values="".join(users.splitlines()))
_call(g, "select", table="Users")


comments = """\
[
  {
    "_key": "daijiro:1",
    "comment": "マイクロブログ作ってみました（甘栗むいちゃいました的な感じで）。",
    "last_modified": "2010/03/17 12:05:00",
    "posted_by": "daijiro",
  },
  {
    "_key": "tasukuchan:1",
    "comment": "初の書き込み。テストテスト。",
    "last_modified": "2010/03/17 12:00:00",
    "posted_by": "tasukuchan",
  },
  {
    "_key": "daijiro:2",
    "comment": "@tasukuchan ようこそ!!!",
    "last_modified": "2010/03/17 12:05:00",
    "replied_to": "tasukuchan:1",
    "replied_users": ["tasukuchan"],
    "posted_by": "daijiro",
  },
  {
    "_key": "tasukuchan:2",
    "comment": "@daijiro ありがとう！",
    "last_modified": "2010/03/17 13:00:00",
    "replied_to": "daijiro:2",
    "replied_users": ["daijiro"],
    "posted_by": "tasukuchan",
  },
  {
    "_key": "tasukuchan:3",
    "comment": "groongaなう #groonga",
    "last_modified": "2010/03/17 14:00:00",
    "hash_tags": ["groonga"],
    "location": "127972422x503117107",
    "posted_by": "tasukuchan",
  },
  {
    "_key": "tasukuchan:4",
    "comment": "groonga開発合宿のため羽田空港に来ました！ #groonga #travel",
    "last_modified": "2010/03/17 14:05:00",
    "hash_tags": ["groonga", "travel"],
    "location": "127975798x502919856",
    "posted_by": "tasukuchan",
  },
  {
    "_key": "OffGao:1",
    "comment": "@daijiro @tasukuchan 登録してみましたよー！",
    "last_modified": "2010/03/17 15:00:00",
    "replied_users": ["daijiro", "tasukuchan"],
    "location": "128551935x502796433",
    "posted_by": "OffGao",
  }
  {
    "_key": "OffGao:2",
    "comment": "中野ブロードウェイなうなう",
    "last_modified": "2010/03/17 15:05:00",
    "location": "128551935x502796434",
    "posted_by": "OffGao",
  }
]
"""
_call(g, "load", table="Comments", values="".join(comments.splitlines()))
_call(g, "select", table="Comments")
