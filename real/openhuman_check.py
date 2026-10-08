"""Сверка OpenHuman: сколько сообщений агент получил как записанные и сколько лежит в sessions.db."""
import glob, os, sqlite3


def check(d, agents):
    dbs = glob.glob(os.path.join(d, "data", "**", "sessions.db"), recursive=True)
    if not dbs:
        return dict(integrity="no-db", acked=sum(a["acked"] for a in agents), missing=sum(a["acked"] for a in agents), stale=0)
    con = sqlite3.connect(dbs[0])
    try:
        integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
        have = dict(con.execute("SELECT session_id, COUNT(*) FROM session_messages GROUP BY session_id"))
    except sqlite3.DatabaseError as e:
        # как у OpenResearch: база на папке мака битая целиком (7.10) —
        # точка — порча, подтверждённое считается потерянным
        integrity, have = f"unreadable: {e}", {}
    con.close()
    acked = sum(a["acked"] for a in agents)
    missing = sum(max(0, a["acked"] - have.get(a["sid"], 0)) for a in agents)
    return dict(integrity=integrity, acked=acked, missing=missing, stale=0)
