from sqlalchemy import select

from plugins.metadata.models import Streams


def test_stream(get_session):
    with get_session() as session:
        rs = session.execute(select(Streams)).scalars().all()
    print(rs)


def test_stream_session(session):
    rs = session.execute(select(Streams)).scalars().all()
    print(rs)
