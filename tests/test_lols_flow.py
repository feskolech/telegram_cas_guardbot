import os
import tempfile
import unittest

from app.db import DB
from app.handlers import check_user
from app.lols import LolsClient, LolsUnavailable


class FakeLocalDB:
    def __init__(self, flagged_ids=None):
        self.flagged_ids = set(flagged_ids or [])

    def contains(self, user_id: int) -> bool:
        return user_id in self.flagged_ids


class FakeLolsClient:
    def __init__(self, result=False, error=None):
        self.result = result
        self.error = error
        self.calls = []
        self.logged = []

    async def is_banned(self, user_id: int) -> bool:
        self.calls.append(user_id)
        if self.error:
            raise self.error
        return self.result

    def should_log_failure(self, message: str, interval_sec: int = 60) -> bool:
        self.logged.append((message, interval_sec))
        return True


class FakeCasClient:
    def __init__(self, result=False, error=None):
        self.result = result
        self.error = error
        self.calls = []
        self.logged = []

    async def is_banned(self, user_id: int) -> bool:
        self.calls.append(user_id)
        if self.error:
            raise self.error
        return self.result

    def should_log_failure(self, message: str, interval_sec: int = 60) -> bool:
        self.logged.append((message, interval_sec))
        return True


class FakeResponse:
    def __init__(self, status: int, payload):
        self.status = status
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self, content_type=None):
        return self.payload

    async def text(self):
        return str(self.payload)


class FakeSession:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.urls = []

    def get(self, url, timeout=None):
        self.urls.append(url)
        return self.response


class CheckUserTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        self.db = DB(self.db_path)
        await self.db.open()

    async def asyncTearDown(self):
        await self.db.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    async def test_export_blacklist_short_circuits_api_checks(self):
        local_db = FakeLocalDB({42})
        lols = FakeLolsClient(result=False)
        cas = FakeCasClient(result=False)

        flagged, reason, source = await check_user(100, 42, local_db, lols, cas, self.db, 3600, 600)

        self.assertTrue(flagged)
        self.assertEqual(reason, "CAS export blacklist")
        self.assertEqual(source, "export")
        self.assertEqual(lols.calls, [])
        self.assertEqual(cas.calls, [])

    async def test_lols_cache_is_shared_across_chats(self):
        await self.db.set_lols_cache(77, True)
        local_db = FakeLocalDB()
        lols = FakeLolsClient(result=False)
        cas = FakeCasClient(result=False)

        flagged, reason, source = await check_user(100, 77, local_db, lols, cas, self.db, 3600, 600)

        self.assertTrue(flagged)
        self.assertEqual(reason, "lols.bot API (record found)")
        self.assertEqual(source, "lols")
        self.assertEqual(lols.calls, [])
        self.assertEqual(cas.calls, [])

    async def test_cas_is_used_when_lols_fails(self):
        local_db = FakeLocalDB()
        lols = FakeLolsClient(error=RuntimeError("boom"))
        cas = FakeCasClient(result=True)

        flagged, reason, source = await check_user(100, 99, local_db, lols, cas, self.db, 3600, 600)

        self.assertTrue(flagged)
        self.assertEqual(reason, "CAS API (record found)")
        self.assertEqual(source, "cas")
        self.assertEqual(lols.calls, [99])
        self.assertEqual(cas.calls, [99])


class LolsClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_is_banned_reads_banned_field(self):
        client = LolsClient(FakeSession(FakeResponse(200, {"ok": True, "user_id": 1, "banned": True})))

        result = await client.is_banned(1)

        self.assertTrue(result)

    async def test_is_banned_rejects_ok_false(self):
        client = LolsClient(FakeSession(FakeResponse(200, {"ok": False, "user_id": 1, "banned": False})))

        with self.assertRaises(LolsUnavailable):
            await client.is_banned(1)


if __name__ == "__main__":
    unittest.main()
