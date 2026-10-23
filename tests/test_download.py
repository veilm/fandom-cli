import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

import fandom


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class AssetStream(httpx.SyncByteStream):
    def __init__(self, content, fail=False):
        self.content = content
        self.fail = fail

    def __iter__(self):
        yield self.content
        if self.fail:
            raise httpx.ReadError('interrupted')


class DownloadTests(unittest.TestCase):
    def download(self, rate, fail=False):
        content = b'archive\x00' * 20000  # Includes a partial final 64 KiB block.
        clock = Clock()

        def serve(request):
            self.assertEqual(request.headers['accept-encoding'], 'identity')
            return httpx.Response(200, stream=AssetStream(content, fail))

        with tempfile.TemporaryDirectory() as directory:
            dest = Path(directory) / 'asset.bin'
            with httpx.Client(transport=httpx.MockTransport(serve)) as client:
                with patch.object(fandom.time, 'monotonic', clock.monotonic), \
                     patch.object(fandom.time, 'sleep', clock.sleep):
                    if fail:
                        with self.assertRaises(httpx.ReadError):
                            fandom._download_file(client, 'https://test/asset', dest, rate)
                        self.assertFalse(dest.exists())
                        self.assertTrue(dest.with_suffix('.bin.part').exists())
                        return
                    count = fandom._download_file(client, 'https://test/asset', dest, rate)
            self.assertEqual(count, len(content))
            self.assertEqual(dest.read_bytes(), content)
            self.assertFalse(dest.with_suffix('.bin.part').exists())
        return len(content), clock.now

    def test_rate_limit_includes_final_partial_block(self):
        count, elapsed = self.download(500000)
        self.assertAlmostEqual(elapsed, count / 500000)

    def test_unlimited_does_not_sleep(self):
        _, elapsed = self.download(0)
        self.assertEqual(elapsed, 0)

    def test_interrupted_file_is_not_marked_complete(self):
        self.download(500000, fail=True)


if __name__ == '__main__':
    unittest.main()
