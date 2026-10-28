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
    def download(self, rate, fail=False, url='https://test/asset'):
        content = b'archive\x00' * 20000  # Includes a partial final 64 KiB block.
        clock = Clock()

        def serve(request):
            self.assertEqual(request.headers['accept-encoding'], 'identity')
            if request.url.host == 'static.wikia.nocookie.net':
                self.assertEqual(request.url.params['cb'], '123')
                self.assertEqual(request.url.params.get_list('format'), ['original'])
            return httpx.Response(200, stream=AssetStream(content, fail))

        with tempfile.TemporaryDirectory() as directory:
            dest = Path(directory) / 'asset.bin'
            with httpx.Client(transport=httpx.MockTransport(serve)) as client:
                with patch.object(fandom.time, 'monotonic', clock.monotonic), \
                     patch.object(fandom.time, 'sleep', clock.sleep):
                    if fail:
                        with self.assertRaises(httpx.ReadError):
                            fandom._download_file(client, url, dest, rate)
                        self.assertFalse(dest.exists())
                        self.assertTrue(dest.with_suffix('.bin.part').exists())
                        return
                    count = fandom._download_file(client, url, dest, rate)
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

    def test_cdn_request_preserves_original_format_and_cache_timestamp(self):
        self.download(0, url='https://static.wikia.nocookie.net/wiki/asset?cb=123&format=webp')

    def test_filename_with_slash_stays_in_media_directory(self):
        media_dir = Path('/archive/media')
        dest = fandom._destination_for_entry(media_dir, {'name': 'Smile/Mimi.jpeg'})
        self.assertEqual(dest.parent, media_dir)
        self.assertEqual(dest.name, 'Smile_Mimi.jpeg')

    def test_permanent_asset_errors_are_not_retried(self):
        for status in [400, 404, 410]:
            with self.subTest(status=status):
                requests = []
                def serve(request):
                    requests.append(request)
                    return httpx.Response(status)
                with tempfile.TemporaryDirectory() as directory, \
                     httpx.Client(transport=httpx.MockTransport(serve)) as client, \
                     patch.object(fandom.time, 'sleep') as sleep:
                    with self.assertRaises(fandom.DownloadUnavailableError) as error:
                        fandom._download_with_backoff(client, 'https://test/asset',
                                                      Path(directory) / 'asset.bin')
                    self.assertEqual(error.exception.status, status)
                    self.assertEqual(len(requests), 1)
                    sleep.assert_not_called()

    def test_temporary_server_error_is_retried(self):
        requests = []
        def serve(request):
            requests.append(request)
            if len(requests) == 1:
                return httpx.Response(503)
            return httpx.Response(200, stream=AssetStream(b'recovered'))
        with tempfile.TemporaryDirectory() as directory, \
             httpx.Client(transport=httpx.MockTransport(serve)) as client, \
             patch.object(fandom.time, 'sleep') as sleep:
            dest = Path(directory) / 'asset.bin'
            self.assertEqual(fandom._download_with_backoff(client, 'https://test/asset', dest), 9)
            self.assertEqual(dest.read_bytes(), b'recovered')
            self.assertEqual(len(requests), 2)
            sleep.assert_called_once_with(5.0)


if __name__ == '__main__':
    unittest.main()
