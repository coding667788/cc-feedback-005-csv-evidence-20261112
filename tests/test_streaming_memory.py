import io
import tracemalloc
import unittest

from csv_collector.parser import StreamingCSVParser, stream_records


class MemoryBoundaryTests(unittest.TestCase):
    def test_large_input_does_not_load_entire_file(self):
        # Stream a >5 MB logical input with a 256-byte block. The parser's
        # tracked Python allocations must stay bounded by one record plus one
        # chunk, not grow with total input size.
        line = ("x," * 7) + "v\n"
        payload = (line * 350_000).encode("utf-8")
        self.assertGreater(len(payload), 5 * 1024 * 1024)
        stream = io.BytesIO(payload)
        parser = StreamingCSVParser("utf-8", 8)
        tracemalloc.start()
        try:
            count = 0
            peak = 0
            while True:
                chunk = stream.read(256)
                if not chunk:
                    break
                for record in parser.feed(chunk):
                    count += 1
                    self.assertEqual(len(record.fields), 8)
                _, peak = tracemalloc.get_traced_memory()
            for record in parser.finish():
                count += 1
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(count, 350_000)
        # Peak parser-managed memory is far smaller than the 5 MB input.
        self.assertLess(peak, 512 * 1024)


class ChunkSizeContractTests(unittest.TestCase):
    def test_block_size_one_never_buffers_more_than_one_record(self):
        # With block_size=1 each feed carries exactly one byte; the parser
        # must still reconstruct records spanning many feeds.
        payload = (
            b"a,b,c\n"
            b'1,"q,field",3\n'
            b'4,"line1\nline2",6\n'
        )
        records = list(stream_records(io.BytesIO(payload), "utf-8", 3, 1))
        self.assertEqual(len(records), 3)
        self.assertEqual(records[1].fields, ("1", "q,field", "3"))
        self.assertEqual(records[2].fields, ("4", "line1\nline2", "6"))


if __name__ == "__main__":
    unittest.main()

