import io
import unittest

from csv_collector.errors import ParseError
from csv_collector.parser import stream_records


def parse_all(data: bytes, encoding: str, columns: int, block_size: int):
    return list(
        stream_records(io.BytesIO(data), encoding, columns, block_size)
    )


class ParserBlockSizeTests(unittest.TestCase):
    TEXT = (
        "uid,note,extra\r\n"
        'A1,"含,逗号",普通\r\n'
        'A2,"转""引号",\r\n'
        'A3,"多行\r\n第二行",x\r\n'
        "A4,纯文本,\r\n"
    )

    def _cases(self, encoding):
        raw_enc = "utf-8" if encoding == "utf-8-sig" else encoding
        raw = self.TEXT.encode(raw_enc)
        if encoding == "utf-8-sig":
            raw = b"\xef\xbb\xbf" + raw
        return raw

    def test_utf8_sig_block_sizes_1_to_7(self):
        raw = self._cases("utf-8-sig")
        baseline = parse_all(raw, "utf-8-sig", 3, 65536)
        for block_size in range(1, 8):
            with self.subTest(block_size=block_size):
                records = parse_all(raw, "utf-8-sig", 3, block_size)
                self.assertEqual(
                    [r.fields for r in records],
                    [r.fields for r in baseline],
                )
                self._assert_offsets(records, len(raw) - 3)  # BOM excluded

    def test_gb18030_block_sizes_1_to_7(self):
        raw = self._cases("gb18030")
        baseline = parse_all(raw, "gb18030", 3, 65536)
        for block_size in range(1, 8):
            with self.subTest(block_size=block_size):
                records = parse_all(raw, "gb18030", 3, block_size)
                self.assertEqual(
                    [r.fields for r in records],
                    [r.fields for r in baseline],
                )
                self._assert_offsets(records, len(raw))

    def _assert_offsets(self, records, total_size):
        self.assertEqual(records[0].start_offset, 0)
        for previous, current in zip(records, records[1:]):
            self.assertEqual(previous.end_offset, current.start_offset)
        self.assertEqual(records[-1].end_offset, total_size)
        for record in records:
            self.assertEqual(
                record.end_offset - record.start_offset, len(record.raw)
            )

    def test_specific_field_values(self):
        raw = self._cases("utf-8-sig")
        records = parse_all(raw, "utf-8-sig", 3, 1)
        data = records[1:]
        self.assertEqual(data[0].fields, ("A1", "含,逗号", "普通"))
        self.assertEqual(data[1].fields, ("A2", '转"引号', ""))
        self.assertEqual(data[2].fields, ("A3", "多行\r\n第二行", "x"))
        self.assertEqual(data[3].fields, ("A4", "纯文本", ""))


class MultibyteBoundaryTests(unittest.TestCase):
    def test_utf8_multibyte_split_across_every_boundary(self):
        text = "uid,city\nX1,北京上海\nX2,深圳杭州\n"
        raw = b"\xef\xbb\xbf" + text.encode("utf-8")
        baseline = [r.fields for r in parse_all(raw, "utf-8-sig", 2, 999)]
        for block_size in range(1, 10):
            records = parse_all(raw, "utf-8-sig", 2, block_size)
            self.assertEqual(
                [r.fields for r in records], baseline, block_size
            )

    def test_gb18030_two_byte_split_across_every_boundary(self):
        text = "uid,city\nX1,北京\nX2,广州深圳\n"
        raw = text.encode("gb18030")
        baseline = [r.fields for r in parse_all(raw, "gb18030", 2, 999)]
        for block_size in range(1, 10):
            records = parse_all(raw, "gb18030", 2, block_size)
            self.assertEqual(
                [r.fields for r in records], baseline, block_size
            )

    def test_quoted_newline_split_across_crlf(self):
        raw = b'uid,n\nX1,"a\r\nb"\nX2,c\n'
        for block_size in range(1, 8):
            records = parse_all(raw, "gb18030", 2, block_size)
            self.assertEqual(records[1].fields, ("X1", "a\r\nb"))
            self.assertEqual(records[2].fields, ("X2", "c"))


class RecordShapeTests(unittest.TestCase):
    def test_lf_terminator(self):
        records = parse_all(b"a,b\n1,2\n", "gb18030", 2, 2)
        self.assertEqual(records[1].fields, ("1", "2"))
        self.assertEqual(records[1].raw, b"1,2\n")

    def test_no_trailing_newline(self):
        records = parse_all(b"a,b\n1,2", "gb18030", 2, 3)
        self.assertEqual(records[1].fields, ("1", "2"))
        self.assertEqual(records[1].raw, b"1,2")

    def test_empty_field_and_empty_quoted_field(self):
        raw = b'a,b\n,\n"",x\n'
        records = parse_all(raw, "gb18030", 2, 2)
        self.assertEqual(records[1].fields, ("", ""))
        self.assertEqual(records[2].fields, ("", "x"))

    def test_escaped_quotes(self):
        raw = b'a,b\nx,"he said ""hi"""\n'
        records = parse_all(raw, "gb18030", 2, 2)
        self.assertEqual(records[1].fields, ("x", 'he said "hi"'))

    def test_first_data_record_excludes_bom_and_header(self):
        raw = b"\xef\xbb\xbfuid,v\nX1,1\n"
        records = parse_all(raw, "utf-8-sig", 2, 2)
        self.assertEqual(records[0].start_offset, 0)
        self.assertEqual(records[1].start_offset, records[0].end_offset)
        self.assertEqual(records[1].fields, ("X1", "1"))


class ParserFailureTests(unittest.TestCase):
    def test_unclosed_quote(self):
        with self.assertRaises(ParseError):
            parse_all(b'a,b\nx,"unclosed\n', "gb18030", 2, 4)

    def test_column_count_too_few(self):
        with self.assertRaises(ParseError):
            parse_all(b"a,b,c\n1,2\n", "gb18030", 3, 4)

    def test_column_count_too_many(self):
        with self.assertRaises(ParseError):
            parse_all(b"a,b\n1,2,3\n", "gb18030", 2, 4)

    def test_bare_quote_in_unquoted_field(self):
        with self.assertRaises(ParseError):
            parse_all(b'a,b\n1,x"y\n', "gb18030", 2, 4)

    def test_invalid_gb18030_byte(self):
        raw = b"a,b\n1,\xff\xfe\n"
        with self.assertRaises(ParseError):
            parse_all(raw, "gb18030", 2, 4)

    def test_invalid_utf8_byte(self):
        raw = b"\xef\xbb\xbfa,b\n1,\xff\n"
        with self.assertRaises(ParseError):
            parse_all(raw, "utf-8-sig", 2, 4)

    def test_missing_bom_declared_utf8_sig(self):
        with self.assertRaises(ParseError):
            parse_all(b"a,b\n1,2\n", "utf-8-sig", 2, 4)

    def test_split_bom_across_chunks_still_accepted(self):
        raw = b"\xef\xbb\xbfa,b\n1,2\n"
        records = parse_all(raw, "utf-8-sig", 2, 1)
        self.assertEqual(records[1].fields, ("1", "2"))


if __name__ == "__main__":
    unittest.main()

