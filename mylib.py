# encoding=utf-8
import os
from typing import Iterator, List
import mmap
from include import *
import struct
from collections import namedtuple


class myargv(object):
    def __init__(self, argv):
        self.argv = argv
        self.parms = {}
        self.tablespace = ""

    def parse_cmdline(self):
        argv = self.argv
        if len(argv) == 1:
            print("Usage: python py_innodb_page_info.py [OPTIONS] tablespace_file")
            print("For more options, use python py_innodb_page_info.py -h")
            return 0
        while argv:
            if argv[0][0] == "-":
                if argv[0][1] == "h":
                    self.parms[argv[0]] = ""
                    argv = argv[1:]
                    break
                if argv[0][1] == "v":
                    self.parms[argv[0]] = ""
                    argv = argv[1:]
                else:
                    self.parms[argv[0]] = argv[1]
                    argv = argv[2:]
            else:
                self.tablespace = argv[0]
                argv = argv[1:]
        if "-h" in self.parms:
            print("Get InnoDB Page Info")
            print("Usage: python py_innodb_page_info.py [OPTIONS] tablespace_file\n")
            print("The following options may be given as the first argument:")
            print("-h        help ")
            print("-o output put the result to file")
            print("-t number thread to anayle the tablespace file")
            print("-v        verbose mode")
            return 0
        return 1


def mach_read_from_n(page: bytes, start_offset: int, length: int) -> str:
    ret = page[start_offset : start_offset + length]
    return ret.hex()


# https://dev.mysql.com/doc/internals/en/innodb-fil-header.html
FilHeader = namedtuple(
    "FilHeader",
    "FIL_PAGE_SPACE FIL_PAGE_OFFSET FIL_PAGE_PREV FIL_PAGE_NEXT FIL_PAGE_LSN "
    "FIL_PAGE_TYPE FIL_PAGE_FILE_FLUSH_LSN FIL_PAGE_ARCH_LOG_NO",
)
FilHeaderStruct = struct.Struct(">IIIIQHQI")


def decode_fil_header(raw_page: bytes) -> FilHeader:
    return FilHeader._make(FilHeaderStruct.unpack_from(raw_page, 0))


# https://dev.mysql.com/doc/internals/en/innodb-page-header.html
PageHeader = namedtuple(
    "PageHeader",
    "PAGE_N_DIR_SLOTS PAGE_HEAP_TOP PAGE_N_HEAP PAGE_FREE PAGE_GARBAGE PAGE_LAST_INSERT "
    "PAGE_DIRECTION PAGE_N_DIRECTION PAGE_N_RECS PAGE_MAX_TRX_ID PAGE_LEVEL "
    "PAGE_INDEX_ID PAGE_BTR_SEG_LEAF PAGE_BTR_SEG_TOP",
)
PageHeaderStruct = struct.Struct(">HHHHHHHHHQHQ10s10s")


def decode_page_header(raw_page: bytes) -> PageHeader:
    # Page header is right after file headers
    return PageHeader._make(
        PageHeaderStruct.unpack_from(raw_page, FilHeaderStruct.size)
    )


NewRecordHeader = namedtuple(
    "NewRecordHeader",
    "INFO_BITS NEXT",
)
NewRecordHeaderStruct = struct.Struct(">3sh")


def decode_new_record_header(raw_page: bytes, record_offset: int) -> NewRecordHeader:
    # Page header is right after file headers
    return NewRecordHeader._make(
        NewRecordHeaderStruct.unpack_from(
            raw_page, record_offset - REC_N_NEW_EXTRA_BYTES
        )
    )


class InnoDBRecord:
    """Single record, with associated header.
    https://github.com/mysql/mysql-server/blob/ee4455a33b10f1b1886044322e4893f587b319ed/storage/innobase/rem/rem0rec.cc
    """

    def __init__(self, raw_page: bytes, offset: int) -> None:
        """Instanciate record from a given page & offset.
        Offset points to the beginning or record's data (ie. past the header).
        """
        self.raw_page = raw_page
        self.offset = offset
        self.header = decode_new_record_header(raw_page, offset)


class InnoDBPage:
    """Wrapper around an InnoDB 16kB page.
    https://blog.jcole.us/2013/01/07/the-physical-structure-of-innodb-index-pages/
    https://dev.mysql.com/doc/internals/en/innodb-fil-header.html
    https://gitee.com/jink2005/percona-data-recovery-tool-for-innodb/blob/master/page_parser.c
    http://assets.en.oreilly.com/1/event/36/Recovery%20of%20Lost%20or%20Corrupted%20InnoDB%20Tables%20Presentation.pdf
    """

    def __init__(self, raw_content: bytes) -> None:
        self.raw_content = raw_content
        self.fil_header = decode_fil_header(raw_content)
        self.page_header = decode_page_header(raw_content)

    def get_offset(self) -> str:
        return self.fil_header.FIL_PAGE_OFFSET

    def get_type(self) -> str:
        return self.fil_header.FIL_PAGE_TYPE

    def get_level(self) -> str:
        return self.page_header.PAGE_LEVEL

    def is_compact(self) -> bool:
        """Return true if page is using COMPACT mode.
        Extracted from first bit of PAGE_N_HEAP.
        """
        return self.page_header.PAGE_N_HEAP & 0x8000 != 0

    def get_infinum_offset(self) -> int:
        """Get the offset of the infinum record according to the page type."""
        if self.is_compact():
            return PAGE_NEW_INFIMUM
        return PAGE_OLD_INFIMUM

    def get_records(self) -> Iterator[InnoDBRecord]:
        """Return iterator of page's user records."""
        # Get static infimum record
        if self.page_header.PAGE_N_RECS:
            # Walk through the single-linked chain of records, sorted by primary key
            offset = self.get_infinum_offset()
            assert self.raw_content[offset : offset + 7] == b"infimum"
            record = InnoDBRecord(self.raw_content, offset)
            while record.header.NEXT:
                # NEXT is relative to current record's offset - which can be negative
                # print(f"{record.offset} + {record.header.NEXT}")
                record = InnoDBRecord(
                    self.raw_content, record.offset + record.header.NEXT
                )
                yield record


class InnoDBFile:
    """Wrapper around an InnoDB file (.ibd)."""

    def __init__(self, filename: str) -> None:
        self.filename = filename

    def get_pages(self) -> Iterator[InnoDBPage]:
        with open(self.filename, "rb") as file_obj:
            fsize = os.path.getsize(file_obj.name) // INNODB_PAGE_SIZE
            with mmap.mmap(
                file_obj.fileno(), length=0, access=mmap.ACCESS_READ
            ) as mmap_obj:
                for _ in range(fsize):
                    page = InnoDBPage(mmap_obj.read(INNODB_PAGE_SIZE))
                    yield page


def get_innodb_page_type(args):
    data_file = InnoDBFile(args.tablespace)
    ret = {}
    nb_pages = 0
    for page in data_file.get_pages():
        nb_pages += 1
        page_offset = page.get_offset()
        page_type = page.get_type()
        if "-v" in args.parms:
            if page_type == 0x45BF:
                page_level = page.get_level()
                print(
                    f"page offset {page_offset}, page type <{innodb_page_type[page_type]}>, page level <{page_level}>"
                    f" - heap top: {page.page_header.PAGE_HEAP_TOP}; heap records: {page.page_header.PAGE_N_HEAP}; records: {page.page_header.PAGE_N_RECS}; compact: {page.is_compact()}"
                )
            else:
                print(
                    "page offset %s, page type <%s>"
                    % (page_offset, innodb_page_type[page_type])
                )
            for record in page.get_records():
                print(f"{record}")
        if page_type not in ret:
            ret[page_type] = 1
        else:
            ret[page_type] = ret[page_type] + 1
    print("Total number of page: %d:" % nb_pages)
    for k, v in ret.items():
        print("%s: %s" % (innodb_page_type[k], v))
