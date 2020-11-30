# encoding=utf-8

# Start of the data on the page
FIL_PAGE_DATA = 38


FIL_PAGE_OFFSET = 4  # page offset inside space
FIL_PAGE_TYPE = 24  # File page type

# Types of an undo log segment */
TRX_UNDO_INSERT = 1
TRX_UNDO_UPDATE = 2

# On a page of any file segment, data may be put starting from this offset
FSEG_PAGE_DATA = FIL_PAGE_DATA

# The offset of the undo log page header on pages of the undo log
TRX_UNDO_PAGE_HDR = FSEG_PAGE_DATA

PAGE_LEVEL = 26  # level of the node in an index tree; the leaf level is the level 0 */
IDX_HEAP_TOP_POS = 2

innodb_page_type = {
    0x0000: "Freshly Allocated Page",
    0x0002: "Undo Log Page",
    0x0003: "File Segment inode",
    0x0004: "Insert Buffer Free List",
    0x0005: "Insert Buffer Bitmap",
    0x0006: "System Page",
    0x0007: "Transaction system Page",
    0x0008: "File Space Header",
    0x0009: "extend description page",
    0x000A: "Uncompressed BLOB Page",
    0x000B: "1st compressed BLOB Page",
    0x000C: "Subsequent compressed BLOB Page",
    0x45BF: "B-tree Node",
}

innodb_page_direction = {
    0x0000: "Unknown(0x0000)",
    0x0001: "Page Left",
    0x0002: "Page Right",
    0x0003: "Page Same Rec",
    0x0004: "Page Same Page",
    0x0005: "Page No Direction",
    0xFFFF: "Unkown2(0xffff)",
}


INNODB_PAGE_SIZE = 1024 * 16  # InnoDB Page 16K

# From https://dev.mysql.com/doc/dev/mysql-server/latest//page0types_8h.html#a204eb2227c76883f72599d6cdedd2aee
PAGE_HEADER = FIL_PAGE_DATA
FSEG_HEADER_SIZE = 10
PAGE_DATA = PAGE_HEADER + 36 + 2 * FSEG_HEADER_SIZE
REC_N_NEW_EXTRA_BYTES = 5
REC_N_OLD_EXTRA_BYTES = 6
PAGE_NEW_INFIMUM = PAGE_DATA + REC_N_NEW_EXTRA_BYTES
PAGE_OLD_INFIMUM = PAGE_DATA + 1 + REC_N_OLD_EXTRA_BYTES
