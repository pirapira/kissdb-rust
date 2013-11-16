static version: uint = 1u;


// some definitions from kissdb.h

struct Kissdb {
  hash_table_size : u8,
  key_size : u8,
  value_size : u8,
  hash_table_size_bytes : u8,
  hash_tables : ~u8,
  f : std::rt::io::file::FileWriter
}

enum Error {
  ErrorIO,
  ErrorMalloc,
  InvalidParameters,
  CorruptDbFile
}

enum OpenMode {
  ReadOnly,
  ReadWrite,
  RWCreate,
  RWReplace
}

fn main()
{
}
