static version: uint = 1u;


// some definitions from kissdb.h

struct Kissdb {
  hash_table_size : u64,
  key_size : u64,
  value_size : u64,
  hash_table_size_bytes : u64,
  hash_tables : ~u64,
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


fn kissdb_hash(b :&[u8], len: u64) -> u64
{
    let mut hash : u64 = 5381;
    for i in range (0, len) {
        hash = ((hash << 5) + hash) + (b[i] as u64);
    }
    return hash
}


fn main()
{
}
