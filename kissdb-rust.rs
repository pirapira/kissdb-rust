use std::io::{FileAccess, FileMode, Open, Read, Truncate};
use std::io::{ReadWrite};
use std::io::{SeekEnd, SeekSet};
use std::io::fs::File;
use std::mem::size_of;


static version: u8 = 1;


// some definitions from kissdb.h

struct Kissdb {
  hash_table_size : u64,
  key_size : u64,
  value_size : u64,
  hash_tables : ~[u64],
  f : std::io::fs::File
}

enum Error {
  ErrorIO,
  ErrorMalloc,
  InvalidParameters,
  CorruptDbFile
}

enum OpenMode {
  ReadOnly,
  RW,
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

// either
// 1. use the c95 based alloc, realloc and free
// 2. use more rustic something
// maybe trying 2. gives me more knowledge
fn kissdb_open(
    path : &Path,
    orig_mode : OpenMode,
    mut hash_table_size : u64,
    mut key_size : u64,
    mut value_size : u64
    ) -> Option<~ Kissdb>
{
    let (mode, access) : (FileMode, FileAccess) =
        match orig_mode {
            ReadOnly =>  (Open, Read), // rb
            RW => (Open, ReadWrite), // r+b
            RWCreate =>  (Open, ReadWrite), // r+b
            RWReplace => (Truncate, ReadWrite) // w+b
        };
    let mut f_ : Option<File>
        = std::io::fs::File::open_mode(path, mode, access);

    if f_.is_none() {
        match (orig_mode) {
            RWCreate =>
            f_ = std::io::fs::File::open_mode(path, Truncate, ReadWrite), // w+b
            _ => f_ = f_
        }
    };

    match f_ {
        None => return None,
        Some (mut f) =>
        {let kissdb_header_size : u64 = ((size_of::<u64>() * 3) + 4) as u64;

         f.seek(0, SeekEnd);
         if (f.tell() < kissdb_header_size) {
             if hash_table_size > 0 && key_size > 0
                 && value_size >0
             {
                 f.seek(0, SeekSet);
                 let tmp2 : [u8, ..4] = ['K' as u8, 'd' as u8, 'R' as u8, version];
                 f.write(tmp2);
                 f.write_le_u64(hash_table_size);
                 f.write_le_u64(key_size);
                 f.write_le_u64(value_size);
                 f.flush();
             }
             else {
                 fail!()
             }
         }
         else {
             f.seek(0, SeekSet);
             let mut tmp2 : [u8, ..4] = [0,0,0,0];
             if f.read(tmp2) != Some(4) { fail! (); }
             if tmp2 != ['K' as u8, 'd' as u8, 'R' as u8, version] {fail! ();}
             hash_table_size = f.read_le_u64();
             key_size = f.read_le_u64();
             value_size = f.read_le_u64();
         }

         // let hash_table_size_bytes : uint = size_of::<u64>() * (hash_table_size + 1) as uint;
         // let mut httmp = std::vec::from_elem(hash_table_size_bytes, 0 as u8);
         //         let httmp = do std::at_vec::build::<u64>(None) |push| { for i in range(0, hash_table_size + 1)
//                  { push (0); }
        //};

         let mut num_hash_tables = 0;

         // should use at_vec, actually
         // no, std::vec is OK.  It has appends
         let mut hash_tables : ~[u64] = std::vec::from_elem(0, 0 as u64);
         let mut h : Option<~[u64]>;

         // do we do realloc in rust? with append, apparently.
         while({h = f.read_one_hash_table(hash_table_size); h.is_some()})
         {
             let httmp = h.unwrap();
             hash_tables = std::vec::append(hash_tables, httmp);
             num_hash_tables = num_hash_tables + 1;
             if (httmp[hash_table_size] > 0) {
                 f.seek(httmp[hash_table_size] as i64, SeekSet)
             }
             else {
                 break;
             }
         }

         // construct h somehow
         return Some(~Kissdb {hash_table_size : hash_table_size,
                         key_size : key_size,
                         value_size : value_size,
                         hash_tables : hash_tables,
                         f : f})
        }
    }
}

trait File2 {
    fn read_one_hash_table(&mut self, u64) -> Option<~[u64]>;
}

impl File2 for File {
        fn read_one_hash_table(&mut self,
                               hash_table_size : u64) -> Option<~[u64]> {
            let bs = (hash_table_size + 1) as uint * size_of::<u64>();
            let mut buf = std::vec::from_elem(bs, 0 as u8);
            let mut result = std::vec::from_elem((hash_table_size + 1) as uint, 0 as u64);
            match self.read(buf) {
                None => None,
                Some(buf_size) if buf_size == bs => {
                    let mut br = std::io::mem::BufReader::new(buf);
                    for i in range(0, hash_table_size + 1) {
                        result[i] = br.read_le_u64();
                    }
                    Some((result))
                },
                _ => None
            }
        }
}

fn kissdb_close(~db : ~Kissdb) {
}



fn main()
{
    let db = kissdb_open(&Path::new("test.db"), RWReplace, 1024, 8, size_of::<u64>() as u64);
    kissdb_close(db.unwrap());
    let db_r = kissdb_open(&Path::new("test.db"), ReadOnly, 4, 8, size_of::<u64>() as u64);
    let db_r = db_r.unwrap();
    println(db_r.hash_table_size.to_str());
}
