use std::io::{Open, Read, Truncate, ReadWrite, SeekEnd, SeekSet};
use std::io::fs::File;
use std::mem::size_of;
use std::io::Decorator;
use std::vec;
use std::io::mem::{BufReader, MemWriter};
use std::io::io_error;

static VERSION: u8 = 1;


// some definitions from kissdb.h

struct Kissdb {
    hash_table_size : u64,
    key_size : u64,
    value_size : u64,
    num_hash_tables : u64,
    hash_tables : ~[u64],
    f : File
}

enum OpenMode {
    ReadOnly,
    RW,
    RWCreate,
    RWReplace
}

fn kissdb_hash(b : &[u8]) -> u64 {
    let mut hash = 5381;
    for &bx in b.iter() {
        hash = ((hash << 5) + hash) + (bx as u64);
    }
    hash
}

fn kissdb_open(
    path : &Path,
    orig_mode : OpenMode,
    mut hash_table_size : u64,
    mut key_size : u64,
    mut value_size : u64
        ) -> Option<~Kissdb>
{
    let (mode, access) = match orig_mode {
        ReadOnly =>  (Open, Read), // rb
        RW => (Open, ReadWrite), // r+b
        RWCreate =>  (Open, ReadWrite), // r+b
        RWReplace => (Truncate, ReadWrite) // w+b
    };
    let f_ = File::open_mode(path, mode, access).or_else(|| {
            match orig_mode {
                RWCreate => File::open_mode(path, Truncate, ReadWrite),
                _ => None
            }
        });

    let mut f = match f_ {
        Some(f) => f,
        None => return None
    };
    let kissdb_header_size = ((size_of::<u64>() * 3) + 4) as u64;

    f.seek(0, SeekEnd);
    if (f.tell() < kissdb_header_size) {
        if hash_table_size > 0 && key_size > 0
            && value_size >0
            {
            f.seek(0, SeekSet);
            let tmp2 = ['K' as u8, 'd' as u8, 'R' as u8, VERSION];
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
        let mut tmp2 = [0, ..4];
        if f.read(tmp2) != Some(4) { fail!() }
        if tmp2 != ['K' as u8, 'd' as u8, 'R' as u8, VERSION] { fail!() }
        hash_table_size = f.read_le_u64();
        key_size = f.read_le_u64();
        value_size = f.read_le_u64();
    }

    let mut num_hash_tables = 0;

    let mut hash_tables = ~[];
    let mut h;

    while ({h = f.read_one_hash_table(hash_table_size); h.is_some()})
    {
        let httmp = h.unwrap();
        hash_tables.push_all(httmp);
        num_hash_tables = num_hash_tables + 1;
        if (httmp[hash_table_size] > 0) {
            f.seek(httmp[hash_table_size] as i64, SeekSet)
        }
        else {
            break;
        }
    }

    debug!("key_size = {:s}", key_size.to_str());

    Some(~Kissdb {hash_table_size : hash_table_size,
                  key_size : key_size,
                  value_size : value_size,
                  num_hash_tables : num_hash_tables,
                  hash_tables : hash_tables,
                  f : f})
}


trait File2 {
    fn read_one_hash_table(&mut self, u64) -> Option<~[u64]>;
    fn write_one_hash_table(&mut self, &[u64]);
}

impl File2 for File {
    fn read_one_hash_table(&mut self,
                           hash_table_size : u64) -> Option<~[u64]> {
        let bs = (hash_table_size + 1) as uint * size_of::<u64>();
        let mut err = false;

        io_error::cond.trap(|_| err = true ).inside(|| {
            let buf = self.read_bytes(bs);
            let mut br = BufReader::new(buf);
            if err { None }
            else {
                Some(vec::from_fn((hash_table_size + 1) as uint,
                                  |_| br.read_le_u64()))
            }
        })
    }
    fn write_one_hash_table(&mut self, ht : &[u64]) {
        for &x in ht.iter() {
            self.write_le_u64(x);
        }
    }
}

trait Kdb {
    fn kissdb_get(&mut self, key : &[u8]) -> Option<~[u8]>;
    fn kissdb_put(&mut self, key : &[u8], value : &[u8]) -> bool;
}

impl Kdb for Kissdb {
    fn kissdb_get(&mut self, key : &[u8]) -> Option<~[u8]> {
        if key.len() != self.key_size as uint { return None }
        let hash = kissdb_hash(key) % self.hash_table_size;
        let hash_tables = &self.hash_tables;
        for i in range(0, self.num_hash_tables) {
            let offset = hash_tables[i * (self.hash_table_size + 1) + hash] as i64;
            if (offset == 0) { return None }; // yeah, 0 is being special. never used normally.
            self.f.seek(offset as i64, SeekSet);

            let klen = self.key_size as uint;
            let tmp = self.f.read_bytes(klen);

            if key == tmp {
                return Some(self.f.read_bytes(self.value_size as uint))
            }
        }
        None
    }

    fn kissdb_put(&mut self, key : &[u8], value : &[u8]) -> bool {
        if key.len() != self.key_size as uint { fail!() }
        if value.len() != self.value_size as uint { fail!() }
        let hash = kissdb_hash(key) % self.hash_table_size;
        let mut last_ht_offset = ((size_of::<u64>() * 3) + 4) as u64;
        let mut ht_offset = last_ht_offset;

        for i in range(0, self.num_hash_tables) {
            let offset = self.hash_tables[(self.hash_table_size + 1) * i + hash] as i64;
            if (offset != 0) { // yes, 0 is treated special. 0 will never used normally
                /* rewrite if already exists */
                self.f.seek(offset, SeekSet);

                let klen = self.key_size as uint;
                let tmp = self.f.read_bytes(klen);
                if key == tmp {
                    self.f.write(value);
                    self.f.flush();
                    return true; /* success */
                }
                // put_no_match_next_hash_table:
                last_ht_offset = ht_offset;
                ht_offset = self.hash_tables[(self.hash_table_size + 1) * i + self.hash_table_size];
                //cmp != , should be put_no_match_next_hash_table
            } else {
                /* add if an empty hash table slot is discovered */
                self.f.seek(0, SeekEnd);
                let end_offset = self.f.tell();

                // debug!("endoffset = {:s}",endoffset.to_str());
                self.f.write(key);
                self.f.write(value);

                self.f.seek(ht_offset as i64 + (size_of::<u64>() as i64 * hash as i64), SeekSet);
                self.f.write_le_u64(end_offset);
                self.hash_tables[(self.hash_table_size + 1) * i + hash] = end_offset;

                self.f.flush();

                return true; /* success */
            }
        }

        /* if no existing slots, add a new page of hash table entries */
        debug!("add new page");
        self.f.seek(0, SeekEnd);
        let endoffset = self.f.tell();

        let mut new_table = vec::from_elem(self.hash_table_size as uint + 1, 0 as u64);
        let hash_table_size_bytes = size_of::<u64>() as u64 * (self.hash_table_size + 1);
        new_table[hash] = endoffset + hash_table_size_bytes; // where key, value are to be written
        self.f.write_one_hash_table(new_table);
        self.hash_tables.push_all_move(new_table);

        debug!("writing at {:s}", self.f.tell().to_str());
        self.f.write(key);
        self.f.write(value);

        if self.num_hash_tables > 0 {
            self.f.seek(last_ht_offset as i64 + (size_of::<u64>() as i64 * self.hash_table_size as i64), SeekSet);
            self.f.write_le_u64(endoffset);
            // mimick rereading
            self.hash_tables[((self.hash_table_size + 1) * (self.num_hash_tables - 1)) + self.hash_table_size] = endoffset;
        }

        self.num_hash_tables = self.num_hash_tables + 1;
        self.f.flush();
        true /* success */
    }

}

#[test]
fn original_test()
{
    println("Opening new empty database test.db...");
    {
        let mut db = kissdb_open(&Path::new("test.db"), RWReplace, 1024, 8, size_of::<u64>() as u64).unwrap();

        println("Adding and then re-getting 10000 64-byte values...");

        let mut v = [0,0,0,0,0,0,0,0];

        for i in range(0, 10000) {
            for j in range(0, 8) {
                v[j] = i as u8;
            }
            let mut key = MemWriter::new();
            key.write_le_u64(i as u64);
            let key_buf = key.inner();
            db.kissdb_put(key_buf, v);
            let gotten = db.kissdb_get(key_buf).unwrap();
            for j in range(0, 8) {
                if gotten[j] != i as u8 { fail!() }
            }
        }
        println("Getting 10000 64-byte values...");

        for i in range(0, 10000) {
            let mut key = MemWriter::new();
            key.write_le_u64(i as u64);
            let gotten = db.kissdb_get(key.inner()).unwrap();
            for j in range(0,8) {
                if gotten[j] != i as u8 { fail!() }
            }
        }

        println("closing and re-opening database in read-only mode...");
    }

    {
        let mut db = kissdb_open(&Path::new("test.db"), ReadOnly, 1024, 8, size_of::<u64>() as u64).unwrap();

        println("Getting 10000 64-byte values...");

        for i in range(0, 10000) {
            let mut key = MemWriter::new();
            key.write_le_u64(i as u64);
            let gotten = db.kissdb_get(key.inner()).unwrap();
            for j in range(0,8) {
                if gotten[j] != i as u8 { fail!() }
            }
        }
    }

    println("Iterator not implemented ...");

    println("All tests OK!");
}
