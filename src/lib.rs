use std::{
    collections::HashSet,
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
};

#[derive(Debug)]
pub struct Config {
    shards: u8,
    buf_writer_size: usize,
}

pub struct ShardedLog {
    shards: Arc<[Shard]>,
    idx: usize,
    next_lsn: Arc<AtomicU64>,
    dirty_shards: HashSet<usize>,
}

impl Clone for ShardedLog {
    fn clone(&self) -> ShardedLog {
        ShardedLog {
            shards: self.shards.clone(),
            idx: self.idx.wrapping_add(1),
            next_lsn: self.next_lsn.clone(),
            dirty_shards: HashSet::new(),
        }
    }
}

impl Write for ShardedLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.shards.len();
        for i in 0..len {
            let idx = (i + self.idx) % len;

            if let Ok(mut shard) = self.shards[idx].file_mu.try_lock() {
                shard.write_all(buf)?;
                return Ok(buf.len());
            }
        }
        let mut shard = self.shards[self.idx].file_mu.lock().unwrap();
        shard.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        for idx in &self.dirty_shards {
            let mut file = self.shards[*idx].file_mu.lock().unwrap();
            file.flush()?;
        }
        self.dirty_shards.clear();
        Ok(())
    }
}

#[derive(Clone)]
struct Shard {
    file_mu: Arc<Mutex<BufWriter<File>>>,
}

pub struct Iter<'a> {
    next_expected_lsn: u64,
    shards: Vec<BufReader<MutexGuard<'a, File>>>,
}

impl ShardedLog {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<ShardedLog> {
        todo!()
    }

    pub fn open_with_config(config: Config) -> io::Result<ShardedLog> {
        todo!()
    }

    pub fn iter(&self) -> Iter<'_> {
        todo!()
    }

    ///
    pub fn reset(&self) -> io::Result<()> {
        todo!()
    }
}
