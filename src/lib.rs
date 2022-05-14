/// Facilitates fault injection. Every time any IO operation
/// is performed, this is decremented. If it hits 0, an
/// io::Error is returned from that IO operation. Use this
/// to ensure that error handling is being performed, by
/// running some test workload, checking the counter, and
/// then setting this to an incrementally-lower number while
/// asserting that your application properly handles the
/// error that will propagate up.
pub static FAULT_INJECT_COUNTER: AtomicU64 =
    AtomicU64::new(u64::MAX);

fn rng_sleep() {
    let rdtsc =
        unsafe { core::arch::x86_64::_rdtsc() as u16 };
    for _ in 0..rdtsc.trailing_zeros() {
        std::thread::yield_now();
    }
}

macro_rules! io_try {
    ($e:expr) => {{
        if crate::FAULT_INJECT_COUNTER
            .fetch_sub(1, Ordering::Relaxed)
            == 1
        {
            return Err(io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "injected fault at {}:{}",
                    file!(),
                    line!()
                ),
            ));
        }

        crate::rng_sleep();

        // converts io::Error to include the location of
        // error creation
        match $e {
            Ok(ok) => ok,
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!(
                        "{}:{} -> {}",
                        file!(),
                        line!(),
                        e.to_string()
                    ),
                ))
            }
        }
    }};
}

const SUBDIR: &str = "sharded_logs";
const WARN: &str = "DO_NOT_PUT_YOUR_FILES_HERE";

use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{
            AtomicBool, AtomicU64, AtomicUsize, Ordering,
        },
        Arc, Mutex, MutexGuard,
    },
};

use crc32fast::Hasher;

#[derive(Debug)]
pub struct Config {
    path: PathBuf,
    shards: u8,
    buf_writer_size: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            path: Default::default(),
            shards: 8,
            buf_writer_size: 512 * 1024,
        }
    }
}

/// Allows batches of bytes to be written
/// with low contention, and read-back
/// in a linearized order.
pub struct ShardedLog {
    shards: Arc<[Shard]>,
    idx: usize,
    idx_counter: Arc<AtomicUsize>,
    next_lsn: Arc<AtomicU64>,
}

pub struct Reservation<'a> {
    shard: MutexGuard<'a, BufWriter<File>>,
    completed: bool,
    lsn: u64,
}

impl<'a> Drop for Reservation<'a> {
    fn drop(&mut self) {
        if !self.completed {
            if let Err(e) = write_batch_inner::<&[u8]>(
                &mut self.shard,
                self.lsn,
                &[],
            ) {
                eprintln!(
                    "error while writing empty batch on \
                    Reservation Drop: {:?}",
                    e
                );
            }
            self.completed = true;
        }
    }
}

impl<'a> Reservation<'a> {
    pub fn write_batch<B: AsRef<[u8]>>(
        mut self,
        write_batch: &[B],
    ) -> io::Result<u64> {
        write_batch_inner(
            &mut self.shard,
            self.lsn,
            write_batch,
        )
    }

    pub fn abort(mut self) -> io::Result<u64> {
        write_batch_inner::<&[u8]>(
            &mut self.shard,
            self.lsn,
            &[],
        )
    }
}

impl Clone for ShardedLog {
    fn clone(&self) -> ShardedLog {
        ShardedLog {
            shards: self.shards.clone(),
            idx_counter: self.idx_counter.clone(),
            idx: self
                .idx_counter
                .fetch_add(1, Ordering::SeqCst),
            next_lsn: self.next_lsn.clone(),
        }
    }
}

struct Shard {
    file_mu: Mutex<BufWriter<File>>,
    dirty: AtomicBool,
}

pub struct Iter<'a> {
    next_expected_lsn: u64,
    shards: Vec<BufReader<MutexGuard<'a, File>>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = io::Result<(u64, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl ShardedLog {
    pub fn open<P: AsRef<Path>>(
        path: P,
    ) -> io::Result<ShardedLog> {
        ShardedLog::open_with_config(Config {
            path: path.as_ref().into(),
            ..Default::default()
        })
    }

    pub fn open_with_config(
        config: Config,
    ) -> io::Result<ShardedLog> {
        use fs2::FileExt;

        let _ = File::create(
            config.path.join(SUBDIR).join(WARN),
        );

        let mut file_opts = OpenOptions::new();
        file_opts.create(true).read(true).write(true);

        let mut shards = vec![];

        for idx in 0..config.shards {
            let path = config
                .path
                .join(SUBDIR)
                .join(idx.to_string());
            let file = io_try!(file_opts.open(path));
            io_try!(file.try_lock_exclusive());
            shards.push(Shard {
                file_mu: Mutex::new(
                    BufWriter::with_capacity(
                        config.buf_writer_size,
                        file,
                    ),
                ),
                dirty: false.into(),
            })
        }

        let next_lsn: u64 = todo!();

        Ok(ShardedLog {
            shards: shards.into(),
            idx: 0,
            idx_counter: Arc::new(AtomicUsize::new(1)),
            next_lsn: Arc::new(next_lsn.into()),
        })
    }

    /// Write a batch of buffers to the sharded log.
    /// Returns the logical sequence number that they
    /// will be recoverable at after your next call
    /// to `flush`.
    ///
    /// If this function returns an error, it is
    /// possible that the log is in a
    /// TODO
    pub fn write_batch<B: AsRef<[u8]>>(
        &mut self,
        write_batch: &[B],
    ) -> io::Result<u64> {
        self.reservation().write_batch(write_batch)
    }

    /// Reserve a log slot at a particular index,
    /// which can then be completed using
    /// `Reservation::write_batch` or aborted using
    /// `Reservation::abort`. This is particularly
    /// useful for logging fallible lock-free operations
    /// to disk in an order-preserving manner.
    pub fn reservation(&mut self) -> Reservation<'_> {
        let shard = self.get_shard();

        // NB lsn has to be created after acquiring the shard.
        let lsn =
            self.next_lsn.fetch_add(1, Ordering::Release);

        Reservation {
            shard,
            lsn,
            completed: false,
        }
    }

    /// Read the entire log off of disk, zipping all shards
    /// together to iterate over non-aborted batches
    pub fn iter(&self) -> Iter<'_> {
        todo!()
    }

    ///
    pub fn reset(&self) -> io::Result<()> {
        todo!()
    }

    /// Flushes and fsyncs in-memory shard data that
    /// have been written to directly from this instance
    /// of `ShardedLog`, but skipping shards that have
    /// been written to by others. To flush all shards,
    /// use `flush_all`.
    pub fn flush(&mut self) -> io::Result<()> {
        for shard in &*self.shards {
            if shard.dirty.load(Ordering::Acquire) {
                let mut file =
                    shard.file_mu.lock().unwrap();
                if shard.dirty.load(Ordering::Acquire) {
                    file.flush()?;
                    file.get_mut().sync_all()?;
                    shard
                        .dirty
                        .store(false, Ordering::Release);
                }
            }
        }
        Ok(())
    }

    fn get_shard(&self) -> MutexGuard<'_, BufWriter<File>> {
        let len = self.shards.len();
        for i in 0..len {
            let idx = self.idx.wrapping_add(i) % len;

            if let Ok(shard) =
                self.shards[idx].file_mu.try_lock()
            {
                self.shards[idx]
                    .dirty
                    .store(true, Ordering::Release);
                return shard;
            }
        }
        self.shards[self.idx]
            .dirty
            .store(true, Ordering::Release);
        self.shards[self.idx].file_mu.lock().unwrap()
    }
}

fn write_batch_inner<B: AsRef<[u8]>>(
    shard: &mut BufWriter<File>,
    lsn: u64,
    write_batch: &[B],
) -> io::Result<u64> {
    let size_bytes: [u8; 8] =
        (write_batch.len() as u64).to_le_bytes();
    let lsn_bytes: [u8; 8] = lsn.to_le_bytes();
    let mut hasher = Hasher::new();
    hasher.update(&size_bytes);
    hasher.update(&lsn_bytes);
    let crc: [u8; 4] = hasher.finalize().to_le_bytes();

    io_try!(shard.write_all(&crc));
    io_try!(shard.write_all(&size_bytes));
    io_try!(shard.write_all(&lsn_bytes));

    for buf_i in write_batch {
        let buf = buf_i.as_ref();
        let crc_bytes: [u8; 4];
        let len_bytes: [u8; 8] =
            (buf.len() as u64).to_le_bytes();

        let mut hasher = Hasher::new();
        hasher.update(&len_bytes);
        hasher.update(&buf);
        crc_bytes = hasher.finalize().to_le_bytes();

        io_try!(shard.write_all(&crc_bytes));
        io_try!(shard.write_all(&len_bytes));
        io_try!(shard.write_all(buf));
    }

    Ok(lsn)
}
