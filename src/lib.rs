//! Shards batches of writes across multiple log files
//! for high throughput and low contention. The general
//! usage pattern is:
//!
//! 1. recover, pass recovered data to downstream storage
//! 2. delete old logs
//! 3. begin writing to the sharded logs

const SUBDIR: &str = "sharded_logs";
const WARN: &str = "DO_NOT_PUT_YOUR_FILES_HERE";

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    mem::MaybeUninit,
    path::PathBuf,
    sync::{
        atomic::{
            AtomicBool, AtomicU64, AtomicUsize, Ordering,
        },
        Arc, Mutex, MutexGuard,
    },
};

use crc32fast::Hasher;
use fault_injection::{fallible, maybe};
use fs2::FileExt;

#[derive(Debug, Clone)]
pub struct Config {
    /// Where to open the log.
    pub path: PathBuf,
    /// Number of sharded log files. Future calls to `recover` must always use at least this
    /// many shards, otherwise the system will not be able to recover. Defaults to 8.
    pub shards: u8,
    /// The in-memory buffer size in front of each log file. Defaults to 512k.
    pub in_memory_buffer_per_log: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            path: Default::default(),
            shards: 8,
            in_memory_buffer_per_log: 512 * 1024,
        }
    }
}

impl Config {
    /// Iterate over all log shards, returning batches in the
    /// order that they were written in, even if they landed
    /// in different shards. This method does not take out an
    /// exclusive file lock on the shards in the way that
    /// `Config::purge` and `Config::create` do, so it can
    /// be used on logs that are actively being written.
    /// HOWEVER: keep in mind that while writing logs,
    /// significant data may remain in the in-memory
    /// `BufWriter` instances until you call
    /// `ShardedLog::flush`!
    pub fn recover(&self) -> io::Result<RecoveryIterator> {
        let mut file_opts = OpenOptions::new();
        file_opts.read(true);

        let mut readers = vec![];

        for idx in 0..self.shards {
            let path = self
                .path
                .join(SUBDIR)
                .join(idx.to_string());

            let file = fallible!(file_opts.open(path));
            readers.push(BufReader::new(file));
        }

        let mut ret = RecoveryIterator {
            done: false,
            next_expected_lsn: 0,
            readers,
            last_shard: None,
            read_buffer: BTreeMap::new(),
        };

        for idx in 0..self.shards {
            ret.tick(idx as usize);
        }

        Ok(ret)
    }

    fn lock(&self) -> io::Result<File> {
        fallible!(fs::create_dir_all(
            self.path.join(SUBDIR)
        ));
        let mut lock_options = OpenOptions::new();
        lock_options.read(true).create(true).write(true);
        let lock_file = fallible!(lock_options
            .open(self.path.join(SUBDIR).join(WARN)));
        fallible!(lock_file.try_lock_exclusive());
        Ok(lock_file)
    }

    /// Clear the previous contents of a log directory.
    /// Requires an exclusive lock over the log directory,
    /// and may not be performed concurrently with an
    /// active `ShardedLog`.
    pub fn purge(&self) -> io::Result<()> {
        let _lock_file = Arc::new(fallible!(self.lock()));
        fs::remove_dir_all(&self.path)
    }

    /// Exclusively create a new log directory that
    /// may be used for writing sharded logs to.
    pub fn create(&self) -> io::Result<ShardedLog> {
        fallible!(fs::create_dir_all(
            self.path.join(SUBDIR)
        ));

        let lock_file = Arc::new(fallible!(self.lock()));

        let mut file_opts = OpenOptions::new();
        file_opts.create_new(true).write(true);

        let mut shards = vec![];
        for idx in 0..self.shards {
            let path = self
                .path
                .join(SUBDIR)
                .join(idx.to_string());

            let file = fallible!(file_opts.open(path));

            shards.push(Shard {
                file_mu: Mutex::new(
                    BufWriter::with_capacity(
                        self.in_memory_buffer_per_log,
                        file,
                    ),
                ),
                dirty: false.into(),
            })
        }

        fallible!(
            File::open(self.path.join(SUBDIR))?.sync_all()
        );

        Ok(ShardedLog {
            shards: shards.into(),
            idx: 0,
            idx_counter: Arc::new(AtomicUsize::new(1)),
            next_lsn: Arc::new(0.into()),
            config: self.clone(),
            lock_file,
        })
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
    config: Config,
    lock_file: Arc<File>,
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
        self.completed = true;
        write_batch_inner(
            &mut self.shard,
            self.lsn,
            write_batch,
        )
    }

    pub fn abort(mut self) -> io::Result<u64> {
        self.completed = true;
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
            config: self.config.clone(),
            lock_file: self.lock_file.clone(),
        }
    }
}

struct Shard {
    file_mu: Mutex<BufWriter<File>>,
    dirty: AtomicBool,
}

pub struct RecoveryIterator {
    next_expected_lsn: u64,
    readers: Vec<BufReader<File>>,
    read_buffer: BTreeMap<u64, (usize, Vec<Vec<u8>>)>,
    last_shard: Option<usize>,
    done: bool,
}

impl Iterator for RecoveryIterator {
    type Item = Vec<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.next_inner();
        if ret.is_none() {
            self.done = true;
        }
        ret
    }
}

impl RecoveryIterator {
    fn next_inner(&mut self) -> Option<Vec<Vec<u8>>> {
        if let Some(last_idx) = self.last_shard {
            self.tick(last_idx);
        }

        let (idx, buf) = self
            .read_buffer
            .remove(&self.next_expected_lsn)?;

        self.next_expected_lsn += 1;
        self.last_shard = Some(idx);
        Some(buf)
    }

    fn tick(&mut self, idx: usize) {
        macro_rules! weak_try {
            ($e:expr) => {{
                match $e {
                    Ok(ok) => ok,
                    _ => return,
                }
            }};
        }

        let mut reader = &mut self.readers[idx];

        let crc_expected: [u8; 4] =
            weak_try!(read_array(&mut reader));
        let size_bytes: [u8; 8] =
            weak_try!(read_array(&mut reader));
        let lsn_bytes: [u8; 8] =
            weak_try!(read_array(&mut reader));

        let mut hasher = Hasher::new();
        hasher.update(&size_bytes);
        hasher.update(&lsn_bytes);
        let crc_actual =
            (hasher.finalize() ^ 0xFF).to_le_bytes();

        if crc_actual != crc_expected {
            eprintln!("encountered corrupted crc in log");
            return;
        }

        let size =
            usize::try_from(u64::from_le_bytes(size_bytes))
                .unwrap();
        let lsn = u64::from_le_bytes(lsn_bytes);

        let mut write_batch = vec![];
        for _ in 0..size {
            let crc_expected: [u8; 4] =
                weak_try!(read_array(&mut reader));
            let len_bytes: [u8; 8] =
                weak_try!(read_array(&mut reader));

            let len = usize::try_from(u64::from_le_bytes(
                len_bytes,
            ))
            .unwrap();

            let mut buf = Vec::with_capacity(len);

            unsafe { buf.set_len(len) };

            weak_try!(maybe!(reader.read_exact(&mut buf)));

            let mut hasher = Hasher::new();
            hasher.update(&len_bytes);
            hasher.update(&buf);
            let crc_actual =
                (hasher.finalize() ^ 0xFF).to_le_bytes();

            if crc_actual != crc_expected {
                return;
            }

            write_batch.push(buf);
        }

        self.read_buffer.insert(lsn, (idx, write_batch));
    }
}

fn read_array<const LEN: usize>(
    mut reader: impl io::Read,
) -> io::Result<[u8; LEN]> {
    let mut buf: [u8; LEN] =
        unsafe { MaybeUninit::uninit().assume_init() };
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

impl ShardedLog {
    /// Write a batch of buffers to the sharded log.
    /// Returns the logical sequence number that they
    /// will be recoverable at after your next call
    /// to `flush`. Writes the batch into a `BufWriter`
    /// in front of the sharded log file, which will
    /// be flushed on Drop, but can also be flushed
    /// using the `flush` method.
    pub fn write_batch<B: AsRef<[u8]>>(
        &self,
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
    pub fn reservation(&self) -> Reservation<'_> {
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

    /// Flushes and fsyncs in-memory shard data that
    /// have been written to directly from this instance
    /// of `ShardedLog`, but skipping shards that have
    /// been written to by others. To flush all shards,
    /// use `flush_all`.
    pub fn flush(&self) -> io::Result<()> {
        for shard in &*self.shards {
            if shard.dirty.load(Ordering::Acquire) {
                let mut file =
                    shard.file_mu.lock().unwrap();
                if shard.dirty.load(Ordering::Acquire) {
                    fallible!(file.flush());
                    fallible!(file.get_mut().sync_all());
                    shard
                        .dirty
                        .store(false, Ordering::Release);
                }
            }
        }
        Ok(())
    }

    /// Delete all logs in the system
    pub fn purge(&self) -> io::Result<()> {
        let mut buffers = vec![];
        for shard in &*self.shards {
            let buffer = shard.file_mu.lock().unwrap();
            buffers.push(buffer);
        }

        for buffer in &mut buffers {
            fallible!(buffer.flush());

            let file = buffer.get_mut();
            fallible!(file.seek(io::SeekFrom::Start(0)));
            fallible!(file.set_len(0));
            fallible!(file.sync_all());
        }

        for shard in &*self.shards {
            shard.dirty.store(false, Ordering::Release);
        }

        // NB: buffers holds mutexes which must be held open until
        // after all dirty flags are clear
        drop(buffers);

        Ok(())
    }

    fn get_shard(&self) -> MutexGuard<'_, BufWriter<File>> {
        let len = self.shards.len();
        for i in 0..len {
            // walk backwards to avoid creating
            // contention for the very next write
            let idx = self.idx.wrapping_sub(i) % len;

            if let Ok(shard) =
                self.shards[idx].file_mu.try_lock()
            {
                self.shards[idx]
                    .dirty
                    .store(true, Ordering::Release);
                return shard;
            }
        }

        let ret =
            self.shards[self.idx].file_mu.lock().unwrap();

        // NB: dirty must be set after locking shard
        // to properly synchronize with flush's unset
        self.shards[self.idx]
            .dirty
            .store(true, Ordering::Release);

        ret
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
    let crc: [u8; 4] =
        (hasher.finalize() ^ 0xFF).to_le_bytes();

    fallible!(shard.write_all(&crc));
    fallible!(shard.write_all(&size_bytes));
    fallible!(shard.write_all(&lsn_bytes));

    for buf_i in write_batch {
        let buf = buf_i.as_ref();
        let crc_bytes: [u8; 4];
        let len_bytes: [u8; 8] =
            (buf.len() as u64).to_le_bytes();

        let mut hasher = Hasher::new();
        hasher.update(&len_bytes);
        hasher.update(&buf);
        crc_bytes =
            (hasher.finalize() ^ 0xFF).to_le_bytes();

        fallible!(shard.write_all(&crc_bytes));
        fallible!(shard.write_all(&len_bytes));
        fallible!(shard.write_all(buf));
    }

    Ok(lsn)
}

#[test]
fn concurrency() {
    let n_threads = 16_usize;
    let n_ops_per_thread = 10 * 1024;

    static CONCURRENCY_TEST_COUNTER: AtomicU64 =
        AtomicU64::new(0);

    let config = Config {
        shards: (n_threads / 4).max(1) as u8,
        path: "test_concurrency".into(),
        ..Default::default()
    };

    config.purge().unwrap();

    let log = Arc::new(config.create().unwrap());

    let barrier =
        Arc::new(std::sync::Barrier::new(n_threads + 1));
    let mut threads = vec![];
    for _ in 0..n_threads {
        let barrier = barrier.clone();
        let log = log.clone();

        let thread = std::thread::spawn(move || {
            barrier.wait();

            let mut successes = 0;
            while successes < n_ops_per_thread {
                let old_value = CONCURRENCY_TEST_COUNTER
                    .load(Ordering::Acquire);

                std::thread::yield_now();

                let reservation = log.reservation();

                std::thread::yield_now();

                let cas_res = CONCURRENCY_TEST_COUNTER
                    .compare_exchange(
                        old_value,
                        old_value + 1,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    );

                if cas_res.is_ok() {
                    let value = old_value.to_le_bytes();
                    reservation
                        .write_batch(&[value])
                        .unwrap();
                    successes += 1;
                } else {
                    reservation.abort().unwrap();
                }
            }
        });

        threads.push(thread);
    }

    barrier.wait();

    for thread in threads.into_iter() {
        thread.join().unwrap();
    }

    drop(log);

    let mut iter = config.recover().unwrap();

    let mut successes = 0;
    while successes < n_threads * n_ops_per_thread {
        if let Some(next) = iter.next().unwrap().pop() {
            let value = u64::from_le_bytes(
                next.try_into().unwrap(),
            );
            assert_eq!(value, successes as u64);
            successes += 1;
        }
    }

    drop(iter);

    let _ = std::fs::remove_dir_all(&config.path);
}
