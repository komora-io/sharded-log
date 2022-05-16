//! Shards batches of writes across multiple log files
//! for high throughput and low contention. The general
//! usage pattern is:
//!
//! 1. recover, pass recovered data to downstream storage
//! 2. delete old logs
//! 3. begin writing to the sharded logs

macro_rules! weak_try {
    ($e:expr) => {{
        match $e {
            Ok(ok) => ok,
            _ => return,
        }
    }};
}

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
    pub fn recover(&self) -> io::Result<RecoveryIterator> {
        fallible!(fs::create_dir_all(
            self.path.join(SUBDIR)
        ));

        let _ =
            File::create(self.path.join(SUBDIR).join(WARN));

        let mut file_opts = OpenOptions::new();
        file_opts.create(true).read(true).write(true);

        let mut readers = vec![];

        for idx in 0..self.shards {
            let path = self
                .path
                .join(SUBDIR)
                .join(idx.to_string());

            let file = fallible!(file_opts.open(path));
            fallible!(file.try_lock_exclusive());
            readers.push(BufReader::new(file));
        }

        let mut ret = RecoveryIterator {
            done: false,
            config: self.clone(),
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
    read_buffer:
        BTreeMap<u64, io::Result<(usize, Vec<Vec<u8>>)>>,
    last_shard: Option<usize>,
    done: bool,
    config: Config,
}

impl Iterator for RecoveryIterator {
    type Item = io::Result<(u64, Vec<Vec<u8>>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.next_inner();
        if ret.is_none() {
            self.done = true;
        }
        ret
    }
}

impl RecoveryIterator {
    fn next_inner(
        &mut self,
    ) -> Option<io::Result<(u64, Vec<Vec<u8>>)>> {
        if let Some(last_idx) = self.last_shard {
            self.tick(last_idx);
        }

        let lsn = self.next_expected_lsn;
        let next_res = self
            .read_buffer
            .remove(&self.next_expected_lsn)?;

        match next_res {
            Ok((idx, buf)) => {
                self.next_expected_lsn += 1;
                self.last_shard = Some(idx);
                Some(Ok((lsn, buf)))
            }
            Err(e) => Some(Err(e)),
        }
    }

    fn tick(&mut self, idx: usize) {
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

        self.read_buffer
            .insert(lsn, Ok((idx, write_batch)));
    }

    /// After iterating over the RecoveryIterator and
    /// applying the recovered logs to downstream storage,
    /// you can delete the logs and create a new writer.
    pub fn truncate_logs_and_start_writing(
        self,
    ) -> io::Result<ShardedLog> {
        if !self.done {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "RecoveryIterator::truncate_logs_and_start_writing \
                called before the log has been iterated over. Please \
                iterate over it first to recover the previous end of \
                the log before writing new data."
            ));
        }

        let mut shards = vec![];

        for reader in self.readers {
            let mut file = reader.into_inner();
            file.seek(io::SeekFrom::Start(0))?;
            file.set_len(0)?;
            shards.push(Shard {
                file_mu: Mutex::new(
                    BufWriter::with_capacity(
                        self.config
                            .in_memory_buffer_per_log,
                        file,
                    ),
                ),
                dirty: false.into(),
            })
        }

        Ok(ShardedLog {
            shards: shards.into(),
            idx: 0,
            idx_counter: Arc::new(AtomicUsize::new(1)),
            next_lsn: Arc::new(0.into()),
            config: self.config,
        })
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
    /// to `flush`.
    ///
    /// If this function returns an error, it is
    /// possible that the log is in a
    /// TODO
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

    let _ = std::fs::remove_dir_all(&config.path);

    let mut recovery = config.recover().unwrap();

    for _ in &mut recovery {}

    let log = Arc::new(
        recovery.truncate_logs_and_start_writing().unwrap(),
    );

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

                let reservation = log.reservation();

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
        if let Some(next) =
            iter.next().unwrap().unwrap().1.pop()
        {
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
