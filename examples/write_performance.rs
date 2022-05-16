use sharded_log::Config;
use std::sync::{Arc, Barrier};

fn main() {
    let number_of_threads = 16;
    let buffers_per_batch = 8;
    let buffer_size = 512;
    let batches_per_thread = 128 * 1024;

    let config = Config {
        path: "write_performance".into(),
        ..Default::default()
    };

    config.purge().unwrap();

    let log = Arc::new(config.create().unwrap());

    let barrier =
        Arc::new(Barrier::new(number_of_threads + 1));

    let mut threads = vec![];

    let before = std::time::Instant::now();

    for _ in 0..number_of_threads {
        let barrier = barrier.clone();
        let log = log.clone();

        let thread = std::thread::spawn(move || {
            barrier.wait();

            for _ in 0..batches_per_thread {
                let mut batch =
                    Vec::with_capacity(buffers_per_batch);
                for _ in 0..buffers_per_batch {
                    let mut buffer =
                        Vec::with_capacity(buffer_size);
                    unsafe {
                        buffer.set_len(buffer_size);
                    }
                    batch.push(buffer);
                }
                log.write_batch(&batch).unwrap();
            }
        });

        threads.push(thread);
    }

    barrier.wait();

    for thread in threads.into_iter() {
        thread.join().unwrap();
    }

    log.flush().unwrap();

    let bytes_written = number_of_threads
        * batches_per_thread
        * buffers_per_batch
        * buffer_size;
    let throughput = bytes_written as u128
        / before.elapsed().as_micros();

    println!("wrote {} megabytes per second, {} threads, {} batches per thread, {} buffers per batch, {} bytes per buffer",
        throughput,
        number_of_threads,
        batches_per_thread,
        buffers_per_batch,
        buffer_size
    );

    let _ = std::fs::remove_dir_all("write_performance");
}
