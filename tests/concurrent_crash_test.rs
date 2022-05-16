use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use sharded_log::Config;

#[test]
fn concurrent_crash() {
    let n_threads = 16_usize;
    let n_ops_per_thread = 128;

    static CONCURRENCY_TEST_COUNTER: AtomicU64 =
        AtomicU64::new(0);

    let config = Config {
        shards: (n_threads / 4).max(1) as u8,
        path: "test_concurrency".into(),
        ..Default::default()
    };

    config.purge().unwrap();

    let log = Arc::new(config.create().unwrap());

    let all_successes = Arc::new(AtomicU64::new(0));

    let barrier =
        Arc::new(std::sync::Barrier::new(n_threads + 1));
    let mut threads = vec![];
    for _ in 0..n_threads {
        let barrier = barrier.clone();
        let log = log.clone();
        let all_successes = all_successes.clone();

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
                    all_successes
                        .fetch_add(1, Ordering::Release);
                } else {
                    reservation.abort().unwrap();
                }
            }
        });

        threads.push(thread);
    }

    barrier.wait();

    while CONCURRENCY_TEST_COUNTER.load(Ordering::Acquire)
        < (n_ops_per_thread * n_threads) as u64
    {
        std::hint::spin_loop();
    }

    // trigger fault
    fault_injection::FAULT_INJECT_COUNTER
        .store(1, Ordering::Release);

    for thread in threads.into_iter() {
        if thread.join().is_err() {
            println!("a thread panicked");
        }
    }

    drop(log);

    let mut iter = config.recover().unwrap();

    let mut successes = 0;
    while successes < all_successes.load(Ordering::Acquire)
    {
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
