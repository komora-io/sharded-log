use sharded_log::Config;

fn main() {
    let config = Config {
        path: "path/to/logs".into(),
        ..Default::default()
    };

    config.purge().unwrap();

    let sharded_log = config.create().unwrap();

    sharded_log
        .write_batch(&[b"a", b"b", b"c", b"d"])
        .unwrap();

    sharded_log.flush().unwrap();

    for write_batch in config.recover().unwrap() {
        println!("got batch: {:?}", write_batch);
        assert_eq!(
            write_batch,
            vec![b"a", b"b", b"c", b"d"]
        );
    }

    let _ = std::fs::remove_dir_all("path");
}
