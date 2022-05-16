# sharded-log

A batch-oriented multi-threaded sharded log for workloads
that occasionally flush logs into some other system.

Built specifically to complement the [marble](https://crates.io/crates/marble)
on-disk garbage-collecting object store by buffering updates before
flushing them to storage. Intended to be used in a "rolling" manner
where you create a new `ShardedLog` instance and swap it out
while serving concurrent write requests, so that flushing to
secondary storage can happen in the background, followed by a removal
of the sharded log directory after it's stable.

```rust
use sharded_log::Config;

let config = Config {
    path: "path/to/logs".into(),
    ..Default::default()
};

// purge
config.purge().unwrap();

let sharded_log = config.create().unwrap();

sharded_log.write_batch(&[b"a", b"b", b"c", b"d"]).unwrap();

for write_batch in config.recover().unwrap() {
  // recover contents to secondary storage
  assert_eq!(write_batch, vec![b"a", b"b", b"c", b"d"]);
}
```
