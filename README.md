# fs_usage_summary

OS X has a thing called `fs_usage`. This is a thing to try to summarize the
output into something scrutable.

Usage:

```sh
cargo build --release
sudo fs_usage -f diskio -w | ./target/release/fs_usage_summary -
```
