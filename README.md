# gorilla-codec
Implements Facebook Gorilla-like compression for floating-point values described in the following paper: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf

A more informal explanation of the algorithm can be found here: https://blog.acolyer.org/2016/05/03/gorilla-a-fast-scalable-in-memory-time-series-database/#:~:text=The%20values%20are%20then%20encoded%20as%20follows%3A

Existing libraries:
* https://github.com/mheffner/rust-gorilla-tsdb
* https://github.com/jeromefroe/tsz-rs

Other algorithms:
* [q_compress](https://crates.io/crates/q_compress) - alternative XOR encoding codec. This ahieves a higher compression rate against gzip, snappy, and zstd. Post [here](https://www.reddit.com/r/rust/comments/surtee/q_compress_07_still_has_35_higher_compression/).
* [compressed_vec](https://docs.rs/compressed_vec/latest/compressed_vec/) compresses data and allows processing directly on the compressed representation.
* [fpzip](https://computing.llnl.gov/projects/fpzip)
