# gorilla-codec
Implements Facebook Gorilla-like compression for floating-point values described in the following paper: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf

A more informal explanation of the algorithm can be found here: https://blog.acolyer.org/2016/05/03/gorilla-a-fast-scalable-in-memory-time-series-database/#:~:text=The%20values%20are%20then%20encoded%20as%20follows%3A

Existing libraries:
* https://github.com/mheffner/rust-gorilla-tsdb
* https://github.com/jeromefroe/tsz-rs (standard)'
* https://github.com/toddboom/tsx.rs
* https://github.com/iffyio/gorilla-compression

Other algorithms:
* [q_compress](https://crates.io/crates/q_compress) - alternative XOR encoding codec. This ahieves a higher compression rate against gzip, snappy, and zstd. Post [here](https://www.reddit.com/r/rust/comments/surtee/q_compress_07_still_has_35_higher_compression/)
* [compressed_vec](https://docs.rs/compressed_vec/latest/compressed_vec/) compresses data and allows processing directly on the compressed representation
* [fpzip](https://computing.llnl.gov/projects/fpzip) - memory-efficient algorithm from 2006

To determine the (approximately) best approach to encode floating point numbers, we record the following evaluation measures:
- Encoding speed
- Compression ratio (old file size / new file size)
- Decoding speed

We will evaluate on two types of datasets pulled from Sisu's dataframes:
- Time-series floating point values
- Non-time-series floating point values

We will compare the following compression libraries:
- [gibbon](https://docs.rs/gibbon/0.1.3/gibbon/)
- [tsz](https://docs.rs/tsz/0.1.0/tsz/) <- Rust's gorilla library
- [compressed_vec](https://docs.rs/compressed_vec/latest/compressed_vec/) (an updated version does not exist, skip for now until a new maintainer is found)
- [q_compress](https://crates.io/crates/q_compress)
