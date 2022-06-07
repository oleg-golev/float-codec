# float-codec
Compares different compression algorithms on floating point numbers.

Tested algorithms:
* [Facebook Gorilla](https://github.com/jeromefroe/tsz-rs)
* [q_compress](https://crates.io/crates/q_compress) - alternative codec with delta-encoding. This ahieves a higher compression rate against gzip, snappy, and zstd. Post [here](https://www.reddit.com/r/rust/comments/surtee/q_compress_07_still_has_35_higher_compression/)

To Explore:
* [compressed_vec](https://docs.rs/compressed_vec/latest/compressed_vec/) compresses data and allows processing directly on the compressed representation
* [fpzip](https://computing.llnl.gov/projects/fpzip) - memory-efficient algorithm from 2006
* [gibbon](https://docs.rs/gibbon/0.1.3/gibbon/)
* [zfp](https://crates.io/crates/zfp-sys)

To determine the (approximately) best approach to encode floating point numbers, we record the following evaluation measures:
- Compression ratio (old file size / new file size)
- Encoding speed
- Decoding speed
