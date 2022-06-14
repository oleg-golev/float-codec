use byteorder::{ByteOrder, LittleEndian};
use core::panic;
use data as sisu_data;
use lzzzz::{lz4, lz4_hc, lz4f};
use sisu_data::{Page, PageBuilder};
use std::fs::File;
use std::io::{self, prelude::*, BufReader, BufWriter, Write};
use std::ops::Add;
use std::time::{Duration, Instant};
use std::vec::Vec;
use tsz::decode::Error as TszError;
use tsz::stream::{BufferedReader, BufferedWriter};
use tsz::{DataPoint, Decode, Encode, StdDecoder, StdEncoder};

const SET: &str = "floats";
const DATA: &str = "floats.txt";
const PATH: &str = "./data/floats.txt";

const PAGE_BYTES: u32 = 65535;

fn main() -> io::Result<()> {
    // prepare to buffer-read the file
    let file = File::open(PATH).unwrap();
    let reader = BufReader::new(file);

    // read line-by-line and move all floats into an in-memory vector
    let mut vec: Vec<f64> = Vec::new();
    for line in reader.lines() {
        let num: f64 = line.unwrap().parse().unwrap();
        vec.push(num);
    }
    let num_floats = vec.len();

    // convert the data to bytes
    let mut buf = vec![0_u8; vec.len() * 8];
    LittleEndian::write_f64_into(&vec, &mut buf);
    let num_bytes = buf.len();

    // --------------------------------- //
    // TEST DIFFERENT CODEC METHODS HERE //
    // --------------------------------- //

    // q_compress
    // https://crates.io/crates/q_compress
    // https://github.com/mwlon/quantile-compression
    test_q_compress(&vec, num_floats, num_bytes);
    println!("q_compress test done");

    // zstd
    // https://docs.rs/zstd/latest/zstd/
    // https://github.com/gyscos/zstd-rs
    test_zstd(&vec, num_floats, num_bytes);
    println!("zstd test done");

    // tsz
    // https://docs.rs/tsz/latest/tsz/
    // https://github.com/jeromefroe/tsz-rs
    test_tsz(&vec, num_floats, num_bytes);
    println!("tsz test done");

    // snap
    // https://lib.rs/crates/snap
    test_snap(&vec, num_floats, num_bytes);
    println!("snap test done");

    // // zfp
    // // https://crates.io/crates/zfp-sys
    // test_zfp(&mut vec, num_floats, num_bytes);
    // println!("zfp test done");

    // lz4
    test_lzzzz(&mut vec, num_floats, num_bytes);
    println!("lzzzz test done");

    // // gorilla
    // test_gorilla(&mut vec, num_bytes);tes
    test_gorilla(&mut vec, num_bytes);

    // baseline
    test_baseline(&mut vec, num_floats, num_bytes);
    println!("baseline test done");

    Ok(())
}

// either runs successfully and prints evaluation results or panics
fn test_q_compress(vec_total: &[f64], og_num_floats: usize, og_num_bytes: usize) {
    let results_path = format!("results/{}/q_compress_{}", SET, DATA);
    let results_file = File::create(results_path).unwrap();
    let mut results_file = BufWriter::new(results_file);
    let mut compression_level = 0;

    while compression_level <= 12 {
        println!(
            "q_compress: compression level {} starting",
            compression_level
        );

        // stuff that gets updated on each chunk of data
        let mut total_encoding_time: Duration = Duration::ZERO;
        let mut total_decoding_time: Duration = Duration::ZERO;
        let mut total_compression_ratio = 0.0;
        let mut head = vec_total;
        let mut done = false;
        let mut chunks = 0;

        // loop that benchmarks the algorithm on each chunk of data
        while !done {
            let vec: &[f64];
            if (PAGE_BYTES / 8) as usize > head.len() {
                vec = head;
                done = true;
            } else {
                (vec, head) = head.split_at((PAGE_BYTES / 8) as usize);
            }

            // initialize the timer
            let timer = Instant::now();

            // specify the most efficient compression level / granularity depends on the data
            let bytes: Vec<u8> = q_compress::auto_compress(vec, compression_level);
            let compressed_num_bytes: usize = bytes.len();

            // record encoding speed
            let encoding_speed: Duration = timer.elapsed();

            // decompress and ensure that we encoded and decoded the same number of floating point values
            let recovered: Vec<f64> =
                q_compress::auto_decompress::<f64>(&bytes).expect("failed to decompress");
            let decompressed_num_floats = recovered.len();
            assert!(vec.len() == decompressed_num_floats);

            // record decoding speed
            let decoding_speed: Duration = timer.elapsed() - encoding_speed;

            // record compression ratio
            let compression_ratio = (vec.len() as f64) * 8.0 / (compressed_num_bytes as f64);

            total_encoding_time = total_encoding_time.add(encoding_speed);
            total_decoding_time = total_decoding_time.add(decoding_speed);
            total_compression_ratio += compression_ratio;
            chunks += 1;
        }

        // write results to file
        let results = format!(
            "Compression level: {}\n\
            Average Compression ratio: {}\n\
            Average Encoding speed: {:?}\n\
            Average Decoding speed: {:?}\n\
            Total Encoding speed: {:?}\n\
            Total Decoding speed: {:?}\n\n",
            compression_level,
            total_compression_ratio / chunks as f64,
            total_encoding_time.div_f64(chunks as f64),
            total_decoding_time.div_f64(chunks as f64),
            total_encoding_time,
            total_decoding_time,
        );
        write!(results_file, "{}", results).expect("write to q_compress results file failed");
        println!("q_compress: compression level {} done", compression_level);
        compression_level += 1;
    }
}

fn test_zstd(vec_total: &[f64], og_num_floats: usize, og_num_bytes: usize) {
    fn compress(data: &[f64], level: i32) -> Vec<u8> {
        // convert the data to bytes
        let mut buf = vec![0_u8; data.len() * 8];
        LittleEndian::write_f64_into(data, &mut buf);
        let mut data_bytes = bytebuffer::ByteBuffer::from_bytes(&buf);

        // compress
        let mut encoder = zstd::stream::Encoder::new(Vec::new(), level).unwrap();
        io::copy(&mut data_bytes, &mut encoder).unwrap();
        encoder.finish().unwrap()
    }
    fn decompress(vec: &[u8]) -> Vec<f64> {
        // convert the byte vector into a buffer
        let compressed_bytes = bytebuffer::ByteBuffer::from_bytes(vec);

        // decompress
        let mut decompressed_bytes = Vec::new();
        zstd::stream::copy_decode(compressed_bytes, &mut decompressed_bytes).unwrap();

        // convert bytes to f64
        let mut decompressed_floats = vec![0_f64; decompressed_bytes.len() / 8];
        LittleEndian::read_f64_into(&decompressed_bytes, &mut decompressed_floats);
        decompressed_floats
    }

    // initialize the results file
    let results_path = format!("results/{}/zstd_{}", SET, DATA);
    let results_file = File::create(results_path).unwrap();
    let mut results_file = BufWriter::new(results_file);

    // # define MINCLEVEL  -99
    // # define MAXCLEVEL   22
    let mut compression_level = -10;
    while compression_level <= 10 {
        // 22 is max it takes too long
        // stuff that gets updated on each chunk of data
        let mut total_encoding_time: Duration = Duration::ZERO;
        let mut total_decoding_time: Duration = Duration::ZERO;
        let mut total_compression_ratio = 0.0;
        let mut head = vec_total;
        let mut done = false;
        let mut chunks = 0;

        // loop that benchmarks the algorithm on each chunk of data
        while !done {
            let vec: &[f64];
            if (PAGE_BYTES / 8) as usize > head.len() {
                vec = head;
                done = true;
            } else {
                (vec, head) = head.split_at((PAGE_BYTES / 8) as usize);
            }

            println!("zstd: compression level {} starting", compression_level);

            // initialize the timer
            let timer = Instant::now();

            // specify the most efficient compression level / granularity depends on the data
            let compressed_vec = compress(vec, compression_level);
            let compressed_num_bytes = compressed_vec.len();

            // record encoding speed
            let encoding_speed: Duration = timer.elapsed();

            // decompress and ensure that we encoded and decoded the same number of floating point values
            let decompressed = decompress(&compressed_vec);
            let decompressed_num_floats = decompressed.len();
            assert!(vec.len() == decompressed_num_floats);

            // record compression ratio
            let compression_ratio = (vec.len() as f64) * 8.0 / (compressed_num_bytes as f64);

            // record decoding speed
            let decoding_speed: Duration = timer.elapsed() - encoding_speed;

            total_encoding_time = total_encoding_time.add(encoding_speed);
            total_decoding_time = total_decoding_time.add(decoding_speed);
            total_compression_ratio += compression_ratio;
            chunks += 1;
        }

        let results = format!(
            "Compression level: {}\n\
            Average Compression ratio: {}\n\
            Average Encoding speed: {:?}\n\
            Average Decoding speed: {:?}\n\
            Total Encoding speed: {:?}\n\
            Total Decoding speed: {:?}\n\n",
            compression_level,
            total_compression_ratio / chunks as f64,
            total_encoding_time.div_f64(chunks as f64),
            total_decoding_time.div_f64(chunks as f64),
            total_encoding_time,
            total_decoding_time,
        );
        write!(results_file, "{}", results).expect("write to zstd results file failed");
        println!("zstd: compression level {} done", compression_level);
        compression_level += 1;
    }
}

fn test_tsz(vec_total: &[f64], og_num_floats: usize, og_num_bytes: usize) {
    println!("tsz compression starting");

    // initialize the results file
    let results_path = format!("results/{}/tsz_{}", SET, DATA);
    let results_file = File::create(results_path).unwrap();
    let mut results_file = BufWriter::new(results_file);

    // stuff that gets updated on each chunk of data
    let mut total_encoding_time: Duration = Duration::ZERO;
    let mut total_decoding_time: Duration = Duration::ZERO;
    let mut total_compression_ratio = 0.0;
    let mut head = vec_total;
    let mut done = false;
    let mut chunks = 0;

    // loop that benchmarks the algorithm on each chunk of data
    while !done {
        let vec: &[f64];
        if (PAGE_BYTES / 8) as usize > head.len() {
            vec = head;
            done = true;
        } else {
            (vec, head) = head.split_at((PAGE_BYTES / 8) as usize);
        }

        // convert data to datapoints
        let default_timestamp: u64 = 1482892260;
        let w = BufferedWriter::new();
        let mut encoder = StdEncoder::new(default_timestamp, w);
        let mut datapoints = Vec::new();

        for num in vec {
            let dp = DataPoint::new(default_timestamp, *num);
            datapoints.push(dp);
        }

        // initialize the timer
        let timer = Instant::now();

        // compress
        for dp in &datapoints {
            encoder.encode(*dp);
        }
        let bytes = encoder.close();
        let compressed_num_bytes = bytes.len();

        // record encoding speed
        let encoding_speed: Duration = timer.elapsed();

        // decode
        let r = BufferedReader::new(bytes);
        let mut decoder = StdDecoder::new(r);
        let mut expected_datapoints = Vec::new();
        let mut done = false;
        loop {
            if done {
                break;
            }
            match decoder.next() {
                Ok(dp) => expected_datapoints.push(dp),
                Err(err) => {
                    if err == TszError::EndOfStream {
                        done = true;
                    } else {
                        panic!("Received an error from decoder: {:?}", err);
                    }
                }
            };
        }
        assert!(expected_datapoints.len() == vec.len());

        // record decoding speed
        let decoding_speed: Duration = timer.elapsed() - encoding_speed;

        // record compression ratio
        let compression_ratio = (datapoints.len() as f64 * 16.0) / (compressed_num_bytes as f64);
        total_encoding_time = total_encoding_time.add(encoding_speed);
        total_decoding_time = total_decoding_time.add(decoding_speed);
        total_compression_ratio += compression_ratio;
        chunks += 1;
    }

    let results = format!(
        "Average Compression ratio: {}\n\
        Average Encoding speed: {:?}\n\
        Average Decoding speed: {:?}\n\
        Total Encoding speed: {:?}\n\
        Total Decoding speed: {:?}\n\n",
        total_compression_ratio / chunks as f64,
        total_encoding_time.div_f64(chunks as f64),
        total_decoding_time.div_f64(chunks as f64),
        total_encoding_time,
        total_decoding_time,
    );
    write!(results_file, "{}", results).expect("write to tsz results file failed");
    println!("tsz compression done");
}

fn test_snap(vec_total: &[f64], og_num_floats: usize, og_num_bytes: usize) {
    fn compress(uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
        compressed.clear();
        let mut encoder = snap::write::FrameEncoder::new(compressed);
        encoder
            .write_all(uncompressed)
            .expect("snappy compression failed");
        encoder.flush().expect("could not flush snappy encoder");
        Ok(())
    }

    fn decompress(compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()> {
        decompressed.clear();
        snap::read::FrameDecoder::new(compressed)
            .read_to_end(decompressed)
            .expect("snappy decompression failed");
        Ok(())
    }

    // initialize the results file
    let results_path = format!("results/{}/snap_{}", SET, DATA);
    let results_file = File::create(results_path).unwrap();
    let mut results_file = BufWriter::new(results_file);

    println!("snap compression starting");

    // stuff that gets updated on each chunk of data
    let mut total_encoding_time: Duration = Duration::ZERO;
    let mut total_decoding_time: Duration = Duration::ZERO;
    let mut total_compression_ratio = 0.0;
    let mut head = vec_total;
    let mut done = false;
    let mut chunks = 0;

    // loop that benchmarks the algorithm on each chunk of data
    while !done {
        let vec: &[f64];
        if (PAGE_BYTES / 8) as usize > head.len() {
            vec = head;
            done = true;
        } else {
            (vec, head) = head.split_at((PAGE_BYTES / 8) as usize);
        }

        // initialize the timer
        let timer = Instant::now();

        // convert data to bytes and encode it
        let mut data_bytes = vec![0_u8; vec.len() * 8];
        LittleEndian::write_f64_into(vec, &mut data_bytes);

        // compress
        let mut compressed_bytes: Vec<u8> = Vec::new();
        compress(&data_bytes, &mut compressed_bytes).expect("could not compress with snappy");
        let compressed_num_bytes = compressed_bytes.len();

        // record encoding speed
        let encoding_speed: Duration = timer.elapsed();

        // decompress
        let mut decompressed_bytes = Vec::new();
        decompress(&compressed_bytes, &mut decompressed_bytes)
            .expect("could not decompress with snappy");

        // convert to floats
        let mut decompressed_floats = vec![0_f64; decompressed_bytes.len() / 8];
        LittleEndian::read_f64_into(&decompressed_bytes, &mut decompressed_floats);

        // ensure that we encoded and decoded the same number of floating point values
        let decompressed_num_floats = decompressed_floats.len();
        assert!(vec.len() == decompressed_num_floats);

        // record decoding speed
        let decoding_speed: Duration = timer.elapsed() - encoding_speed;

        // record compression ratio
        let compression_ratio = (data_bytes.len() as f64) / (compressed_num_bytes as f64);

        total_encoding_time = total_encoding_time.add(encoding_speed);
        total_decoding_time = total_decoding_time.add(decoding_speed);
        total_compression_ratio += compression_ratio;
        chunks += 1;
    }

    let results = format!(
        "Average Compression ratio: {}\n\
        Average Encoding speed: {:?}\n\
        Average Decoding speed: {:?}\n\
        Total Encoding speed: {:?}\n\
        Total Decoding speed: {:?}\n\n",
        total_compression_ratio / chunks as f64,
        total_encoding_time.div_f64(chunks as f64),
        total_decoding_time.div_f64(chunks as f64),
        total_encoding_time,
        total_decoding_time,
    );
    write!(results_file, "{}", results).expect("write to snap results file failed");
    println!("snap compression  done");
}

// fn test_zfp(vec_total: &mut [f64], og_num_floats: usize, og_num_bytes: usize) {
//     println!("zfp compression starting");

//     // stuff that gets updated on each chunk of data
//     let mut total_encoding_time = 0;
//     let mut total_decoding_time = 0;
//     let mut total_compression_ratio = 0.0;
//     let mut head = vec_total;
//     let mut done = false;
//     let mut chunks = 0;

//     // loop that benchmarks the algorithm on each chunk of data
//     while !done {
//         let vec: &[f64];
//         if (PAGE_BYTES / 8) as usize > head.len() {
//             vec = head;
//             done = true;
//         } else {
//             (vec, head) = head.split_at((PAGE_BYTES / 8) as usize);
//         }

//         let mut vec_clone = vec![0_f64; vec.len()];
//         vec_clone.clone_from_slice(vec);

//         /* allocate meta data for the data vector of floats */
//         let data_type = zfp_sys::zfp_type_zfp_type_double;
//         let len = vec.len();
//         let field = unsafe {
//             zfp_sys::zfp_field_1d(
//                 vec.as_mut_ptr() as *mut std::ffi::c_void,
//                 data_type,
//                 len as u32,
//             )
//         };

//         /* allocate meta data for a compressed stream */
//         let zfp =
//             unsafe { zfp_sys::zfp_stream_open(std::ptr::null_mut() as *mut zfp_sys::bitstream) };

//         /* set compression mode and parameters */
//         // unsafe { zfp_sys::zfp_stream_set_rate(zfp, 8.0, data_type, 1, 0) };
//         // Compression ratio: 7.99999949257665
//         // Encoding speed: 292.536271ms
//         // Decoding speed: 282.665824ms
//         unsafe { zfp_sys::zfp_stream_set_reversible(zfp) };

//         /* allocate buffer for compressed data */
//         let bufsize = unsafe { zfp_sys::zfp_stream_maximum_size(zfp, field) };
//         let mut buffer: Vec<u8> = vec![0; bufsize as usize];

//         /* associate bit stream with allocated buffer */
//         let stream =
//             unsafe { zfp_sys::stream_open(buffer.as_mut_ptr() as *mut std::ffi::c_void, bufsize) };
//         unsafe {
//             zfp_sys::zfp_stream_set_bit_stream(zfp, stream);
//             zfp_sys::zfp_stream_rewind(zfp);
//         }

//         // initialize the timer
//         let timer = Instant::now();

//         /* compress */
//         let compressed_num_bytes = unsafe { zfp_sys::zfp_compress(zfp, field) };
//         if compressed_num_bytes == 0 {
//             panic!("compression failed");
//         } else {
//             let original_size = len * std::mem::size_of::<f64>();
//             let ratio = (original_size as f64) / (compressed_num_bytes as f64);
//             println!(
//                 "bufsize: {} bytes, original size: {} bytes, compressed size: {} bytes, ratio: {}",
//                 bufsize, original_size, compressed_num_bytes, ratio
//             );
//         }

//         // record encoding speed
//         let encoding_speed: Duration = timer.elapsed();

//         /* decompress */
//         unsafe { zfp_sys::zfp_stream_rewind(zfp) };
//         let compressed_size = unsafe { zfp_sys::zfp_decompress(zfp, field) };
//         if compressed_size == 0 {
//             panic!("decompression failed");
//         } else {
//             println!("ret: {}", compressed_size);
//         }

//         // record decoding speed
//         let decoding_speed: Duration = timer.elapsed() - encoding_speed;

//         // record compression ratio
//         let compression_ratio = (vec.len() as f64) / (compressed_num_bytes as f64);

//         /* clean up */
//         unsafe {
//             zfp_sys::zfp_field_free(field);
//             zfp_sys::zfp_stream_close(zfp);
//             zfp_sys::stream_close(stream);
//         }

//         total_encoding_time += encoding_speed.as_micros();
//         total_decoding_time += decoding_speed.as_micros();
//         total_compression_ratio += compression_ratio;
//         chunks += 1;
//     }

//     let results_path = format!("results/{}/zfp_{}", SET, DATA);
//     let results_file = File::create(results_path).unwrap();
//     let mut results_file = BufWriter::new(results_file);

//     // write results to file
//     let results = format!(
//         "Average Compression ratio: {}\n\
//         Average Encoding speed: {:?}\n\
//         Average Decoding speed: {:?}\n\n",
//         total_compression_ratio / chunks as f64,
//         Duration::from_micros((total_encoding_time as f64 / chunks as f64) as u64),
//         Duration::from_micros((total_decoding_time as f64 / chunks as f64) as u64),
//     );
//     write!(results_file, "{}", results).expect("write to zfp results file failed");
//     println!("zfp compression done");
// }

fn test_lzzzz(vec_total: &[f64], og_num_floats: usize, og_num_bytes: usize) {
    // results file
    let results_path = format!("results/{}/lzzzz_{}", SET, DATA);
    let results_file = File::create(results_path).unwrap();
    let mut results_file = BufWriter::new(results_file);

    // stuff that gets updated on each chunk of data
    let mut total_encoding_time: Duration = Duration::ZERO;
    let mut total_decoding_time: Duration = Duration::ZERO;
    let mut total_compression_ratio = 0.0;
    let mut head = vec_total;
    let mut done = false;
    let mut chunks = 0;

    // loop that benchmarks the algorithm on each chunk of data
    while !done {
        let vec: &[f64];
        if (PAGE_BYTES / 8) as usize > head.len() {
            vec = head;
            done = true;
        } else {
            (vec, head) = head.split_at((PAGE_BYTES / 8) as usize);
        }

        // convert the data to bytes
        let mut vec_bytes = vec![0_u8; vec.len() * 8];
        LittleEndian::write_f64_into(&vec, &mut vec_bytes);
        let num_bytes = vec_bytes.len();

        // initialize the timer
        let timer = Instant::now();

        // LZ4 compression
        let mut comp_bytes = Vec::new();
        let num_compressed_bytes =
            lz4::compress_to_vec(&vec_bytes, &mut comp_bytes, lz4::ACC_LEVEL_DEFAULT).unwrap();

        // record encoding speed
        let encoding_speed: Duration = timer.elapsed();

        // LZ4/LZ4_HC decompression
        let mut decomp_bytes = vec![0_u8; vec.len() * 8];
        lz4::decompress(&comp_bytes, &mut decomp_bytes).unwrap();
        let mut decomp_floats = vec![0_f64; decomp_bytes.len() / 8];
        LittleEndian::read_f64_into(&decomp_bytes, &mut decomp_floats);
        // assert!(decomp_floats.len() == og_num_floats);
        // assert!(decomp_bytes.len() == og_num_bytes);

        // record decoding speed
        let decoding_speed: Duration = timer.elapsed() - encoding_speed;

        // record compression ratio
        let compression_ratio = (num_bytes as f64) / (num_compressed_bytes as f64);

        total_encoding_time = total_encoding_time.add(encoding_speed);
        total_decoding_time = total_decoding_time.add(decoding_speed);
        total_compression_ratio += compression_ratio;
        chunks += 1;
    }

    let results = format!(
        "Average Compression ratio: {}\n\
        Average Encoding speed: {:?}\n\
        Average Decoding speed: {:?}\n\
        Total Encoding speed: {:?}\n\
        Total Decoding speed: {:?}\n\n",
        total_compression_ratio / chunks as f64,
        total_encoding_time.div_f64(chunks as f64),
        total_decoding_time.div_f64(chunks as f64),
        total_encoding_time,
        total_decoding_time,
    );
    write!(results_file, "{}", results).expect("write to lz4 results file failed");
    println!("lz4 compression done");

    // ================================================================================

    // stuff that gets updated on each chunk of data
    total_encoding_time = Duration::ZERO;
    total_decoding_time = Duration::ZERO;
    total_compression_ratio = 0.0;
    head = vec_total;
    done = false;
    chunks = 0;

    // loop that benchmarks the algorithm on each chunk of data
    while !done {
        // initialize the timer
        let timer = Instant::now();

        let vec: &[f64];
        if (PAGE_BYTES / 8) as usize > head.len() {
            vec = head;
            done = true;
        } else {
            (vec, head) = head.split_at((PAGE_BYTES / 8) as usize);
        }

        let mut vec_bytes = vec![0_u8; vec.len() * 8];
        LittleEndian::write_f64_into(&vec, &mut vec_bytes);

        // LZ4_HC compression
        let mut comp_bytes = Vec::new();
        let num_compressed_bytes =
            lz4_hc::compress_to_vec(&vec_bytes, &mut comp_bytes, lz4_hc::CLEVEL_DEFAULT).unwrap();

        // record encoding speed
        let encoding_speed: Duration = timer.elapsed();

        // LZ4/LZ4_HC decompression
        let mut decomp_bytes = vec![0_u8; vec.len() * 8];
        lz4::decompress(&comp_bytes, &mut decomp_bytes).unwrap();
        let mut decomp_floats = vec![0_f64; decomp_bytes.len() / 8];
        LittleEndian::read_f64_into(&decomp_bytes, &mut decomp_floats);

        // record decoding speed
        let decoding_speed: Duration = timer.elapsed() - encoding_speed;

        // record compression ratio
        let compression_ratio = (vec_bytes.len() as f64) / (num_compressed_bytes as f64);

        total_encoding_time = total_encoding_time.add(encoding_speed);
        total_decoding_time = total_decoding_time.add(decoding_speed);
        total_compression_ratio += compression_ratio;
        chunks += 1;
    }

    // write results to file
    let results = format!(
        "Average Compression ratio: {}\n\
        Average Encoding speed: {:?}\n\
        Average Decoding speed: {:?}\n\
        Total Encoding speed: {:?}\n\
        Total Decoding speed: {:?}\n\n",
        total_compression_ratio / chunks as f64,
        total_encoding_time.div_f64(chunks as f64),
        total_decoding_time.div_f64(chunks as f64),
        total_encoding_time,
        total_decoding_time,
    );
    write!(results_file, "{}", results).expect("write to lz4 results file failed");
    println!("lzzzz compression LZ4_HZ done");

    // ================================================================================

    // stuff that gets updated on each chunk of data
    total_encoding_time = Duration::ZERO;
    total_decoding_time = Duration::ZERO;
    total_compression_ratio = 0.0;
    head = vec_total;
    done = false;
    chunks = 0;

    // loop that benchmarks the algorithm on each chunk of data
    while !done {
        // initialize the timer
        let timer = Instant::now();

        let vec: &[f64];
        if (PAGE_BYTES / 8) as usize > head.len() {
            vec = head;
            done = true;
        } else {
            (vec, head) = head.split_at((PAGE_BYTES / 8) as usize);
        }

        let mut vec_bytes = vec![0_u8; vec.len() * 8];
        LittleEndian::write_f64_into(&vec, &mut vec_bytes);

        // initialize the timer
        let timer = Instant::now();

        // LZ4F compression
        let prefs = lz4f::Preferences::default();
        let mut comp_bytes = Vec::new();
        let num_compressed_bytes =
            lz4f::compress_to_vec(&vec_bytes, &mut comp_bytes, &prefs).unwrap();

        // record encoding speed
        let encoding_speed: Duration = timer.elapsed();

        // LZ4F decompression
        let mut decomp_bytes = Vec::new();
        lz4f::decompress_to_vec(&comp_bytes, &mut decomp_bytes).unwrap();
        let mut decomp_floats = vec![0_f64; decomp_bytes.len() / 8];
        LittleEndian::read_f64_into(&decomp_bytes, &mut decomp_floats);

        // record decoding speed
        let decoding_speed: Duration = timer.elapsed() - encoding_speed;

        // record compression ratio
        let compression_ratio = (vec_bytes.len() as f64) / (num_compressed_bytes as f64);

        total_encoding_time = total_encoding_time.add(encoding_speed);
        total_decoding_time = total_decoding_time.add(decoding_speed);
        total_compression_ratio += compression_ratio;
        chunks += 1;
    }

    // write results to file
    let results = format!(
        " Average Compression ratio: {}\n\
        Average Encoding speed: {:?}\n\
        Average Decoding speed: {:?}\n\
        Total Encoding speed: {:?}\n\
        Total Decoding speed: {:?}\n\n",
        total_compression_ratio / chunks as f64,
        total_encoding_time.div_f64(chunks as f64),
        total_decoding_time.div_f64(chunks as f64),
        total_encoding_time,
        total_decoding_time,
    );
    write!(results_file, "{}", results).expect("write to lz4 results file failed");
    println!("lzzzz compression LZ4F done");
}

fn test_baseline(vec: &[f64], og_num_floats: usize, og_num_bytes: usize) {
    let mut total_encoding_time: Duration = Duration::ZERO;
    let mut total_decoding_time: Duration = Duration::ZERO;
    let mut chunks = 0;

    // encode
    let mut pages = Vec::new();
    let mut i = 0;
    let mut j = 0;
    let mut builder = sisu_data::page::NotNanFloatBuilder::with_capacity(65535);
    // initialize the timer
    let mut timer = Instant::now();
    while j < vec.len() {
        if i * 8 >= 65535 {
            let page = builder.build();
            pages.push(page);
            builder = sisu_data::page::NotNanFloatBuilder::with_capacity(65535);
            i = 0;
            chunks += 1;
            // record encoding speed
            let encoding_speed: Duration = timer.elapsed();
            timer = Instant::now();
            total_encoding_time = total_encoding_time.add(encoding_speed);
        }
        builder.push(data::macros::ordered_float::NotNan::new(vec[j]).unwrap());
        i += 1;
        j += 1
    }

    // decode
    // let mut counter = 0;
    for page in pages {
        timer = Instant::now();
        page.try_fold(0_f64, |acc, _obs| if true { Ok(acc) } else { Err(acc) })
            .unwrap();
        // counter += 1;
        let decoding_speed: Duration = timer.elapsed();
        total_decoding_time = total_decoding_time.add(decoding_speed);
    }
    // println!("{:?}", counter);

    // write results to file
    let results = format!(
        " Average Encoding speed: {:?}\n\
        Average Decoding speed: {:?}\n\\
        Total Encoding speed: {:?}\n\
        Total Decoding speed: {:?}\n\n",
        total_encoding_time.div_f64(chunks as f64),
        total_decoding_time.div_f64(chunks as f64),
        total_encoding_time,
        total_decoding_time,
    );
    // results file
    let results_path = format!("results/{}/baseline_{}", SET, DATA);
    let results_file = File::create(results_path).unwrap();
    let mut results_file = BufWriter::new(results_file);
    write!(results_file, "{}", results).expect("write to baseline results file failed");
    println!("baseline compression done");
}

fn test_gorilla(vec_total: &[f64], og_num_bytes: usize) {
    // results file
    let results_path = format!("results/{}/gorilla_{}", SET, DATA);
    let results_file = File::create(results_path).unwrap();
    let mut results_file = BufWriter::new(results_file);

    // stuff that gets updated on each chunk of data
    let mut total_encoding_time: Duration = Duration::ZERO;
    let mut total_decoding_time: Duration = Duration::ZERO;
    let mut total_compression_ratio = 0.0;
    let mut head = vec_total;
    let mut done = false;
    let mut chunks = 0;

    // loop that benchmarks the algorithm on each chunk of data
    while !done {
        let vec: &[f64];
        if (PAGE_BYTES / 8) as usize > head.len() {
            vec = head;
            done = true;
        } else {
            (vec, head) = head.split_at((PAGE_BYTES / 8) as usize);
        }
        // ------------------------------------
        let timer = Instant::now();
        // ------------------------------------

        let mut dst = Vec::new();
        gorilla_encode(vec, &mut dst);
        let num_compressed_bytes = dst.len();

        // ------------------------------------
        let encoding_speed: Duration = timer.elapsed();
        // ------------------------------------

        let mut decoded: Vec<f64> = Vec::new();
        gorilla_decode(&dst, &mut decoded);

        // ------------------------------------
        let decoding_speed: Duration = timer.elapsed() - encoding_speed;
        // ------------------------------------

        // record compression ratio
        let compression_ratio = ((vec.len() * 8) as f64) / (num_compressed_bytes as f64);

        // verify got same values back
        assert_eq!(decoded, vec);
        total_encoding_time = total_encoding_time.add(encoding_speed);
        total_decoding_time = total_decoding_time.add(decoding_speed);
        total_compression_ratio += compression_ratio;
        chunks += 1;
    }

    // write results to file
    let results = format!(
        "Average Compression ratio: {}\n\
        Average Encoding speed: {:?}\n\
        Average Decoding speed: {:?}\n\
        Total Encoding speed: {:?}\n\
        Total Decoding speed: {:?}\n\n",
        total_compression_ratio / chunks as f64,
        total_encoding_time.div_f64(chunks as f64),
        total_decoding_time.div_f64(chunks as f64),
        total_encoding_time,
        total_decoding_time,
    );
    write!(results_file, "{}", results).expect("write to gorilla2 results file failed");
    println!("gorilla2 compression done");
}

use bit_streamer::Writer;

pub fn gorilla_encode(src: &[f64], dst: &mut Vec<u8>) {
    let data = src;

    // Start by making a buffer that writes into a file called input_filename.gorilla
    let mut writer = Writer::new(dst);

    // First write the first value in full
    let mut previous_value = data[0].to_bits();

    // Initialize leading and trailing zeros
    let mut previous_leading_zeros: u32 = 64;
    let mut previous_trailing_zeros: u32 = 64;

    writer.write_bits(previous_value as u128, 64).unwrap();
    for &d in &data[1..] {
        let next_value = d.to_bits();
        let xor = next_value ^ previous_value;

        // Update previous values for next iteration
        previous_value = next_value;

        if xor == 0 {
            // If there is no difference from previous value, then we write a 0
            writer.write_bit(false).unwrap();
        } else {
            // Otherwise a 1, followed by more logic to show difference
            writer.write_bit(true).unwrap();

            let current_leading_zeros = xor.leading_zeros();
            let current_trailing_zeros = xor.trailing_zeros();

            // If block of meaningful bits is within previous meaningful bits
            if current_leading_zeros >= previous_leading_zeros
                && current_trailing_zeros >= previous_trailing_zeros
            {
                // Write a zero control bit followed by meaningful bits
                writer.write_bit(false).unwrap();
                writer
                    .write_bits(
                        xor.wrapping_shr(previous_trailing_zeros) as u128,
                        (64 - previous_leading_zeros - previous_trailing_zeros) as usize,
                    )
                    .unwrap();
            } else {
                // Otherwise, we write a 1 control bit, followed by the 5 bits of the number of
                // leading zeros, then 6 bits of the number of significant bits
                // Followed by the significant bits
                writer.write_bit(true).unwrap();
                writer.write_bits(current_leading_zeros as u128, 5).unwrap();

                let significant_bits = 64 - current_leading_zeros - current_trailing_zeros;
                writer
                    .write_bits((significant_bits - 1) as u128, 6)
                    .unwrap();
                writer
                    .write_bits(
                        xor.wrapping_shr(current_trailing_zeros) as u128,
                        significant_bits as usize,
                    )
                    .unwrap();
                previous_trailing_zeros = current_trailing_zeros;
                previous_leading_zeros = current_leading_zeros;
            }
        }
    }

    // Write an end marker that says it's a new value, with more meaningful bits, 0 leading zeros,
    // 64 significant values, then a total value of 0. This cannot happen so it's a safe end marker
    writer
        .write_bits(0b11_00000_111111 << (128 - 13), 128)
        .unwrap();
    writer.flush().unwrap();
}

use bit_streamer::Reader;
use std::convert::TryInto;

pub fn gorilla_decode(bytes: &[u8], values: &mut Vec<f64>) {
    // Initialize
    let mut leading_zeros: u128 = 0;
    let mut trailing_zeros: u128 = 0;

    // Initialize reader
    let mut reader = Reader::new(bytes);

    // Read the first value
    let mut previous_value = u64::from_be_bytes(
        reader
            .read_bytes(8)
            .unwrap()
            .try_into()
            .expect("Read wrong number of bytes"),
    );
    values.push(f64::from_bits(previous_value));

    // Now loop through entire file
    loop {
        let next_bit: bool;
        let eof_check = reader.read_bit();
        if eof_check.is_err() {
            // Break when we don't read anymore bits
            break;
        } else {
            next_bit = eof_check.unwrap();
        }
        if !next_bit {
            // If next bit is 0, then it's the same value as previously
            values.push(f64::from_bits(previous_value));
        } else {
            // next bit was 1 and there's a difference from last bit
            if reader.read_bit().unwrap() {
                // If control bit is 1, we get number of leading zeros from next 5 bits
                // then the length of meaningful XORed value in the next 6 bits
                leading_zeros = reader.read_bits(5).unwrap();
                trailing_zeros = 64 - leading_zeros - (reader.read_bits(6).unwrap() + 1);
            }
            let size = 64 - leading_zeros - trailing_zeros;
            let next_bits = reader.read_bits(size as usize).unwrap() as u64;

            // Check for end marker
            if leading_zeros == 0 && size == 64 && next_bits == 0 {
                break;
            }

            previous_value ^= next_bits << trailing_zeros;
            values.push(f64::from_bits(previous_value));
        }
    }
}
