use byteorder::{ByteOrder, LittleEndian};
use std::fs::{self, File};
use std::io::{self, prelude::*, BufReader, BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use std::vec::Vec;
use tsz::decode::Error;
use tsz::stream::{BufferedReader, BufferedWriter};
use tsz::{DataPoint, Decode, Encode, StdDecoder, StdEncoder};

fn main() -> io::Result<()> {
    // get file size
    let path = Path::new("data/floats.txt");
    let metadata = fs::metadata(path)?;
    let num_bytes: usize = metadata.len() as usize;

    // prepare to buffer-read the file
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);

    // read line-by-line and move all floats into an in-memory vector
    let mut vec: Vec<f64> = Vec::new();
    for line in reader.lines() {
        let num: f64 = line.unwrap().parse().unwrap();
        vec.push(num);
    }
    let num_floats = vec.len();

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
    println!("zstd test done");

    Ok(())
}

// either runs successfully and prints evaluation results or panics
fn test_q_compress(vec: &[f64], og_num_floats: usize, og_num_bytes: usize) {
    let results_path = "results/q_compress";
    let results_file = File::create(results_path).unwrap();
    let mut results_file = BufWriter::new(results_file);
    let mut compression_level = 0;
    while compression_level <= 12 {
        println!(
            "q_compress: compression level {} starting",
            compression_level
        );

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
        let compressed_num_floats = recovered.len();
        assert!(og_num_floats == compressed_num_floats);

        // record decoding speed
        let decoding_speed: Duration = timer.elapsed() - encoding_speed;

        // write results to file
        let results = format!(
            "Compression level: {}\n\
            Compression ratio: {}\n\
            Encoding speed: {:?}\n\
            Decoding speed: {:?}\n\n",
            compression_level,
            (og_num_bytes as f64) / (compressed_num_bytes as f64),
            encoding_speed,
            decoding_speed,
        );
        write!(results_file, "{}", results).expect("write to q_compress results file failed");
        println!("q_compress: compression level {} done", compression_level);
        compression_level += 1;
    }
}

fn test_zstd(vec: &Vec<f64>, og_num_floats: usize, og_num_bytes: usize) {
    fn compress(data: &Vec<f64>, level: i32) -> Vec<u8> {
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
    let results_path = "results/zstd";
    let results_file = File::create(results_path).unwrap();
    let mut results_file = BufWriter::new(results_file);

    // # define MINCLEVEL  -99
    // # define MAXCLEVEL   22
    let mut compression_level = 0;
    while compression_level <= 22 {
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
        assert!(og_num_floats == decompressed_num_floats);

        // record decoding speed
        let decoding_speed: Duration = timer.elapsed() - encoding_speed;

        // write results to file
        let results = format!(
            "Compression level: {}\n\
            Compression ratio: {}\n\
            Encoding speed: {:?}\n\
            Decoding speed: {:?}\n\n",
            compression_level,
            (og_num_bytes as f64) / (compressed_num_bytes as f64),
            encoding_speed,
            decoding_speed,
        );
        write!(results_file, "{}", results).expect("write to zstd results file failed");
        println!("zstd: compression level {} done", compression_level);
        compression_level += 1
    }
}

fn test_tsz(vec: &Vec<f64>, og_num_floats: usize, og_num_bytes: usize) {
    println!("tsz compression starting");

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
                if err == Error::EndOfStream {
                    done = true;
                } else {
                    panic!("Received an error from decoder: {:?}", err);
                }
            }
        };
    }
    assert!(expected_datapoints.len() == og_num_floats);

    // record decoding speed
    let decoding_speed: Duration = timer.elapsed() - encoding_speed;

    // write results to file
    let results_path = "results/tsz";
    let results_file = File::create(results_path).unwrap();
    let mut results_file = BufWriter::new(results_file);
    let results = format!(
        "Compression ratio: {}\n\
        Encoding speed: {:?}\n\
        Decoding speed: {:?}\n\n",
        (og_num_bytes as f64) / (compressed_num_bytes as f64),
        encoding_speed,
        decoding_speed,
    );
    write!(results_file, "{}", results).expect("write to zstd results file failed");
    println!("tsz compression done");
}
