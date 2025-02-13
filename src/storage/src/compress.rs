pub enum CompressArgo {
    ZSTD,
    Snappy,
    LZ4,
}

pub trait Compressor: Send + Sync + 'static {
    fn name(&self) -> String;

    /// CompressBound returns the worst case size needed for a destination buffer
    fn compress_bound(&self, len: usize) -> usize;

    /// compress data
    fn compress(&self, data: &[u8]) -> Vec<u8>;

    /// decompress data
    fn decompress(&self, data: &[u8]) -> Vec<u8>;
}

impl Compressor for Box<dyn Compressor> {
    fn name(&self) -> String {
        (**self).name()
    }

    fn compress_bound(&self, len: usize) -> usize {
        (**self).compress_bound(len)
    }

    fn compress(&self, data: &[u8]) -> Vec<u8> {
        (**self).compress(data)
    }

    fn decompress(&self, data: &[u8]) -> Vec<u8> {
        (**self).decompress(data)
    }
}
