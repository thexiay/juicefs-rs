pub trait Compressor: Send + Sync + 'static {
    fn name(&self) -> String;

    /// Compute max compressed length
    fn compress_bound(len: usize) -> usize;

    /// compress data
    fn compress(&self, data: &[u8]) -> Vec<u8>;

    /// decompress data
    fn decompress(&self, data: &[u8]) -> Vec<u8>;
}