use {
    failure::Fail,
    rust_sodium::crypto::secretbox::{gen_key, Key},
};

#[derive(Debug, Fail)]
#[fail(display = "Key read error")]
pub struct ReadError;

#[derive(Clone, Debug)]
pub struct EncryptionKey(Key);

impl EncryptionKey {
    pub fn new() -> EncryptionKey {
        EncryptionKey(gen_key())
    }

    pub fn from_slice(s: &[u8]) -> Result<EncryptionKey, ReadError> {
        Key::from_slice(s).map(EncryptionKey).ok_or(ReadError)
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0[..]
    }
}

impl Default for EncryptionKey {
    fn default() -> Self {
        Self::new()
    }
}
