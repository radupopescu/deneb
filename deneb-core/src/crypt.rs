use {
    failure::Fail,
    serde::{Deserialize, Serialize},
    sodiumoxide::crypto::secretbox::{gen_key, gen_nonce, open, seal, Key, Nonce as SodiumNonce},
};

#[derive(Debug, Fail)]
#[fail(display = "Key read error")]
pub struct ReadError;

#[derive(Debug, Fail)]
#[fail(display = "Decryption error")]
pub struct DecryptionError;

#[derive(Clone, Debug)]
pub struct EncryptionKey(Key);

#[derive(Deserialize, Serialize)]
pub struct Nonce(SodiumNonce);

impl Nonce {
    pub fn new() -> Nonce {
        Nonce(gen_nonce())
    }
}

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

pub fn encrypt(msg: &[u8], nonce: &Nonce, key: &EncryptionKey) -> Vec<u8> {
    seal(msg, &nonce.0, &key.0)
}

pub fn decrypt(cyphertext: &[u8], nonce: &Nonce, key: &EncryptionKey) -> Result<Vec<u8>, DecryptionError> {
    open(cyphertext, &nonce.0, &key.0).map_err(|_| DecryptionError)
}

#[cfg(test)]
mod tests {
    use {
        crate::errors::*,
        rand::{thread_rng, RngCore},
        super::*
    };

    const TEST_CHUNK_SIZE: usize = 1024 * 1024; // 1 MB

    #[test]
    fn encrypt_decrypt() -> DenebResult<()> {
        let mut msg = vec![0 as u8; TEST_CHUNK_SIZE];
        thread_rng().fill_bytes(msg.as_mut());

        let key = EncryptionKey::new();
        let nonce = Nonce::new();

        let cyphertext = encrypt(msg.as_slice(), &nonce, &key);
        let recovered = decrypt(cyphertext.as_slice(), &nonce, &key)?;

        assert_eq!(msg, recovered);

        Ok(())
    }
}