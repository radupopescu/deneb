use {
    crate::{cas::Digest, errors::DenebResult, util::atomic_write},
    serde::{Deserialize, Serialize},
    std::{fs::File, io::Read, path::Path},
    time::Tm,
};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub root_hash: Digest,
    #[serde(with = "serde_tm")]
    pub timestamp: Tm,
}

impl Manifest {
    pub fn new(hash: Digest, timestamp: Tm) -> Manifest {
        Manifest {
            root_hash: hash,
            timestamp,
        }
    }

    pub fn save(&self, manifest_file: &Path) -> DenebResult<()> {
        let m = toml::to_string(self)?;
        atomic_write(manifest_file, m.as_bytes())?;
        Ok(())
    }

    pub fn load(manifest_file: &Path) -> DenebResult<Manifest> {
        let mut f = File::open(manifest_file)?;
        let mut m = String::new();
        let _ = f.read_to_string(&mut m)?;
        toml::from_str(m.as_str()).map_err(std::convert::Into::into)
    }

    pub fn serialize(&self) -> DenebResult<Vec<u8>> {
        toml::to_vec(self).map_err(std::convert::Into::into)
    }

    pub fn deserialize(s: &[u8]) -> DenebResult<Manifest> {
        toml::from_slice(s).map_err(std::convert::Into::into)
    }
}

mod serde_tm {
    use serde::de::{Error, Visitor};
    use serde::{Deserializer, Serializer};
    use std::fmt;
    use time::{strptime, Tm};

    pub fn serialize<S>(tm: &Tm, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", tm.rfc822());
        serializer.serialize_str(s.as_str())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Tm, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TmVisitor;

        impl<'de> Visitor<'de> for TmVisitor {
            type Value = Tm;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("A string representing a date according to RFC 822")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                strptime(v, "%a, %d %b %Y %H:%M:%S GMT").map_err(Error::custom)
            }
        }

        deserializer.deserialize_str(TmVisitor)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use time::now_utc;
    use toml;

    use crate::cas::hash;

    use super::*;

    #[test]
    fn manifest_serde() -> DenebResult<()> {
        let fake_stuff = vec![0 as u8; 100];
        let digest = hash(fake_stuff.as_slice());
        let mut manifest = Manifest::new(digest, now_utc());
        // Set to zero the fields which are not serialized
        {
            let ts = &mut manifest.timestamp;
            ts.tm_yday = 0;
            ts.tm_isdst = 0;
            ts.tm_utcoff = 0;
            ts.tm_nsec = 0;
        }
        println!("Manifest  (original): {:?}", manifest);
        let manifest_text = toml::to_string(&manifest)?;
        println!("Manifest (serialized): {:?}", manifest_text);
        let manifest2 = toml::from_str(manifest_text.as_str())?;
        println!("Manifest (recovered): {:?}", manifest2);
        assert_eq!(manifest, manifest2);

        Ok(())
    }

    #[test]
    fn manifest_save_load() -> DenebResult<()> {
        let tmp = TempDir::new("/tmp/deneb_manifest_test")?;
        let fake_stuff = vec![0 as u8; 100];
        let digest = hash(fake_stuff.as_slice());
        let mut manifest1 = Manifest::new(digest, now_utc());
        {
            let ts = &mut manifest1.timestamp;
            ts.tm_yday = 0;
            ts.tm_isdst = 0;
            ts.tm_utcoff = 0;
            ts.tm_nsec = 0;
        }
        let manifest_file = tmp.path().to_owned().join("manifest");
        manifest1.save(manifest_file.as_path())?;
        let manifest2 = Manifest::load(manifest_file.as_path())?;
        assert_eq!(manifest1, manifest2);

        Ok(())
    }
}
