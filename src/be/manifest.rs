use time::Tm;
use toml::{from_str, to_string};

use std::fs::File;
use std::io::Read;
use std::path::Path;

use be::cas::Digest;
use common::errors::DenebResult;
use common::atomic_write;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub root_hash: Digest,
    // Note: may need to be changed to previous_manifest (store old manifests as CA chunks)
    pub previous_root_hash: Option<Digest>,
    #[serde(with = "serde_tm")]
    pub timestamp: Tm,
}

impl Manifest {
    pub fn new(hash: Digest, previous_hash: Option<Digest>, timestamp: Tm) -> Manifest {
        Manifest {
            root_hash: hash,
            previous_root_hash: previous_hash,
            timestamp: timestamp,
        }
    }

    pub fn save(&self, manifest_file: &Path) -> DenebResult<()> {
        let m = to_string(self)?;
        atomic_write(manifest_file, m.as_bytes())?;
        Ok(())
    }

    pub fn load(manifest_file: &Path) -> DenebResult<Manifest> {
        let mut f = File::open(manifest_file)?;
        let mut m = String::new();
        let _ = f.read_to_string(&mut m)?;
        from_str(m.as_str()).map_err(|e| e.into())
    }
}

mod serde_tm {
    use std::fmt;
    use serde::{Deserializer, Serializer};
    use serde::de::{Error, Visitor};
    use time::{Tm, strptime};

    pub fn serialize<S>(tm: &Tm, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let s = format!("{}", tm.rfc822());
        serializer.serialize_str(s.as_str())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Tm, D::Error>
        where D: Deserializer<'de>
    {
        struct TmVisitor;

        impl<'de> Visitor<'de> for TmVisitor {
            type Value = Tm;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("A string representing a date according to RFC 822")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where E: Error
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

    use be::cas::hash;

    use super::*;

    #[test]
    fn manifest_serde() {
        let fake_stuff = vec![0 as u8; 100];
        let digest = hash(fake_stuff.as_slice());
        let mut manifest = Manifest::new(digest, None, now_utc());
        // Set to zero the fields which are not serialized
        {
            let ts = &mut manifest.timestamp;
            ts.tm_yday = 0;
            ts.tm_isdst = 0;
            ts.tm_utcoff = 0;
            ts.tm_nsec = 0;
        }
        println!("Manifest  (original): {:?}", manifest);
        let manifest_text = toml::to_string(&manifest);
        assert!(manifest_text.is_ok());
        if let Ok(manifest_text) = manifest_text {
            println!("Manifest (serialized): {:?}", manifest_text);
            let manifest2 = toml::from_str(manifest_text.as_str());
            assert!(manifest2.is_ok());
            if let Ok(manifest2) = manifest2 {
                println!("Manifest (recovered): {:?}", manifest2);
                assert_eq!(manifest, manifest2);
            }
        }
    }

    #[test]
    fn manifest_save_load() {
        let tmp = TempDir::new("/tmp/deneb_manifest_test");
        assert!(tmp.is_ok());
        if let Ok(prefix) = tmp {
            let fake_stuff = vec![0 as u8; 100];
            let digest = hash(fake_stuff.as_slice());
            let mut manifest1 = Manifest::new(digest, None, now_utc());
            {
                let ts = &mut manifest1.timestamp;
                ts.tm_yday = 0;
                ts.tm_isdst = 0;
                ts.tm_utcoff = 0;
                ts.tm_nsec = 0;
            }
            let manifest_file = prefix.path().to_owned().join("manifest");
            let ret = manifest1.save(manifest_file.as_path());
            assert!(ret.is_ok());
            if ret.is_ok() {
                let manifest2 = Manifest::load(manifest_file.as_path());
                assert!(manifest2.is_ok());
                if let Ok(manifest2) = manifest2 {
                    assert_eq!(manifest1, manifest2);
                }
            }
        }
    }
}
