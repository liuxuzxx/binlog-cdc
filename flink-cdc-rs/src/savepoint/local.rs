use std::{fs, path::PathBuf};

use tracing::{info, warn};

use crate::savepoint::SavePoints;

pub struct LocalFileSystem {}

impl LocalFileSystem {
    const SAVEPOINT_PATH: &'static str = "./savepoints/";
    const SAVEPOINT_FILE: &'static str = "savepoint.data";

    pub fn default() -> Self {
        LocalFileSystem {}
    }
}

impl SavePoints for LocalFileSystem {
    fn save(&self, savepoint: &str) {
        let mut path = PathBuf::from(Self::SAVEPOINT_PATH);
        if path.exists() {
            info!("Savepoint already exists:{}", path.to_str().unwrap());
        } else {
            fs::create_dir_all(&path).unwrap();
        }
        path.push(Self::SAVEPOINT_FILE);
        match fs::write(path, savepoint) {
            Ok(_) => info!("Savepoint saved:{} success", savepoint),
            Err(e) => warn!("Error saving savepoint:{}", e),
        }
    }

    fn load(&self) -> Option<String> {
        let mut path = PathBuf::from(Self::SAVEPOINT_PATH);
        path.push(Self::SAVEPOINT_FILE);
        if path.exists() {
            match fs::read_to_string(path) {
                Ok(s) => Some(s),
                Err(e) => {
                    warn!("Error loading savepoint:{}", e);
                    None
                }
            }
        } else {
            None
        }
    }
}
