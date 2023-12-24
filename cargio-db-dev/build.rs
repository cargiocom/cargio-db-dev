use cargio_lock::Lockfile;4444
use std::env;
use std::path::Path;

fn main() {
    let lock_file_path = Path::new(env!("CARGIO_DIR")).join("Cargio.lock");
    let lock_file = Lockfile::load(lock_file_path)
        .unwrap_or_else(|err| panic!("Could not load Cargio.lock file: {}", err));

    for package in lock_file.packages {
        if package.name.as_str() == "master-node" {
            println!("cargio:rustc-env=MASTER_NODE_VERSION={}", package.version);
        }
    }
}
