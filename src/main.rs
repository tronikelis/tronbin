use std::{
    env,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use smol::{
    Timer,
    fs::{File, create_dir, remove_file},
    io,
    lock::Mutex,
    net::{TcpListener, TcpStream},
    prelude::*,
};

macro_rules! clone_expr {
    ($($var:ident),+ => $expr:expr) => {{
        $(
            let $var = $var.clone();
        )+
        $expr
    }};
}

macro_rules! println_err {
    ($expr: expr) => {{
        let res = $expr;
        if let Err(e) = &res {
            println!("{}:{} {}", file!(), line!(), e)
        }
        res
    }};
}

type DataId = [u8; 4];

struct BitIterator {
    expanded_bits: Vec<u8>,
    index: usize,
    bits: usize,
}

impl BitIterator {
    fn new(buf: &[u8], bits: usize) -> Self {
        Self {
            expanded_bits: Self::expand_bits(buf),
            index: 0,
            bits,
        }
    }

    fn expand_bits(buf: &[u8]) -> Vec<u8> {
        let mut expanded = Vec::with_capacity(buf.len() * 8);
        for v in buf {
            for i in (0..=7).rev() {
                expanded.push((v & (1 << i)) >> i);
            }
        }
        expanded
    }
}

impl Iterator for BitIterator {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.expanded_bits.len() {
            return None;
        }

        let mut acc = 0u64;
        for i in 0..self.bits {
            let Some(bit) = self.expanded_bits.get(self.index + i) else {
                break;
            };
            acc |= (*bit as u64) << self.bits - 1 - i;
        }
        self.index += self.bits;
        Some(acc)
    }
}

const BIN_TO_BASE64_CHAR: [char; 64] = [
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
    'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
    'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4',
    '5', '6', '7', '8', '9', '-', '_',
];

fn base64_string(buf: &[u8]) -> String {
    let mut string = String::new();
    for six_bits in BitIterator::new(buf, 6) {
        string.push(BIN_TO_BASE64_CHAR[six_bits as usize]);
    }
    string
}

#[derive(Debug, Clone)]
struct CreatedFile {
    created_at: Instant,
    path: PathBuf,
}

impl CreatedFile {
    fn new(path: PathBuf) -> Self {
        Self {
            created_at: Instant::now(),
            path,
        }
    }

    fn expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }
}

#[derive(Debug, Clone)]
struct DataDb {
    dir: String,
    files: Arc<Mutex<Vec<CreatedFile>>>,
}

impl DataDb {
    fn new(dir: String) -> Self {
        Self {
            dir,
            files: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_filename(&self, id: &str) -> PathBuf {
        Path::new(&self.dir).join(id)
    }

    async fn insert(&self, id: String, reader: impl AsyncRead) -> anyhow::Result<()> {
        let reader = reader.take(2u64.pow(20) * 10); // 10mib

        let path = self.get_filename(&id);

        println!("creating file: {}", path.to_str().unwrap());
        let file = File::create(path.to_str().unwrap()).await?;
        self.files.lock().await.push(CreatedFile::new(path.clone()));

        io::copy(reader, file).await?;
        println!("[{}] copied into {}", &id, path.to_str().unwrap());

        Ok(())
    }

    async fn reader_for(&self, id: &str) -> anyhow::Result<Option<impl AsyncRead>> {
        let path = self.get_filename(id);
        if !path.try_exists()? {
            return Ok(None);
        }

        let file = File::open(path).await?;
        Ok(Some(file))
    }

    async fn gc(&self) -> anyhow::Result<()> {
        let mut timer = Timer::interval(Duration::from_secs(1));
        while let Some(_) = timer.next().await {
            let mut files = self.files.lock().await;
            let mut futures = Vec::new();
            let mut i = 0;
            while i < files.len() {
                let file = files[i].clone();

                if !file.expired(Duration::from_hours(1)) {
                    i += 1;
                    continue;
                }

                futures.push(async move {
                    println!("removing: {}", file.path.to_str().unwrap());
                    let _ = println_err!(remove_file(&file.path).await);
                });
                files.swap_remove(i);
            }
            futures::future::join_all(futures).await;
        }
        unreachable!();
    }
}

async fn fill_rand_bytes(buf: &mut [u8]) -> anyhow::Result<()> {
    let mut file = File::open("/dev/random").await?;
    file.read_exact(buf).await?;
    Ok(())
}

async fn handle_upload(data_db: DataDb, mut stream: TcpStream) -> anyhow::Result<()> {
    let mut data_id: DataId = [0; 4];
    fill_rand_bytes(&mut data_id).await?;

    let id = base64_string(&data_id);
    data_db.insert(id.clone(), &mut stream).await?;

    stream.write_all(id.as_bytes()).await?;
    stream.write(b"\n").await?;

    Ok(())
}

async fn handle_download(data_db: DataDb, mut stream: TcpStream) -> anyhow::Result<()> {
    let mut id = String::new();
    (&mut stream).take(512).read_to_string(&mut id).await?;
    let id = id.trim_end_matches('\n');

    let Some(file_reader) = data_db.reader_for(id).await? else {
        stream
            .write_all(format!("{id} does not exist").as_bytes())
            .await?;
        anyhow::bail!("{id} does not exist");
    };

    println!("reading {id}");
    io::copy(file_reader, stream).await?;

    Ok(())
}

async fn async_main() -> anyhow::Result<()> {
    let address_host = env::var("LISTEN_HOST")
        .map(|v| String::from(v))
        .unwrap_or("0.0.0.0".to_string());
    let address_port = env::var("LISTEN_PORT")
        .map(|v| v.parse::<u16>())
        .unwrap_or(Ok(3000))?;

    let upload_listener = TcpListener::bind(format!("{}:{}", address_host, address_port)).await?;
    let download_listener =
        TcpListener::bind(format!("{}:{}", address_host, address_port + 1)).await?;
    println!("bound: {}, {}", address_port, address_port + 1);

    if let Err(e) = create_dir("/tmp/tronbin").await {
        if e.kind() != io::ErrorKind::AlreadyExists {
            anyhow::bail!(e);
        }
    }
    let data_db = DataDb::new("/tmp/tronbin".to_string());

    let gc_future = data_db.gc();

    let upload_future = async {
        loop {
            let (stream, addr) = match upload_listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    println!("error accepting: {}", e);
                    continue;
                }
            };
            println!("accepted: {}", addr);
            smol::spawn(clone_expr!(data_db => async move {
                let _ = println_err!(handle_upload(data_db, stream).await);
            }))
            .detach();
        }
    };

    let download_future = async {
        loop {
            let (stream, addr) = match download_listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    println!("error accepting: {}", e);
                    continue;
                }
            };
            println!("accepted: {}", addr);
            smol::spawn(clone_expr!(data_db => async move {
                let _ = println_err!(handle_download(data_db, stream).await);
            }))
            .detach();
        }
    };

    futures::join!(upload_future, gc_future, download_future);
    Ok(())
}

fn main() {
    smol::block_on(async_main()).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base64_string() {
        assert_eq!(
            base64_string(b"Many hands make light work."),
            "TWFueSBoYW5kcyBtYWtlIGxpZ2h0IHdvcmsu".to_string()
        );
    }
}
