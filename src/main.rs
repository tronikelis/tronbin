use std::{
    collections::VecDeque,
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

macro_rules! err_side_effect {
    ($expr:expr, $effect:expr) => {{
        match $expr {
            Ok(v) => Ok(v),
            Err(e) => {
                $effect
                Err(e)
            }
        }
    }}
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

#[derive(Debug)]
struct CreatedFile {
    created_at: Instant,
    path: PathBuf,
    size: usize,
}

impl CreatedFile {
    fn new(path: PathBuf, size: usize, file: File) -> Self {
        drop(file); // this struct has ownership of the file, but it does not need to use it
        Self {
            created_at: Instant::now(),
            path,
            size,
        }
    }

    fn expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }
}

impl Drop for CreatedFile {
    fn drop(&mut self) {
        let path = self.path.clone();
        smol::spawn(async move {
            println!("dropping: {}", path.to_str().unwrap_or("?"));
            let _ = println_err!(remove_file(path).await);
        })
        .detach();
    }
}

#[derive(Debug)]
struct Files {
    files: VecDeque<CreatedFile>,
    size: usize,
    max_size: usize,
}

impl Files {
    fn new(max_size: usize) -> Self {
        Self {
            files: VecDeque::new(),
            size: 0,
            max_size,
        }
    }

    fn push_back(&mut self, file: CreatedFile) {
        self.size += file.size;
        if self.size > self.max_size {
            self.create_space_for(self.size - self.max_size);
        }
        self.files.push_back(file);
    }

    fn pop_front(&mut self) -> Option<CreatedFile> {
        let file = self.files.pop_front()?;
        self.size -= file.size;
        Some(file)
    }

    fn try_expire_many(&mut self, ttl: Duration) {
        loop {
            let Some(file) = self.files.get(0) else {
                break;
            };
            if file.expired(ttl) {
                self.pop_front();
            } else {
                break;
            }
        }
    }

    fn create_space_for(&mut self, size: usize) {
        let mut removed_space = 0;
        while removed_space < size {
            let Some(file) = self.pop_front() else {
                break;
            };
            removed_space += file.size;
        }
    }
}

#[derive(Debug, Clone)]
struct DataDb {
    dir: String,
    files: Arc<Mutex<Files>>,
}

impl DataDb {
    fn new(dir: String, max_size: usize) -> Self {
        Self {
            files: Arc::new(Mutex::new(Files::new(max_size))),
            dir,
        }
    }

    fn get_filename(&self, id: &str) -> anyhow::Result<PathBuf> {
        if id.contains(".") || id.contains("/") {
            anyhow::bail!("path {} contains dots,slashes", id);
        }
        Ok(Path::new(&self.dir).join(id))
    }

    async fn insert(&self, id: String, reader: impl AsyncRead) -> anyhow::Result<()> {
        let reader = reader.take(2u64.pow(20) * 10); // 10mib

        let path = self.get_filename(&id)?;

        println!("creating file: {}", path.to_str().unwrap());
        let mut file = File::create(&path).await?;

        let size = err_side_effect!(io::copy(reader, &mut file).await, {
            remove_file(&path).await?;
        })?;

        println!(
            "[{}] copied {}kib into {}",
            &id,
            size / 1024,
            path.to_str().unwrap()
        );
        self.files
            .lock()
            .await
            .push_back(CreatedFile::new(path.clone(), size as usize, file));

        Ok(())
    }

    async fn reader_for(&self, id: &str) -> anyhow::Result<Option<impl AsyncRead>> {
        let path = self.get_filename(id)?;
        if !path.try_exists()? {
            return Ok(None);
        }

        let file = File::open(path).await?;
        Ok(Some(file))
    }

    async fn gc(&self) -> anyhow::Result<()> {
        let mut timer = Timer::interval(Duration::from_secs(1));
        while let Some(_) = timer.next().await {
            self.files
                .lock()
                .await
                .try_expire_many(Duration::from_hours(1));
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
    let address_host = env::var("LISTEN_HOST").unwrap_or("0.0.0.0".to_string());
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

    let max_size = env::var("MAX_SIZE")
        .map(|v| v.parse::<usize>())
        .unwrap_or(Ok(2usize.pow(20) * 100))?;
    let data_db = DataDb::new("/tmp/tronbin".to_string(), max_size);

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
