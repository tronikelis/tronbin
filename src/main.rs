use std::{env, net::Shutdown, path::Path, sync::Arc};

use smol::{
    fs::{File, create_dir, create_dir_all},
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

const BIN_TO_HEX_CHAR: [char; 16] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
];

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

fn hex_string(buf: &[u8]) -> String {
    let mut string = String::new();
    for v in buf {
        let first_half = (v & 0xF0) >> 4;
        let second_half = v & 0x0F;
        string.push(BIN_TO_HEX_CHAR[first_half as usize]);
        string.push(BIN_TO_HEX_CHAR[second_half as usize]);
    }
    string
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
struct DataDb {
    dir: String,
    files: Arc<Mutex<Vec<String>>>,
}

impl DataDb {
    fn new(dir: String) -> Self {
        Self {
            dir,
            files: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn insert(&self, id: String, reader: impl AsyncRead) -> anyhow::Result<()> {
        self.files.lock().await.push(id.clone());

        let path = Path::new(&self.dir).join(&id);

        println!("creating file: {}", path.to_str().unwrap());
        let file = File::create(path.to_str().unwrap()).await?;

        io::copy(reader, file).await?;
        println!("copied {} into {}", &id, path.to_str().unwrap());

        Ok(())
    }
}

async fn fill_rand_bytes(buf: &mut [u8]) -> anyhow::Result<()> {
    let mut file = File::open("/dev/random").await?;
    file.read_exact(buf).await?;
    Ok(())
}

async fn handle_stream(data_db: DataDb, mut stream: TcpStream) -> anyhow::Result<()> {
    let mut data_id: [u8; 4] = [0; 4];
    fill_rand_bytes(&mut data_id).await?;

    let id = base64_string(&data_id);
    data_db.insert(id.clone(), &mut stream).await?;

    stream.write_all(id.as_bytes()).await?;
    stream.write(b"\n").await?;

    Ok(())
}

async fn async_main() -> anyhow::Result<()> {
    let address_host = env::var("LISTEN_HOST")
        .map(|v| String::from(v))
        .unwrap_or("127.0.0.1".to_string());
    let address_port = env::var("LISTEN_PORT")
        .map(|v| v.parse::<u16>())
        .unwrap_or(Ok(3000))?;

    let listener = TcpListener::bind(format!("{}:{}", address_host, address_port)).await?;

    if let Err(e) = create_dir("/tmp/tronbin").await {
        if e.kind() != io::ErrorKind::AlreadyExists {
            anyhow::bail!(e);
        }
    }
    let data_db = DataDb::new("/tmp/tronbin".to_string());

    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                println!("error accepting: {}", e);
                continue;
            }
        };
        println!("accepted: {}", addr);
        smol::spawn(clone_expr!(data_db => async move {
            match handle_stream(data_db, stream).await {
                Ok(_) => {},
                Err(e) => println!("handle_stream err: {}", e),
            };
        }))
        .detach();
    }
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
