use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::os::unix::io::AsRawFd;
use libc::{lseek, SEEK_SET, SEEK_END};

fn main() -> io::Result<()> {
    // 打开文件进行读写
    let file_path = "/mnt/jfs/1.txt";
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(file_path)?;

    // 获取文件描述符
    let fd = file.as_raw_fd();

    // 使用 lseek 移动文件指针到文件开头
    unsafe {
        if lseek(fd, 2, SEEK_SET) == -1 {
            eprintln!("Failed to seek to the beginning");
            return Err(io::Error::last_os_error());
        }
    }

    // 写入数据
    let data = b"Hello, World!";
    file.write_all(data)?;

    // 使用 lseek 移动文件指针到文件末尾
    unsafe {
        if lseek(fd, 0, SEEK_END) == -1 {
            eprintln!("Failed to seek to the end");
            return Err(io::Error::last_os_error());
        }
    }

    // 追加数据
    let more_data = b" Goodbye!";
    file.write_all(more_data)?;

    // 使用 lseek 移动文件指针到文件开头
    unsafe {
        if lseek(fd, 0, SEEK_SET) == -1 {
            eprintln!("Failed to seek to the beginning");
            return Err(io::Error::last_os_error());
        }
    }

    // 读取文件内容
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    println!("File content: {}", String::from_utf8_lossy(&buffer));

    Ok(())
}