use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use subspace_farmer_components::file_ext::{FileExt, OpenOptionsExt};
use subspace_farmer_components::ReadAtSync;

use iouring::Manager;
use std::hint::spin_loop;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicU8, Ordering};

pub struct IoUringFile {
    _file: File,
    io_uring: Manager,
}

impl Drop for IoUringFile {
    #[inline]
    fn drop(&mut self) {
        self.io_uring
            .terminate_and_wait()
            .expect("fail to terminate_and_wait");
    }
}

impl ReadAtSync for IoUringFile {
    fn read_at(&self, buf: &mut [u8], offset: usize) -> io::Result<()> {
        let result = AtomicU8::from(iouring::OP_RESULT_PENDING);
        let io = self.io_uring.read(buf, 0, &result);

        unsafe {
            (*io.get()).offset = offset;
        }

        self.io_uring.push(io);

        let mut i = 0;
        while result.load(Ordering::Acquire) == iouring::OP_RESULT_PENDING {
            i += 1;
            if i > 1000000 {
                panic!("hangup")
            }
            spin_loop();
        }

        if result.load(Ordering::Acquire) == iouring::OP_RESULT_NG {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }

        Ok(())
    }
}

impl ReadAtSync for &IoUringFile {
    fn read_at(&self, buf: &mut [u8], offset: usize) -> io::Result<()> {
        (*self).read_at(buf, offset)
    }
}

impl IoUringFile {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .advise_random_access()
            .open(path)?;
        file.advise_random_access()?;

        let io_uring = Manager::new(&[file.as_raw_fd()], 3000).unwrap();
        Ok(IoUringFile {
            _file: file,
            io_uring,
        })
    }
}
