mod utils;
use utils::*;

use kanal::{bounded, unbounded, ReceiveError, Receiver, SendError, Sender};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

fn new<T>(cap: Option<usize>) -> (Sender<T>, Receiver<T>) {
    match cap {
        None => unbounded(),
        Some(cap) => bounded(cap),
    }
}

fn delay() {
    std::thread::sleep(Duration::from_millis(10));
}

fn mpmc(cap: Option<usize>) {
    let (tx, rx) = new(cap);

    crossbeam::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|_| {
                for _i in 0..MESSAGES / THREADS {
                    tx.send(Box::new(1)).unwrap();
                }
            });
        }

        for _ in 0..THREADS {
            scope.spawn(|_| {
                for _ in 0..MESSAGES / THREADS {
                    assert_eq!(rx.recv().unwrap(), Box::new(1));
                }
            });
        }
    })
    .unwrap();
}

macro_rules! mpmc_dyn {
    ($new:expr,$cap:expr) => {
        let (tx, rx) = new($cap);

        crossbeam::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|_| {
                    for _i in 0..MESSAGES / THREADS {
                        tx.send($new).unwrap();
                    }
                });
            }

            for _ in 0..THREADS {
                scope.spawn(|_| {
                    for _ in 0..MESSAGES / THREADS {
                        rx.recv().unwrap();
                    }
                });
            }
        })
        .unwrap();
    };
}

macro_rules! integrity_test {
    ($zero:expr,$ones:expr) => {
        let (tx, rx) = new(Some(0));
        crossbeam::scope(|scope| {
            scope.spawn(|_| {
                for _ in 0..MESSAGES {
                    tx.send($zero).unwrap();
                    tx.send($ones).unwrap();
                }
            });
            for _ in 0..MESSAGES {
                assert_eq!(rx.recv().unwrap(), $zero);
                assert_eq!(rx.recv().unwrap(), $ones);
            }
        })
        .unwrap();
    };
}

fn mpsc(cap: Option<usize>) {
    let (tx, rx) = new(cap);

    crossbeam::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|_| {
                for _i in 0..MESSAGES / THREADS {
                    tx.send(Box::new(1)).unwrap();
                }
            });
        }

        for _ in 0..MESSAGES {
            assert_eq!(rx.recv().unwrap(), Box::new(1));
        }
    })
    .unwrap();
}

fn seq(cap: Option<usize>) {
    let (tx, rx) = new(cap);

    for _i in 0..MESSAGES {
        tx.send(Box::new(1)).unwrap();
    }

    for _ in 0..MESSAGES {
        assert_eq!(rx.recv().unwrap(), Box::new(1));
    }
}

fn spsc(cap: Option<usize>) {
    let (tx, rx) = new(cap);

    crossbeam::scope(|scope| {
        scope.spawn(|_| {
            for _i in 0..MESSAGES {
                tx.send(Box::new(1)).unwrap();
            }
        });

        for _ in 0..MESSAGES {
            assert_eq!(rx.recv().unwrap(), Box::new(1));
        }
    })
    .unwrap();
}

#[test]
fn spsc_delayed_receive() {
    let (tx, rx) = new(0.into());
    crossbeam::scope(|scope| {
        scope.spawn(|_| {
            for _i in 0..10 {
                tx.send(Box::new(1)).unwrap();
            }
        });

        for _ in 0..10 {
            delay();
            assert_eq!(rx.recv().unwrap(), Box::new(1));
        }
    })
    .unwrap();
}

#[test]
fn spsc_delayed_send() {
    let (tx, rx) = new(0.into());
    crossbeam::scope(|scope| {
        scope.spawn(|_| {
            for _i in 0..10 {
                delay();
                tx.send(Box::new(1)).unwrap();
            }
        });

        for _ in 0..10 {
            assert_eq!(rx.recv().unwrap(), Box::new(1));
        }
    })
    .unwrap();
}

#[test]
fn spsc_overaligned_zst() {
    #[repr(align(1024))]
    struct Foo;

    let (tx, rx) = new(0.into());
    crossbeam::scope(|scope| {
        scope.spawn(|_| {
            for _i in 0..10 {
                delay();
                tx.send(Foo).unwrap();
            }
        });

        for _ in 0..10 {
            rx.recv().unwrap();
        }
    })
    .unwrap();
}

#[test]
fn integrity_u8() {
    integrity_test!(0u8, !0u8);
}

#[test]
fn integrity_u16() {
    integrity_test!(0u16, !0u16);
}

#[test]
fn integrity_u32() {
    integrity_test!(0u32, !0u32);
}

#[test]
fn integrity_u64() {
    integrity_test!(0u64, !0u64);
}

#[test]
fn integrity_box_u64() {
    integrity_test!(Box::new(0u64), Box::new(!0u64));
}

#[test]
fn integrity_big() {
    integrity_test!((0u64, 0u64, 0u64, 0u64), (!0u64, !0u64, !0u64, !0u64));
}

#[test]
fn integrity_string() {
    integrity_test!("", "not empty");
}

#[test]
fn integrity_big_tail() {
    integrity_test!((0u64, 0u64, 0u64, 0u8), (!0u64, !0u64, !0u64, !0u8));
}

#[test]
fn integrity_padded_rust() {
    integrity_test!(
        Padded {
            a: false,
            b: 0x0,
            c: 0x0
        },
        Padded {
            a: true,
            b: 0xFF,
            c: 0xFFFFFFFF
        }
    );
}

#[test]
fn integrity_padded_c() {
    integrity_test!(
        PaddedReprC {
            a: false,
            b: 0x0,
            c: 0x0
        },
        PaddedReprC {
            a: true,
            b: 0xFF,
            c: 0xFFFFFFFF
        }
    );
}

#[test]
fn single_message() {
    let (tx, rx) = new(Some(1));
    tx.send(Box::new(1)).unwrap();
    assert_eq!(rx.recv().unwrap(), Box::new(1));
}

// Channel logic tests
#[test]
fn recv_from_half_closed_queue() {
    let (tx, rx) = new(Some(1));
    tx.send(Box::new(1)).unwrap();
    drop(tx);
    // it's ok to receive data from queue of half closed channel
    assert_eq!(rx.recv().unwrap(), Box::new(1));
}

#[test]
fn drain_into_test() {
    const TEST_LENGTH: usize = 1000;
    let (tx, rx) = new(Some(TEST_LENGTH));
    for i in 0..TEST_LENGTH {
        tx.send(Box::new(i)).unwrap();
    }
    let mut vec = Vec::new();
    assert_eq!(rx.drain_into(&mut vec).unwrap(), TEST_LENGTH);
    assert_eq!(vec.len(), TEST_LENGTH);
    for (i, v) in vec.iter().enumerate() {
        assert_eq!(**v, i);
    }
}

#[test]
fn drain_into_test_zero_sized() {
    const TEST_LENGTH: usize = 100;
    let (tx, rx) = new(None);
    for _ in 0..TEST_LENGTH {
        let tx = tx.clone();
        thread::spawn(move || {
            tx.send(0xff).unwrap();
        });
    }
    std::thread::sleep(Duration::from_millis(1000));
    let mut vec = Vec::new();
    assert_eq!(rx.drain_into(&mut vec).unwrap(), TEST_LENGTH);
    assert_eq!(vec.len(), TEST_LENGTH);
    for v in vec.iter() {
        assert_eq!(*v, 0xff);
    }
}

#[test]
fn recv_from_half_closed_channel() {
    let (tx, rx) = new::<u64>(Some(1));
    drop(tx);
    assert_eq!(rx.recv().err().unwrap(), ReceiveError::SendClosed);
}

#[test]
fn recv_from_closed_channel() {
    let (tx, rx) = new::<u64>(Some(1));
    tx.close().unwrap();
    assert_eq!(rx.recv().err().unwrap(), ReceiveError::Closed);
}

#[test]
fn recv_from_closed_channel_queue() {
    let (tx, rx) = new(Some(1));
    tx.send(Box::new(1)).unwrap();
    tx.close().unwrap();
    // it's not possible to read data from queue of fully closed channel
    assert_eq!(rx.recv().err().unwrap(), ReceiveError::Closed);
}

#[test]
fn send_to_half_closed_channel() {
    let (tx, rx) = new(Some(1));
    drop(rx);
    assert_eq!(
        tx.send(Box::new(1)).err().unwrap(),
        SendError::ReceiveClosed
    );
}

#[test]
fn send_to_closed_channel() {
    let (tx, rx) = new(Some(1));
    rx.close().unwrap();
    assert_eq!(tx.send(Box::new(1)).err().unwrap(), SendError::Closed);
}

// Channel drop tests
#[test]
fn drop_test() {
    let counter = Arc::new(AtomicUsize::new(0));
    mpmc_dyn!(DropTester::new(counter.clone(), 10), Some(1));
    assert_eq!(counter.load(Ordering::SeqCst), MESSAGES);
}

#[test]
fn drop_test_in_queue() {
    let counter = Arc::new(AtomicUsize::new(0));
    let (s, r) = new(Some(10));
    for _ in 0..10 {
        s.send(DropTester::new(counter.clone(), 1234)).unwrap();
    }
    r.close().unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 10_usize);
}

#[test]
fn drop_test_send_to_closed() {
    let counter = Arc::new(AtomicUsize::new(0));
    let (s, r) = new(Some(10));
    r.close().unwrap();
    for _ in 0..10 {
        // will fail
        let _ = s.send(DropTester::new(counter.clone(), 1234));
    }
    assert_eq!(counter.load(Ordering::SeqCst), 10_usize);
}

#[test]
fn drop_test_send_to_half_closed() {
    let counter = Arc::new(AtomicUsize::new(0));
    let (s, r) = new(Some(10));
    drop(r);
    for _ in 0..10 {
        // will fail
        let _ = s.send(DropTester::new(counter.clone(), 1234));
    }
    assert_eq!(counter.load(Ordering::SeqCst), 10_usize);
}

#[test]
fn drop_test_in_signal() {
    let (s, r) = new(Some(0));

    crossbeam::scope(|scope| {
        let counter = Arc::new(AtomicUsize::new(0));
        let mut list = Vec::new();
        for _ in 0..10 {
            let counter = counter.clone();
            let s = s.clone();
            let t = scope.spawn(move |_| {
                let _ = s.send(DropTester::new(counter.clone(), 1234));
            });
            list.push(t);
        }
        std::thread::sleep(Duration::from_millis(1000));
        r.close().unwrap();
        for t in list {
            t.join().unwrap();
        }
        assert_eq!(counter.load(Ordering::SeqCst), 10_usize);
    })
    .unwrap();
}

#[test]
fn vec_test() {
    mpmc_dyn!(vec![1, 2, 3], Some(1));
}

#[test]
fn seq_n() {
    seq(Some(MESSAGES));
}

#[test]
fn seq_u() {
    seq(None);
}

#[test]
fn spsc_1() {
    spsc(Some(1));
}
#[test]
fn spsc_0() {
    spsc(Some(0));
}
#[test]
fn spsc_n() {
    spsc(Some(MESSAGES));
}
#[test]
fn spsc_u() {
    spsc(None);
}

#[test]
fn mpsc_1() {
    mpsc(Some(1));
}
#[test]
fn mpsc_0() {
    mpsc(Some(0));
}
#[test]
fn mpsc_n() {
    mpsc(Some(MESSAGES));
}
#[test]
fn mpsc_u() {
    mpsc(None);
}

#[test]
fn mpmc_1() {
    mpmc(Some(1));
}
#[test]
fn mpmc_0() {
    mpmc(Some(0));
}
#[test]
fn mpmc_n() {
    mpmc(Some(MESSAGES));
}
#[test]
fn mpmc_u() {
    mpmc(None);
}
