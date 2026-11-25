use rex_wave_api::room::{Body, RoomEvent, RoomId, RoomScope};
use rex_wave_api::{ChannelName, Event, IdemKey, Principal, SeatName, Seq, StreamId, UnixMs};
use rex_wave_store::{Appended, Journal, StoreError};

fn room(name: &str) -> StreamId {
    StreamId::Room {
        room: RoomId::Channel {
            scope: RoomScope::Global,
            name: ChannelName::parse(name).unwrap(),
        },
    }
}

fn sam() -> Principal {
    Principal::Operator {
        seat: SeatName::parse("sam").unwrap(),
    }
}

fn post(text: &str) -> Event {
    Event::Room(RoomEvent::Posted {
        body: Body::parse(text).unwrap(),
        mentions: Default::default(),
        re: None,
        after: None,
        labels: Vec::new(),
        task: None,
        idem: None,
        origin: Default::default(),
    })
}

fn texts(entries: &[rex_wave_api::Envelope]) -> Vec<String> {
    entries
        .iter()
        .map(|e| match &e.event {
            Event::Room(RoomEvent::Posted { body, .. }) => body.as_str().to_owned(),
            other => format!("{other:?}"),
        })
        .collect()
}

#[test]
fn appends_are_gapless_and_read_back_in_order() {
    let j = Journal::in_memory().unwrap();
    let s = room("general");
    let mut w = j.claim(s.clone()).unwrap();
    w.append(&sam(), UnixMs(1), None, vec![post("a"), post("b")])
        .unwrap();
    w.append(&sam(), UnixMs(2), None, vec![post("c")]).unwrap();
    let all = j.read(&s, Seq::ORIGIN, 100).unwrap();
    assert_eq!(
        all.iter().map(|e| e.seq.0).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(texts(&all), ["a", "b", "c"]);
    assert_eq!(
        j.read(&s, Seq(1), 1).unwrap()[0].seq,
        Seq(2),
        "after + limit page correctly"
    );
    assert_eq!(j.head(&s).unwrap(), Seq(3));
    assert_eq!(all[0].by, sam(), "principal round-trips");
}

#[test]
fn same_idem_returns_the_original_batch_without_appending() {
    let j = Journal::in_memory().unwrap();
    let s = room("general");
    let key = IdemKey::parse("retry-1").unwrap();
    let mut w = j.claim(s.clone()).unwrap();
    let first = w
        .append(&sam(), UnixMs(1), Some(&key), vec![post("x"), post("y")])
        .unwrap();
    assert!(matches!(first, Appended::Fresh(_)));
    let again = w
        .append(&sam(), UnixMs(9), Some(&key), vec![post("different")])
        .unwrap();
    let Appended::Replayed(prior) = again else {
        panic!("expected replay")
    };
    assert_eq!(texts(&prior), ["x", "y"]);
    assert_eq!(prior[0].at, UnixMs(1));
    assert_eq!(j.head(&s).unwrap(), Seq(2), "nothing appended on replay");
    assert_eq!(texts(&w.lookup(&key).unwrap().unwrap()), ["x", "y"]);
    let other = room("random");
    let mut w2 = j.claim(other.clone()).unwrap();
    assert!(
        matches!(
            w2.append(&sam(), UnixMs(1), Some(&key), vec![post("z")])
                .unwrap(),
            Appended::Fresh(_)
        ),
        "idem is scoped to one stream"
    );
}

#[test]
fn one_writer_per_stream_until_dropped() {
    let j = Journal::in_memory().unwrap();
    let s = room("general");
    let w = j.claim(s.clone()).unwrap();
    assert!(matches!(
        j.clone().claim(s.clone()),
        Err(StoreError::AlreadyClaimed(_))
    ));
    drop(w);
    let mut again = j.claim(s.clone()).unwrap();
    again
        .append(&sam(), UnixMs(1), None, vec![post("ok")])
        .unwrap();
}

#[test]
fn rejects_events_from_another_family() {
    let j = Journal::in_memory().unwrap();
    let mut w = j.claim(StreamId::Mail).unwrap();
    assert!(matches!(
        w.append(&sam(), UnixMs(1), None, vec![post("nope")]),
        Err(StoreError::WrongStream(_))
    ));
    assert_eq!(j.head(&StreamId::Mail).unwrap(), Seq::ORIGIN);
}

#[test]
fn file_journal_is_wal_persists_and_detects_a_foreign_writer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal.sqlite3");
    let s = room("general");
    {
        let j = Journal::open(&path).unwrap();
        j.claim(s.clone())
            .unwrap()
            .append(&sam(), UnixMs(1), None, vec![post("kept")])
            .unwrap();
    }
    let a = Journal::open(&path).unwrap();
    assert_eq!(
        texts(&a.read(&s, Seq::ORIGIN, 10).unwrap()),
        ["kept"],
        "survives reopen"
    );
    assert_eq!(a.streams().unwrap(), vec![(s.clone(), Seq(1))]);

    let b = Journal::open(&path).unwrap();
    let mut wa = a.claim(s.clone()).unwrap();
    let mut wb = b.claim(s.clone()).unwrap();
    wa.append(&sam(), UnixMs(2), None, vec![post("a")]).unwrap();
    let err = wb
        .append(&sam(), UnixMs(3), None, vec![post("b")])
        .unwrap_err();
    assert!(
        matches!(
            err,
            StoreError::HeadMoved {
                expected: Seq(1),
                found: Seq(2)
            }
        ),
        "{err:?}"
    );
}
