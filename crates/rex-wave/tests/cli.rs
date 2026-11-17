use rex_wave_api::{Client, SeatName, WireCommand, WireReply};
use rex_wave_runtime::{
    BoxFut, DecideCtx, Hub, NoAgentFeed, ReadSide, Router, Service, SidebarProjection,
};
use rex_wave_store::Journal;
use rex_wave_transport::{Authenticator, TableAuthenticator, bind_unix, serve_listener};
use rex_wave_tui::demo::DemoClient;
use std::collections::BTreeMap;
use std::process::Command;
use std::sync::Arc;

struct DemoForwardService<C>(DemoClient, std::marker::PhantomData<C>);

impl<C> Service<C> for DemoForwardService<C>
where
    C: Into<WireCommand> + Send + Sync + 'static,
{
    fn dispatch(
        &self,
        cmd: C,
        _ctx: DecideCtx,
    ) -> BoxFut<'_, Result<WireReply, rex_wave_api::ApiError>> {
        let wire: WireCommand = cmd.into();
        let demo = self.0.clone();
        Box::pin(async move { demo.call_wire(wire).await })
    }
}

fn bin_path() -> &'static str {
    env!("CARGO_BIN_EXE_rex-wave")
}

#[test]
fn tui_self_test_and_schema_flags_run_standalone_zero_daemon() {
    let self_test = Command::new(bin_path())
        .args(["tui", "--self-test"])
        .output()
        .expect("spawn rex-wave tui --self-test");
    assert!(
        self_test.status.success(),
        "tui --self-test stderr: {}",
        String::from_utf8_lossy(&self_test.stderr)
    );
    let stdout = String::from_utf8_lossy(&self_test.stdout);
    assert!(stdout.contains("SELF-TEST PASSED"));
    assert!(stdout.contains("Org Tree"));

    let schema = Command::new(bin_path())
        .args(["schema", "--bb"])
        .output()
        .expect("spawn rex-wave schema --bb");
    assert!(schema.status.success());
    let tools: serde_json::Value = serde_json::from_slice(&schema.stdout).expect("valid json");
    let arr = tools.as_array().expect("tool array");
    assert!(
        arr.iter()
            .any(|t| t.get("name").and_then(|n| n.as_str()) == Some("room_post"))
    );
    assert!(
        arr.iter()
            .any(|t| t.get("name").and_then(|n| n.as_str()) == Some("run_code"))
    );
}

#[tokio::test]
async fn all_cli_verbs_round_trip_over_real_unix_socket() {
    let dir = std::env::temp_dir().join(format!("rex-wave-cli-uds-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let sock_path = dir.join("daemon.sock");
    let journal_path = dir.join("journal.sqlite3");
    let goal_path = dir.join("goal.md");
    std::fs::write(&goal_path, "Ship two-pane TUI and CLI").unwrap();

    let journal = Journal::open(&journal_path).unwrap();
    let hub = Hub::new(128);
    let sidebar = SidebarProjection::spawn(journal.clone(), hub.clone());
    let demo = DemoClient::new();

    let router = Arc::new(Router {
        room: Arc::new(DemoForwardService(demo.clone(), std::marker::PhantomData)),
        wave: Arc::new(DemoForwardService(demo.clone(), std::marker::PhantomData)),
        board: Arc::new(DemoForwardService(demo.clone(), std::marker::PhantomData)),
        inbox: Arc::new(DemoForwardService(demo.clone(), std::marker::PhantomData)),
        mail: Arc::new(DemoForwardService(demo, std::marker::PhantomData)),
        read: Arc::new(ReadSide {
            journal,
            hub,
            sidebar,
            agents: Arc::new(NoAgentFeed),
        }),
    });

    let (listener, daemon_uid) = bind_unix(&sock_path).unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(TableAuthenticator {
        operator: SeatName::parse("owner").unwrap(),
        daemon_uid: Some(daemon_uid),
        tokens: BTreeMap::new(),
    });
    let server = tokio::spawn(serve_listener(listener, router, auth));

    let sock_str = sock_path.to_str().unwrap().to_owned();
    let goal_str = goal_path.to_str().unwrap().to_owned();

    let run_verb = move |args: Vec<String>| -> serde_json::Value {
        let mut full = vec!["--socket".to_owned(), sock_str.clone(), "--json".to_owned()];
        full.extend(args);
        let out = Command::new(bin_path())
            .args(&full)
            .output()
            .expect("run rex-wave");
        assert!(
            out.status.success(),
            "cmd {full:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout).expect("json reply")
    };

    let sock_for_tok = sock_path.to_str().unwrap().to_owned();
    let tok_val = tokio::task::spawn_blocking(move || {
        let out = Command::new(bin_path())
            .args(["--socket", &sock_for_tok, "token", "--seat", "sol"])
            .output()
            .unwrap();
        assert!(out.status.success());
        String::from_utf8(out.stdout).unwrap().trim().to_owned()
    })
    .await
    .unwrap();
    assert!(tok_val.starts_with("rw_"));

    let r_wave_start = run_verb.clone();
    let ws_json = tokio::task::spawn_blocking(move || {
        r_wave_start(vec![
            "wave".into(),
            "start".into(),
            "auth-v2".into(),
            "--project".into(),
            "p_123".into(),
            "--goal-file".into(),
            goal_str,
            "--worker".into(),
            "author-jwt=codex/gpt-6.1-sol".into(),
        ])
    })
    .await
    .unwrap();
    assert_eq!(ws_json["reply"], "wave_start");

    let cases: Vec<(&str, Vec<&str>)> = vec![
        (
            "room_post",
            vec!["post", "#p_123/wave-auth-v2", "@verifier check #4"],
        ),
        (
            "room_post",
            vec!["post", "@sol", "can you take the migration?"],
        ),
        (
            "wait",
            vec![
                "wait",
                "#p_123/wave-auth-v2",
                "--since",
                "0",
                "--timeout",
                "0",
            ],
        ),
        (
            "wave_add_worker",
            vec![
                "wave",
                "add-worker",
                "auth-v2",
                "verifier=claude-code/claude-opus-5-5",
                "--brief",
                "Verify slice",
            ],
        ),
        (
            "wave_resolve",
            vec!["wave", "approve", "auth-v2", "migrator", "int_8f2"],
        ),
        (
            "wave_resolve",
            vec!["wave", "deny", "auth-v2", "migrator", "int_8f2"],
        ),
        (
            "wave_tell",
            vec!["wave", "steer", "auth-v2", "author-jwt", "continue"],
        ),
        (
            "wave_retire_worker",
            vec!["wave", "retire", "auth-v2", "migrator"],
        ),
        ("wave_view", vec!["wave", "status", "auth-v2"]),
        (
            "card_upsert",
            vec![
                "task",
                "add",
                "Wire OAuth refresh",
                "--board",
                "project:p_123",
                "--lane",
                "todo",
                "--assign",
                "@auth-v2/author-jwt",
            ],
        ),
        (
            "card_upsert",
            vec!["task", "move", "project:p_123", "1", "doing"],
        ),
        (
            "board_view",
            vec!["task", "list", "--board", "project:p_123"],
        ),
        (
            "promote_to_letter",
            vec![
                "promote",
                "#p_123/wave-auth-v2",
                "40..44",
                "--to",
                "owner",
                "--subject",
                "auth-v2 ruling",
                "--topic",
                "auth-v2-ruling",
            ],
        ),
        (
            "room_post",
            vec![
                "call",
                "room_post",
                r#"{"room":{"ref":"dm","with":{"kind":"seat","seat":"astra"}},"body":"lgtm"}"#,
            ],
        ),
    ];

    for (expected_reply, raw_args) in cases {
        let runner = run_verb.clone();
        let owned: Vec<String> = raw_args.into_iter().map(str::to_owned).collect();
        let reply = tokio::task::spawn_blocking(move || runner(owned))
            .await
            .unwrap();
        assert_eq!(reply["reply"], expected_reply);
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
