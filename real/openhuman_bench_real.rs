//! Один «агент» OpenHuman без LLM: их tinyagents-session (sessions.db, WAL,
//! кэш соединения на процесс). На границе каждого шага — их `record_message`
//! и `record_tool_call`, как пишет ход агента OpenHuman. Шаг — BENCH_STEP_MS
//! (по умолчанию 250 мс; живой шаг с вызовом модели реже — это верхняя оценка).
//! Копируется в crates/tinyagents-session/tests/bench_real.rs:
//!   BENCH_ID=a1 BENCH_DUR=30 BENCH_OUT=a1.json BENCH_WS=… bench_real-<hash> agent --exact --ignored
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tinyagents_session::ops::{record_message, record_session_start, record_tool_call};

#[test]
#[ignore]
fn agent() {
    let id = std::env::var("BENCH_ID").unwrap();
    let dur = Duration::from_secs_f64(std::env::var("BENCH_DUR").unwrap().parse().unwrap());
    let out = std::env::var("BENCH_OUT").unwrap();
    let ws = PathBuf::from(std::env::var("BENCH_WS").unwrap());
    let step = Duration::from_millis(std::env::var("BENCH_STEP_MS").ok().and_then(|s| s.parse().ok()).unwrap_or(250));
    let sid = format!("sess_{id}");
    let mut tries = 0;
    while let Err(e) = record_session_start(&ws, &sid, "bench", "bench", &sid, None, None, None, None, None) {
        tries += 1;
        if tries > 50 { panic!("session start: {e}") }
        std::thread::sleep(Duration::from_millis(100));
    }
    let body = "x".repeat(4000);
    let t0 = Instant::now();
    let (mut lat, mut errs, mut acked) = (Vec::new(), Vec::<String>::new(), 0u64);
    while t0.elapsed() < dur {
        let tick = Instant::now();
        let s = Instant::now();
        let r = record_message(&ws, &sid, "assistant", &body, None, Some(10), Some(10), None)
            .and_then(|mid| record_tool_call(&ws, &sid, Some(mid), "bash", Some("{}"), Some(&body), "ok", Some(1)));
        let ms = s.elapsed().as_secs_f64() * 1000.0;
        match r {
            Ok(_) => { lat.push((t0.elapsed().as_secs_f64(), ms)); acked += 1; }
            Err(e) => errs.push(e.to_string()),
        }
        if let Some(rest) = step.checked_sub(tick.elapsed()) { std::thread::sleep(rest); }
    }
    let j = serde_json::json!({"id": id, "sid": sid, "lat": lat, "errs": errs, "acked": acked});
    std::fs::write(out, j.to_string()).unwrap();
}
