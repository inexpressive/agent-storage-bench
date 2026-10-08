//! Один «агент» OpenResearch без LLM. Сброс повторяет `flush()` из
//! src/local/chat/mod.rs: каждые FLUSH_INTERVAL (75 мс) — их `Store::open()`
//! (новое соединение, схема и миграции) и их `upsert_chat_message_on_branch`
//! с текущим сообщением целиком. Копируется в src/bench_real.rs, подключается
//! `#[cfg(test)] mod bench_real;` в main.rs и запускается как ignored-тест:
//!   BENCH_ID=a1 BENCH_DUR=30 BENCH_OUT=a1.json ORX_DATA_DIR=… \
//!     orx-test-bin bench_real::agent --exact --ignored --nocapture
#[cfg(test)]
mod tests {
    use crate::local::chat::WirePart;
    use crate::store::{Store, StoredChatMessage, StoredChatSession};
    use std::time::{Duration, Instant};

    const FLUSH_INTERVAL: Duration = Duration::from_millis(75);

    fn session(id: &str) -> StoredChatSession {
        StoredChatSession {
            id: id.into(), project_id: "proj_bench".into(), harness: "claude-code".into(),
            native_session_id: None, title: None, title_source: None, model: None,
            service_tier: None, permission_mode: None, plan_mode: false, plan_reset_pending: false,
            reasoning_level: None, archived: false, context_usage_json: None, bootstrap_context: None,
            goal: None, autonomy: None, active_leaf_id: None, parent_session_id: None,
            side_parent_session_id: None, created_at: 1, updated_at: 1,
        }
    }

    #[test]
    #[ignore]
    fn agent() {
        let id = std::env::var("BENCH_ID").unwrap();
        let dur = Duration::from_secs_f64(std::env::var("BENCH_DUR").unwrap().parse().unwrap());
        let out = std::env::var("BENCH_OUT").unwrap();
        let sid = format!("chat_{id}");
        // сессия создаётся один раз, как при старте агента
        let mut tries = 0;
        loop {
            match Store::open().and_then(|s| s.create_chat_session(&session(&sid))) {
                Ok(()) => break,
                Err(e) => { tries += 1; if tries > 50 { panic!("create session: {e}") } std::thread::sleep(Duration::from_millis(100)) }
            }
        }
        let t0 = Instant::now();
        let (mut lat, mut errs, mut acked) = (Vec::new(), Vec::<String>::new(), Vec::new());
        let (mut msg, mut text, mut n) = (0u32, String::new(), 0u32);
        while t0.elapsed() < dur {
            let tick = Instant::now();
            // рост сообщения: 1 → 64 КБ, новое каждые 160 сбросов (как в синтетике)
            n += 1;
            if n % 160 == 0 { msg += 1; text.clear(); }
            text.push_str(&"x".repeat(400));
            let mid = format!("m_{id}_{msg}");
            let parts = vec![WirePart::text("p0", text.clone())];
            let s = Instant::now();
            let r = Store::open().and_then(|store| {
                store.upsert_chat_message_on_branch(&StoredChatMessage {
                    id: mid.clone(), session_id: sid.clone(), role: "assistant".into(),
                    parts_json: serde_json::to_string(&parts).unwrap(),
                    created_at: 1, completed_at: None, parent_id: None,
                    base_native_session_id: None, result_native_session_id: None,
                })
            });
            let ms = s.elapsed().as_secs_f64() * 1000.0;
            match r {
                Ok(_) => { lat.push((t0.elapsed().as_secs_f64(), ms)); acked.push((mid, text.len())); }
                Err(e) => errs.push(e.to_string()),
            }
            if let Some(rest) = FLUSH_INTERVAL.checked_sub(tick.elapsed()) { std::thread::sleep(rest); }
        }
        // последнее подтверждённое состояние каждого сообщения — для сверки с базой
        let mut last = std::collections::BTreeMap::new();
        for (m, l) in &acked { last.insert(m.clone(), *l); }
        let j = serde_json::json!({"id": id, "lat": lat, "errs": errs, "last": last});
        std::fs::write(out, j.to_string()).unwrap();
    }
}
