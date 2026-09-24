//! Milestone 1 step 6:跨執行緒共用的關閉旗標。
//!
//! `main.rs::cmd_watch` 註冊 SIGTERM/SIGINT 處理器,收到訊號就把這個
//! `Arc<AtomicBool>` 設成 `true`。`supervisor::watch`/`watcher::watch_log_files`/
//! `resource::watch_resources` 各自既有的輪詢迴圈在每個 tick 順便檢查它,
//! 檢查到就結束迴圈、回傳 —— 不再重啟被監控程序、不再繼續監看。
//!
//! 這是刻意選的最簡單機制:一個 `AtomicBool` + 訊號處理器,而不是額外開一個
//! 「關閉協調」執行緒或用 channel 廣播 —— 因為所有需要回應關閉的迴圈本來就
//! 已經有短週期的 poll(200ms/500ms/poll_interval_ms),多檢查一個 bool 幾乎
//! 零成本,也不需要每個迴圈都額外處理「channel 已關閉」之類的分支。

use anyhow::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub type ShutdownFlag = Arc<AtomicBool>;

pub fn new_flag() -> ShutdownFlag {
    Arc::new(AtomicBool::new(false))
}

pub fn is_set(flag: &ShutdownFlag) -> bool {
    flag.load(Ordering::SeqCst)
}

/// 睡 `dur`,但每 200ms 檢查一次 `flag`,設了就提早返回——讓使用者可設的長 poll 間隔
/// (例如 `resources.poll_interval_ms`)不會把關閉拖過 `shutdown_grace_secs`。
pub fn sleep_interruptible(flag: &ShutdownFlag, dur: std::time::Duration) {
    let step = std::time::Duration::from_millis(200);
    let deadline = std::time::Instant::now() + dur;
    while !is_set(flag) {
        let now = std::time::Instant::now();
        if now >= deadline {
            break;
        }
        std::thread::sleep(step.min(deadline - now));
    }
}

/// 主動要求關閉(例如 supervisor 正常結束後,通知其他還在跑的監看執行緒
/// 也一起收尾)。跟收到訊號時的效果完全一樣。
pub fn request(flag: &ShutdownFlag) {
    flag.store(true, Ordering::SeqCst);
}

/// 註冊 SIGTERM/SIGINT:
/// - 第一次收到 → 把 `flag` 設成 `true`(`signal_hook::flag::register`),
///   讓各執行緒的下一個 poll tick 自然收尾。
/// - `flag` 已經是 `true` 時又收到一次(使用者連按兩次 Ctrl+C,或卡住的
///   escalation 讓人等得不耐煩)→ 直接讓行程結束
///   (`signal_hook::flag::register_conditional_shutdown`),不用等 grace
///   period —— 這是 nice-to-have,不影響正常關閉路徑的正確性。
///
/// **註冊順序很重要**(`signal-hook` 的 `flag` module 文件明講):
/// `register_conditional_shutdown` 必須先註冊,`register` 後註冊。同一次
/// 訊號送達時,同一個 signal 的多個 handler 會按註冊順序依序執行 —— 如果
/// 順序反過來(`register` 先跑),第一次收到訊號時 `register` 的 handler
/// 會先把 `flag` 設成 `true`,緊接著 `register_conditional_shutdown` 的
/// handler 才跑,讀到的 `flag` 已經是 `true`,於是「第一次」訊號就被誤判成
/// 「第二次」而立刻強制結束行程 —— 完全繞過 grace period,等於這個
/// nice-to-have 直接吃掉了正常的優雅關閉路徑。曾經真的以錯誤順序寫過,
/// 手動用 `kill -TERM` 測試時行程在幾毫秒內就結束、子程序來不及被
/// `child.kill()`,才發現這個順序限制。
pub fn install(flag: &ShutdownFlag) -> Result<()> {
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register_conditional_shutdown(sig, 130, flag.clone())?;
        signal_hook::flag::register(sig, flag.clone())?;
    }
    Ok(())
}
