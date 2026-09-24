//! 事件指紋(fingerprint):把同一類錯誤的不同發生實例(不同行號、pid、時間戳記)
//! 正規化成同一組 template,再雜湊成一個穩定、可持久化的 16 進位字串,用來判斷
//! 「這是不是同一個錯誤又發生了一次」。
//!
//! 指紋會寫進 incidents/*.json,必須跨版本穩定,因此雜湊演算法用手刻的 FNV-1a
//! 64 位元(而非 std 的 `DefaultHasher`,後者的演算法/種子不保證穩定,不適合
//! 持久化用途)。
//!
//! 正規化(`normalize_template`)則用 `regex`(本專案已是既有依賴,見
//! Cargo.toml)實作,而非再手刻一套字元掃描器 —— UUID/ISO 時間戳/16 進位/路徑
//! 這幾類 pattern 用正則表達,比手刻狀態機更不容易漏邊界情況,且不會新增依賴。

use crate::incident::{Frame, Source};
use regex::Regex;
use std::sync::LazyLock as Lazy;

/// FNV-1a 64 位元 offset basis / prime(FNV 官方定義的常數)。
const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

/// 手刻 FNV-1a 64。不可以用 `std::collections::hash_map::DefaultHasher`
/// 取代 —— 那個雜湊的演算法與種子並未保證跨 Rust 版本穩定,而指紋要持久化到
/// incidents/*.json 並長期比對,必須每次都算出同一個值。
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

static RE_UUID: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b").unwrap()
});

// ISO-8601 風格,例如 2024-01-02T15:04:05.123Z / 2024-01-02 15:04:05+08:00。
static RE_TS_ISO: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?").unwrap()
});

// syslog 風格,例如 "Jan  2 15:04:05" / "Jan 02 15:04:05"。
static RE_TS_SYSLOG: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\b")
        .unwrap()
});

// 0x 開頭的 16 進位,或 8 碼以上的裸 16 進位字元(含純數字的長串,依規格要求
// 一律當 hex 處理,順序上排在數字正規化之前)。
static RE_HEX_PREFIXED: Lazy<Regex> = Lazy::new(|| Regex::new(r"0[xX][0-9a-fA-F]+").unwrap());
static RE_HEX_BARE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\b[0-9a-fA-F]{8,}\b").unwrap());

// 絕對或相對檔案路徑,例如 /var/log/app.log、./src/main.rs、src/foo/bar.py:42。
static RE_PATH: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?:\.{1,2}/|/)?(?:[A-Za-z0-9_.\-]+/){1,}[A-Za-z0-9_.\-]+").unwrap());

static RE_NUMBER: Lazy<Regex> = Lazy::new(|| Regex::new(r"\d+").unwrap());

static RE_WHITESPACE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());

const TEMPLATE_MAX_CHARS: usize = 256;

/// 把一則錯誤訊息正規化成 template:把會隨每次發生而變動的部分(UUID、時間戳、
/// 16 進位位址、檔案路徑、數字)換成固定 placeholder,讓「同一類錯誤、不同時間
/// /行號/pid」的訊息可以正規化成同一個字串,進而算出同一個指紋。
///
/// 取代順序刻意固定:UUID → 時間戳 → 16 進位 → 路徑 → 數字。順序反過來會出錯,
/// 例如先做數字會把 UUID/時間戳/16 進位裡的數字片段吃掉,导致格式跑掉、
/// 正則失配。
pub fn normalize_template(msg: &str) -> String {
    let s = RE_UUID.replace_all(msg, "<uuid>");
    let s = RE_TS_ISO.replace_all(&s, "<ts>");
    let s = RE_TS_SYSLOG.replace_all(&s, "<ts>");
    let s = RE_HEX_PREFIXED.replace_all(&s, "<hex>");
    let s = RE_HEX_BARE.replace_all(&s, "<hex>");
    let s = RE_PATH.replace_all(&s, "<path>");
    let s = RE_NUMBER.replace_all(&s, "<n>");
    let s = RE_WHITESPACE.replace_all(&s, " ");
    let s = s.trim();

    if s.chars().count() <= TEMPLATE_MAX_CHARS {
        s.to_string()
    } else {
        s.chars().take(TEMPLATE_MAX_CHARS).collect()
    }
}

/// 事件來源的雜湊 key:process 一律是 "process";log 依路徑區分(system-resources
/// 這個合成的 LogFile 也走這條路徑,變成 "log:system-resources")。
fn source_key(source: &Source) -> String {
    match source {
        Source::Process => "process".to_string(),
        Source::LogFile(path) => format!("log:{path}"),
    }
}

/// 取檔名(不含路徑)。純字串處理,不依賴檔案系統是否存在該檔案。
fn basename(path: &str) -> &str {
    path.rsplit(['/', '\\']).next().unwrap_or(path)
}

/// 最上層(第一個)stack frame 的 `basename(file)::function`,沒有 frame 時回傳空字串。
fn top_frame_key(frames: &[Frame]) -> String {
    match frames.first() {
        Some(f) => format!("{}::{}", basename(&f.file), f.function),
        None => String::new(),
    }
}

/// 計算事件指紋:回傳 `(16 碼 16 進位指紋, 正規化後的 template)`。
///
/// 雜湊輸入固定為 `source_key|template|top_frame`,只要來源、正規化後的錯誤
/// 樣式、最上層呼叫點三者都相同,就視為同一種錯誤(不管行號、pid、時間戳記
/// 是否不同)。
pub fn compute(source: &Source, message: &str, frames: &[Frame]) -> (String, String) {
    let template = normalize_template(message);
    let key = source_key(source);
    let top_frame = top_frame_key(frames);
    let hash_input = format!("{key}|{template}|{top_frame}");
    let hash = fnv1a64(hash_input.as_bytes());
    (format!("{hash:016x}"), template)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::incident::Frame;

    fn frame(file: &str, function: &str, line: u32) -> Frame {
        Frame {
            function: function.to_string(),
            file: file.to_string(),
            line,
            column: None,
            raw: String::new(),
        }
    }

    #[test]
    fn fnv1a64_known_vectors() {
        // FNV-1a 64 官方測試向量。
        assert_eq!(fnv1a64(b""), 0xcbf29ce484222325);
        assert_eq!(fnv1a64(b"a"), 0xaf63dc4c8601ec8c);
    }

    #[test]
    fn normalizes_uuid() {
        let s = normalize_template("request 550e8400-e29b-41d4-a716-446655440000 failed");
        assert_eq!(s, "request <uuid> failed");
    }

    #[test]
    fn normalizes_iso_timestamp() {
        let s = normalize_template("error at 2024-01-02T15:04:05.123Z during startup");
        assert_eq!(s, "error at <ts> during startup");
    }

    #[test]
    fn normalizes_syslog_timestamp() {
        let s = normalize_template("Jan 2 15:04:05 host kernel: oom-killer triggered");
        assert_eq!(s, "<ts> host kernel: oom-killer triggered");
    }

    #[test]
    fn normalizes_hex() {
        let s = normalize_template("segfault at address 0xdeadbeef in thread");
        assert_eq!(s, "segfault at address <hex> in thread");
    }

    #[test]
    fn normalizes_bare_long_hex_run() {
        let s = normalize_template("panic code abcdef1234 occurred");
        assert_eq!(s, "panic code <hex> occurred");
    }

    #[test]
    fn normalizes_absolute_path() {
        let s = normalize_template("failed to read /var/log/app/error.log now");
        assert_eq!(s, "failed to read <path> now");
    }

    #[test]
    fn normalizes_relative_path() {
        let s = normalize_template("at ./src/main.rs handler");
        assert_eq!(s, "at <path> handler");
    }

    #[test]
    fn normalizes_remaining_numbers() {
        let s = normalize_template("retry attempt 3 of 5 failed");
        assert_eq!(s, "retry attempt <n> of <n> failed");
    }

    #[test]
    fn collapses_whitespace_and_trims() {
        let s = normalize_template("  too    many   \t spaces  \n here  ");
        assert_eq!(s, "too many spaces here");
    }

    #[test]
    fn truncates_to_256_chars_char_boundary_safe() {
        // 用多位元組字元(中文)組成超過 256 字元的訊息,確保 truncate 是照字元數
        // 切,而不是照 byte 數切(否則會在字元中間切斷、UTF-8 不合法而 panic)。
        let long_msg: String = "錯".repeat(300);
        let s = normalize_template(&long_msg);
        assert_eq!(s.chars().count(), 256);
    }

    #[test]
    fn same_error_different_line_pid_timestamp_same_fingerprint() {
        let frames_a = vec![frame("/app/src/worker.py", "handle_job", 42)];
        let frames_b = vec![frame("/app/src/worker.py", "handle_job", 108)];

        let (fp_a, _) = compute(
            &Source::Process,
            "2024-01-02T15:04:05Z worker pid 1234 crashed with code 0xdead",
            &frames_a,
        );
        let (fp_b, _) = compute(
            &Source::Process,
            "2024-03-09T08:11:59Z worker pid 9999 crashed with code 0xbeef",
            &frames_b,
        );
        assert_eq!(fp_a, fp_b);
    }

    #[test]
    fn different_source_different_fingerprint() {
        let frames = vec![frame("/app/src/worker.py", "handle_job", 42)];
        let (fp_process, _) = compute(&Source::Process, "worker crashed", &frames);
        let (fp_log, _) = compute(
            &Source::LogFile("/var/log/worker.log".to_string()),
            "worker crashed",
            &frames,
        );
        assert_ne!(fp_process, fp_log);
    }

    #[test]
    fn different_message_different_fingerprint() {
        let (fp_a, _) = compute(&Source::Process, "out of memory", &[]);
        let (fp_b, _) = compute(&Source::Process, "connection refused", &[]);
        assert_ne!(fp_a, fp_b);
    }

    #[test]
    fn fingerprint_is_16_hex_chars() {
        let (fp, _) = compute(&Source::Process, "some error", &[]);
        assert_eq!(fp.len(), 16);
        assert!(fp.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
