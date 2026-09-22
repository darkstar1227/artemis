use crate::incident::Frame;
use regex::Regex;

static NODE_FRAME: &str = r"^\s*at\s+(?:(.+?)\s+\()?(.+?):(\d+):(\d+)\)?$";
static PY_FRAME: &str = r#"^\s*File "(.+?)", line (\d+), in (.+)$"#;

pub struct ErrorEvent {
    pub message: String,
    pub frames: Vec<Frame>,
    pub raw: String,
}

/// 逐行掃描一段輸出,依 error_patterns 找出錯誤起始行,並嘗試往下擷取
/// Node.js / Python 風格的堆疊追蹤,組成一個或多個 ErrorEvent。
pub struct StreamMatcher {
    patterns: Vec<Regex>,
    node_frame: Regex,
    py_frame: Regex,
}

impl StreamMatcher {
    pub fn new(patterns: &[String]) -> anyhow::Result<Self> {
        let patterns = patterns
            .iter()
            .map(|p| Regex::new(p))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            patterns,
            node_frame: Regex::new(NODE_FRAME).unwrap(),
            py_frame: Regex::new(PY_FRAME).unwrap(),
        })
    }

    fn is_error_line(&self, line: &str) -> bool {
        self.patterns.iter().any(|re| re.is_match(line))
    }

    fn is_frame_line(&self, line: &str) -> bool {
        self.node_frame.is_match(line) || self.py_frame.is_match(line)
    }

    fn parse_frame(&self, line: &str) -> Option<Frame> {
        if let Some(caps) = self.node_frame.captures(line) {
            return Some(Frame {
                function: caps
                    .get(1)
                    .map(|m| m.as_str().to_string())
                    .unwrap_or_else(|| "<anonymous>".into()),
                file: caps[2].to_string(),
                line: caps[3].parse().unwrap_or(0),
                column: caps[4].parse().ok(),
                raw: line.trim().to_string(),
            });
        }
        if let Some(caps) = self.py_frame.captures(line) {
            return Some(Frame {
                function: caps[3].to_string(),
                file: caps[1].to_string(),
                line: caps[2].parse().unwrap_or(0),
                column: None,
                raw: line.trim().to_string(),
            });
        }
        None
    }
}

/// 逐行(串流)版本的錯誤偵測器,供監看即時輸出的執行緒使用:
/// 每次餵一行進來,遇到符合 error_patterns 的行就開始蒐集後續堆疊,
/// 直到遇到非堆疊格式的行為止,回傳組好的 ErrorEvent。
pub struct LiveScanner<'a> {
    matcher: &'a StreamMatcher,
    pending: Option<(String, Vec<String>, Vec<Frame>)>,
}

impl<'a> LiveScanner<'a> {
    pub fn new(matcher: &'a StreamMatcher) -> Self {
        Self {
            matcher,
            pending: None,
        }
    }

    /// 回傳:若這一行讓某個先前累積的事件確定結束,則回傳該事件。
    pub fn feed(&mut self, line: &str) -> Option<ErrorEvent> {
        if self.pending.is_some() {
            if self.matcher.is_frame_line(line) {
                let (_, raw_lines, frames) = self.pending.as_mut().unwrap();
                if let Some(frame) = self.matcher.parse_frame(line) {
                    frames.push(frame);
                }
                raw_lines.push(line.to_string());
                if frames.len() >= 50 {
                    return self.flush();
                }
                return None;
            } else {
                let finished = self.flush();
                if self.matcher.is_error_line(line) {
                    self.start(line);
                }
                return finished;
            }
        }

        if self.matcher.is_error_line(line) {
            self.start(line);
        }
        None
    }

    fn start(&mut self, line: &str) {
        self.pending = Some((line.trim().to_string(), vec![line.to_string()], Vec::new()));
    }

    fn flush(&mut self) -> Option<ErrorEvent> {
        self.pending.take().map(|(message, raw_lines, frames)| ErrorEvent {
            message,
            frames,
            raw: raw_lines.join("\n"),
        })
    }

    /// 串流結束時呼叫,把最後尚未結案的事件收尾。
    pub fn finish(&mut self) -> Option<ErrorEvent> {
        self.flush()
    }
}
