//! Milestone 1 step 4:把 `Store::record` 的「偵測執行緒同步做完一切」拆成
//! 非同步的 intake + escalation-worker 兩個執行緒,讓 `submit()` 永不阻塞
//! 呼叫端(supervisor/watcher/resource 的偵測執行緒)。
//!
//! ## 執行緒與 channel 設計
//!
//! ```text
//!            submit()                 job_tx (unbounded, 手動以
//!  偵測執行緒 ──try_send──▶ [bounded]  ──pending 計數把關──▶ 升級處置
//!  (supervisor/            detected      intake              worker執行緒
//!   watcher/resource)      channel       執行緒                  │
//!                          (容量        (唯一寫 incidents/       │
//!                           max_queue)   的執行緒)                │
//!                                          ▲                     │
//!                                          └──── done_tx ────────┘
//!                                            (unbounded,低流量)
//! ```
//!
//! - **Detected**(偵測執行緒 → intake):`std::sync::mpsc::sync_channel`,
//!   容量 `incidents.max_queue`。`Recorder::submit` 只呼叫 `try_send`,滿了
//!   就丟到 overflow map(不阻塞、不掉次數),絕不 `send`(會阻塞)。
//! - **EscalationDone**(worker → intake):`std::sync::mpsc::channel`
//!   (unbounded)。量很小(受 `max_pending_escalations` 限制),用 unbounded
//!   是為了讓 worker 執行緒送回結果這一步「永遠不會被 intake 忙碌卡住」—— 唯一
//!   會讓 worker 阻塞的地方就只剩它自己呼叫 `Escalator::escalate` 那一行,
//!   這正是我們要的(escalation 慢不會回頭拖慢 intake 或偵測執行緒)。
//! - **Job**(intake → worker):也是 unbounded `mpsc::channel`,但 intake
//!   送出前會先檢查自己維護的 `pending: HashSet<id>` 是否已經達到
//!   `max_pending_escalations`,達到就直接視為「滿了」不送。這裡刻意不用
//!   `sync_channel` 的 `try_send`/容量來做這件事:worker 一次只處理一個
//!   job,一旦 `recv()` 把 job 從 channel 拿出來,channel 的緩衝區就空了,
//!   但那個 job 其實還在執行中(逻辑上仍然「pending」)——用 channel 容量
//!   判斷「Full」會低估真正在途的數量,在 `max_pending_escalations == 1`
//!   時會出現 race(worker 剛把 job 拿出、還沒真正跑完的空窗期,channel
//!   容量顯示為空,下一個 job 卻應該被視為「滿了」)。intake 自己維護的
//!   `pending` 集合(在 `EscalationDone` 收到時才移除)沒有這個問題,是
//!   單一執行緒內的純狀態,沒有 race。
//!
//! intake 迴圈本身是單一 `recv_timeout(100ms)` 輪詢 Detected channel,每輪
//! 之後用 `try_recv` 把 EscalationDone 全部瀝乾(不阻塞),再視需要跑一次
//! 「Tick」(節流重寫 + 攤平 overflow 次數,見 `handle_tick`)。這比另外開一個
//! 真正的計時器執行緒或使用 `Tick` 訊息類型簡單,且因為 EscalationDone 量小、
//! `try_recv` 便宜,不會讓 Detected 的即時性打折扣。
//!
//! ## Shutdown(Milestone 1 step 6)
//!
//! 有兩條路都會走到同一個收尾流程(`run_shutdown_sequence`):
//!
//! 1. **顯式關閉**(production 走這條):`Recorder::shutdown_and_join` 透過
//!    `control_tx`(`done_tx` 的另一份 clone,intake 本來就在 poll 的同一個
//!    unbounded channel)送出 `IntakeEvent::Shutdown`。intake 在 done_rx 的
//!    `try_recv` 迴圈裡認出這個訊息就直接進入收尾,**不必等任何 channel
//!    斷線**。這是這次修的 bug 的關鍵:舊版只靠「所有 `Recorder` clone 都
//!    被 drop」這件事觸發收尾,但 `watcher`/`resource` 的監看執行緒過去是
//!    無窮迴圈,永遠不會自然 drop 掉手上的 clone;`cmd_watch` 一 return,
//!    intake 執行緒就被整個行程的結束直接砍斷,連最後一次 flush 都來不及
//!    跑,尾段幾筆事件的 occurrence_count/last_seen 就憑空消失了。
//!    **重要前提**:呼叫 `shutdown_and_join` 前,呼叫端必須確保所有其他
//!    還持有 `Recorder` clone 的執行緒(`watcher`/`resource`)都已經先
//!    停止呼叫 `submit()`(`cmd_watch` 的作法是靠 `shutdown::ShutdownFlag`
//!    讓那些執行緒的迴圈自己先返回、`join()` 完再呼叫這裡)——否則收尾流程
//!    開始瀝乾 Detected channel之後才送達的事件會停留在 channel 緩衝區裡,
//!    既不會被計入最終狀態,也不會被當成遺失來記 log。
//! 2. **所有 sender 被 drop**(單元測試沿用這條、也是保底):`detected_tx`
//!    的最後一份 clone 消失時,`detected_rx.recv_timeout` 回報
//!    `Disconnected`,intake 一樣進入同一個 `run_shutdown_sequence`。
//!
//! `run_shutdown_sequence` 做的事(不論從哪條路進來都一樣):瀝乾 channel
//! 裡剩下的 Detected、**強制**跑一次 `handle_tick`(`rewrite_interval` 傳
//! `Duration::zero()`,忽略節流,把 overflow 次數攤平、所有 dirty 的
//! incident 全部落地,不是等下一次自然 Tick)、drop `job_tx`(讓 worker
//! 在處理完手上的 job、且沒有新 job 進來後自然結束)、最多等
//! `shutdown_grace_secs` 秒讓在途的升級處置回報 `EscalationDone`(每筆一
//! 回報就照常落地)、最後再強制 flush 一次收尾。grace 內沒回報完的
//! escalation 就放著:對應的 incident JSON 早在送出 job 前就已經以
//! `status = Escalating` 落地過,不是遺失,只是「中斷在升級處置途中」
//! ——step 5 的啟動重建之後可以把這種狀態改標成 `Open`("interrupted"),
//! 這裡先不動。

use crate::agent_client;
use crate::central_client;
use crate::config::{Config, IncidentsConfig};
use crate::dedup::{Decision, Deduper, Entry};
use crate::fingerprint;
use crate::incident::{EscalationReport, Incident, IncidentStatus};
use crate::store::Store;
use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration as StdDuration, Instant};

/// 把「事件寫落地」拆成幾個獨立的原語,方便測試用假實作取代真正的檔案
/// I/O/agent_service/central collector 呼叫。真正的實作見 `RealSink`。
pub trait Sink: Send + Sync {
    fn write_json(&self, inc: &Incident) -> Result<()>;
    fn render_markdown(&self, inc: &Incident);
    fn push_central(&self, inc: &Incident);
    fn snapshot_diagnostics(&self, inc: &mut Incident);
}

/// 把「呼叫 agent_service 執行四層分級自主處置」拆成一個獨立介面,方便測試
/// 用假實作(包括故意會卡住的假實作)取代真正的 HTTP 呼叫。
pub trait Escalator: Send + Sync {
    fn escalate(&self, inc: &Incident) -> Result<EscalationReport>;
}

/// `RealSink`:包著現有的 `Store` + `Config`,行為跟 Milestone 1 step 4 之前
/// `Store::record` 內嵌的邏輯完全一致(分析器失敗只記 log、central 推送
/// fire-and-forget)。
pub struct RealSink {
    store: Store,
    cfg: Arc<Config>,
}

impl RealSink {
    pub fn new(store: Store, cfg: Arc<Config>) -> Self {
        Self { store, cfg }
    }
}

impl Sink for RealSink {
    fn write_json(&self, inc: &Incident) -> Result<()> {
        self.store.write_json(inc)?;
        Ok(())
    }

    fn render_markdown(&self, inc: &Incident) {
        if let Err(e) = self.store.render_markdown(inc, &self.cfg) {
            eprintln!("[artemis] 根因分析執行失敗,已保留原始事件 JSON:{e}");
        }
    }

    fn push_central(&self, inc: &Incident) {
        if !self.cfg.central.enabled {
            return;
        }
        let report_markdown = fs::read_to_string(self.store.report_path(&inc.id)).ok();
        if let Err(e) = central_client::push(inc, report_markdown.as_deref(), &self.cfg) {
            eprintln!("[artemis] 推送事件到 central collector 失敗,不影響本機記錄:{e}");
        }
    }

    fn snapshot_diagnostics(&self, inc: &mut Incident) {
        if self.cfg.diagnostics.enabled && !self.cfg.diagnostics.commands.is_empty() {
            inc.diagnostics_history = crate::store::collect_diagnostics_history(&self.cfg);
        }
    }
}

/// `RealEscalator`:包著 `Config`,委派給既有的 `agent_client::escalate`。
pub struct RealEscalator {
    cfg: Arc<Config>,
}

impl RealEscalator {
    pub fn new(cfg: Arc<Config>) -> Self {
        Self { cfg }
    }
}

impl Escalator for RealEscalator {
    fn escalate(&self, inc: &Incident) -> Result<EscalationReport> {
        agent_client::escalate(inc, &self.cfg)
    }
}

/// 時鐘注入點:production 用 `Utc::now`,測試用可控制的假時鐘,讓去重/
/// storm guard/節流重寫的時間判斷完全不受真實執行緒排程時間影響。
pub type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

struct EscalationJob {
    id: String,
    fp: String,
    incident: Incident,
}

// `Shutdown` 是個 0 大小的變體,跟帶 `EscalationReport` 的 `EscalationDone`
// 大小差很多;這個 enum 只走一個非熱路徑的控制 channel(整個生命週期最多
// 收到個位數則訊息),沒有為了省幾十個 byte 而把 `EscalationDone` 的欄位
// 包一層 `Box` 的必要。
#[allow(clippy::large_enum_variant)]
enum IntakeEvent {
    EscalationDone {
        id: String,
        fp: String,
        result: Result<EscalationReport, String>,
    },
    /// 顯式關閉要求(見模組文件的「Shutdown」一節)。跟 `EscalationDone` 走
    /// 同一個 channel,因為兩者都是「intake 之外的人要通知 intake 一件
    /// 事」,不需要為了一個訊息類型再多開一個 channel。
    Shutdown,
}

/// intake 執行緒持有的可變狀態(只有這個執行緒會碰它,不需要鎖)。
struct IntakeState {
    deduper: Deduper,
    /// 目前仍「活著」(非 Resolved)的 incident,以 id 為 key。resolve 的
    /// 清除邏輯是 step 5 的 TODO(見 `handle_tick` 註解)。
    active: HashMap<String, Incident>,
    /// 指紋 → 目前代表它的 incident id,方便 overflow 折算次數時找到目標。
    fp_active: HashMap<String, String>,
    /// 有未落地的次數異動(occurrence_count/last_seen)、等下一次 Tick
    /// 節流寫入的 incident id 集合。
    dirty: HashSet<String>,
    /// 每個 incident 最近一次「由 Tick 觸發」的落地時間,節流用。
    last_flush: HashMap<String, DateTime<Utc>>,
    /// 已送出、還在等 `EscalationDone` 回報的 incident id。
    pending: HashSet<String>,
}

/// `submit()` 永不阻塞的 handle。`Clone` 便宜(全是 channel sender/Arc)。
#[derive(Clone)]
pub struct Recorder {
    detected_tx: SyncSender<Incident>,
    overflow: Arc<Mutex<HashMap<String, u64>>>,
    last_drop_log_ms: Arc<AtomicI64>,
    /// `done_tx` 的一份 clone,讓任何持有 `Recorder` 的人都能送出
    /// `IntakeEvent::Shutdown`。實務上只有 `cmd_watch` 留著的那個 clone
    /// 會真的呼叫 `shutdown_and_join`,但這裡不做額外的「唯一 owner」限制
    /// ——重複呼叫、或多個 clone 都呼叫,intake 端也只是重複收到
    /// Shutdown、無害地再跑一次收尾。
    control_tx: Sender<IntakeEvent>,
}

impl Recorder {
    /// production 建構子:接上真正的 `Store`/agent_client/central_client。
    pub fn new(cfg: Arc<Config>) -> Result<(Recorder, thread::JoinHandle<()>)> {
        let store = Store::new(&cfg.incidents_dir)?;
        let sink: Arc<dyn Sink> = Arc::new(RealSink::new(store, cfg.clone()));
        let escalator: Arc<dyn Escalator> = Arc::new(RealEscalator::new(cfg.clone()));
        let clock: Clock = Arc::new(Utc::now);
        Ok(Self::with_deps(
            cfg.incidents.clone(),
            cfg.escalation.enabled,
            sink,
            escalator,
            clock,
            cfg.incidents_dir.clone(),
            cfg.name.clone(),
        ))
    }

    /// 測試/依賴注入用建構子:換掉 Sink/Escalator/Clock,行為(去重、節流、
    /// pending 上限)完全比照 production。`incidents_dir`/`project` 是
    /// Milestone 1 step 5 啟動重建掃描用的——測試通常指向一個乾淨/不存在的
    /// 目錄,重建就是無事可做的 no-op(見 `rebuild_state` 對讀不到目錄的
    /// 處理)。
    #[allow(clippy::too_many_arguments)]
    pub fn with_deps(
        incidents_cfg: IncidentsConfig,
        escalation_enabled: bool,
        sink: Arc<dyn Sink>,
        escalator: Arc<dyn Escalator>,
        clock: Clock,
        incidents_dir: String,
        project: String,
    ) -> (Recorder, thread::JoinHandle<()>) {
        let (detected_tx, detected_rx) =
            mpsc::sync_channel::<Incident>(incidents_cfg.max_queue.max(1));
        let (job_tx, job_rx) = mpsc::channel::<EscalationJob>();
        let (done_tx, done_rx) = mpsc::channel::<IntakeEvent>();
        let control_tx = done_tx.clone();

        thread::spawn(move || run_escalation_worker(job_rx, escalator, done_tx));

        let overflow = Arc::new(Mutex::new(HashMap::new()));
        let overflow_for_intake = overflow.clone();
        let handle = thread::spawn(move || {
            run_intake(
                detected_rx,
                done_rx,
                job_tx,
                overflow_for_intake,
                incidents_cfg,
                escalation_enabled,
                sink,
                clock,
                incidents_dir,
                project,
            );
        });

        let recorder = Recorder {
            detected_tx,
            overflow,
            last_drop_log_ms: Arc::new(AtomicI64::new(0)),
            control_tx,
        };
        (recorder, handle)
    }

    /// 提交一筆新偵測到的事件。**永不阻塞**:滿了就記進 overflow map(下一次
    /// Tick 會把次數折算進對應指紋目前的 incident),channel 斷了就記 log
    /// 放棄這筆(intake 執行緒已經不在了,沒有地方可以記錄)。
    pub fn submit(&self, inc: Incident) {
        match self.detected_tx.try_send(inc) {
            Ok(()) => {}
            Err(TrySendError::Full(inc)) => self.record_overflow(inc),
            Err(TrySendError::Disconnected(inc)) => {
                eprintln!(
                    "[artemis] recorder intake 執行緒已結束,事件遺失:{}",
                    inc.id
                );
            }
        }
    }

    fn record_overflow(&self, inc: Incident) {
        let fp = inc.fingerprint.clone().unwrap_or_default();
        {
            let mut map = self.overflow.lock().unwrap();
            *map.entry(fp.clone()).or_insert(0) += 1;
        }
        // 只記 rate-limited 警告(至多每秒一次),避免 storm 情境下洗版 log。
        let now_ms = Utc::now().timestamp_millis();
        let last = self.last_drop_log_ms.load(Ordering::Relaxed);
        if now_ms - last >= 1000
            && self
                .last_drop_log_ms
                .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            eprintln!(
                "[artemis] recorder 佇列已滿,事件將併入既有事件的次數統計(fingerprint={fp})"
            );
        }
    }

    /// 送出顯式關閉要求(不等待)。`shutdown_and_join` 是大多數呼叫端該用
    /// 的入口;這個方法單獨留著是給想要「發出去、自己另外處理 join」的
    /// 情境用(目前沒有 production 呼叫端這樣用,但保留彈性)。
    pub fn request_shutdown(&self) {
        // 送不出去代表 intake 已經自己結束了(例如所有 clone 都先被 drop
        // 走完了 Disconnected 那條路)——無害,忽略即可。
        let _ = self.control_tx.send(IntakeEvent::Shutdown);
    }

    /// 觸發 intake 收尾並等它真正結束。**呼叫前必須確保沒有其他執行緒還在
    /// 用其他 `Recorder` clone 呼叫 `submit()`**(見模組文件「Shutdown」一
    /// 節的說明)——`cmd_watch` 的作法是先讓 watcher/resource 的監看執行緒
    /// 透過 `ShutdownFlag` 自己停下來、`join()` 完,才呼叫這裡。
    ///
    /// 消耗 `self`:呼叫後這個 `Recorder` clone 不該再被用來 `submit`
    /// (channel 那端的 intake 隨時可能已經在收尾或已經結束)。
    pub fn shutdown_and_join(self, intake_handle: thread::JoinHandle<()>) {
        self.request_shutdown();
        if let Err(e) = intake_handle.join() {
            eprintln!("[artemis] recorder intake 執行緒 join 失敗:{e:?}");
        }
    }
}

fn write_json(sink: &dyn Sink, inc: &Incident) {
    if let Err(e) = sink.write_json(inc) {
        eprintln!("[artemis] 事件 JSON 寫入失敗:{e}");
    }
}

#[allow(clippy::too_many_arguments)]
fn record_escalate_path(
    state: &mut IntakeState,
    sink: &dyn Sink,
    job_tx: &Sender<EscalationJob>,
    incidents_cfg: &IncidentsConfig,
    escalation_enabled: bool,
    now: DateTime<Utc>,
    fp: String,
    mut inc: Incident,
    is_recurrence: bool,
) {
    sink.snapshot_diagnostics(&mut inc);

    if !escalation_enabled {
        // escalation 整個關閉:仍然走去重(累積次數/建立新 incident 的邏輯
        // 不變),但不送任何 job,狀態停在 Open。
        inc.status = IncidentStatus::Open;
        write_json(sink, &inc);
        sink.render_markdown(&inc);
        sink.push_central(&inc);
        let id = inc.id.clone();
        if is_recurrence {
            state
                .deduper
                .on_recurrence(fp.clone(), id.clone(), IncidentStatus::Open, now, false);
        } else {
            state
                .deduper
                .on_new(fp.clone(), id.clone(), IncidentStatus::Open, now, false);
        }
        state.fp_active.insert(fp, id.clone());
        state.active.insert(id, inc);
        return;
    }

    inc.status = IncidentStatus::Escalating;
    write_json(sink, &inc);
    sink.render_markdown(&inc);
    sink.push_central(&inc);

    let id = inc.id.clone();
    let has_capacity = state.pending.len() < incidents_cfg.max_pending_escalations;
    let accepted = has_capacity
        && job_tx
            .send(EscalationJob {
                id: id.clone(),
                fp: fp.clone(),
                incident: inc.clone(),
            })
            .is_ok();

    if accepted {
        state.pending.insert(id.clone());
        if is_recurrence {
            state
                .deduper
                .on_recurrence(fp.clone(), id.clone(), IncidentStatus::Escalating, now, true);
        } else {
            state
                .deduper
                .on_new(fp.clone(), id.clone(), IncidentStatus::Escalating, now, true);
        }
    } else {
        inc.status = IncidentStatus::Suppressed;
        inc.status_reason = Some("queue_full".to_string());
        write_json(sink, &inc);
        sink.render_markdown(&inc);
        if is_recurrence {
            state.deduper.on_recurrence(
                fp.clone(),
                id.clone(),
                IncidentStatus::Suppressed,
                now,
                false,
            );
        } else {
            state
                .deduper
                .on_new(fp.clone(), id.clone(), IncidentStatus::Suppressed, now, false);
        }
    }

    state.fp_active.insert(fp, id.clone());
    state.active.insert(id, inc);
}

fn record_suppressed_path(
    state: &mut IntakeState,
    sink: &dyn Sink,
    now: DateTime<Utc>,
    fp: String,
    mut inc: Incident,
    reason: &'static str,
    is_recurrence: bool,
) {
    sink.snapshot_diagnostics(&mut inc);
    inc.status = IncidentStatus::Suppressed;
    inc.status_reason = Some(reason.to_string());
    write_json(sink, &inc);
    sink.render_markdown(&inc);
    sink.push_central(&inc);

    let id = inc.id.clone();
    if is_recurrence {
        state
            .deduper
            .on_recurrence(fp.clone(), id.clone(), IncidentStatus::Suppressed, now, false);
    } else {
        state
            .deduper
            .on_new(fp.clone(), id.clone(), IncidentStatus::Suppressed, now, false);
    }
    state.fp_active.insert(fp, id.clone());
    state.active.insert(id, inc);
}

fn handle_append(state: &mut IntakeState, id: String, fp: &str, now: DateTime<Utc>) {
    if let Some(inc) = state.active.get_mut(&id) {
        inc.occurrence_count += 1;
        inc.last_seen = Some(now);
    }
    state.deduper.on_append(fp, now);
    state.dirty.insert(id);
}

#[allow(clippy::too_many_arguments)]
fn handle_detected(
    state: &mut IntakeState,
    sink: &dyn Sink,
    job_tx: &Sender<EscalationJob>,
    incidents_cfg: &IncidentsConfig,
    escalation_enabled: bool,
    now: DateTime<Utc>,
    mut inc: Incident,
) {
    let fp = inc.fingerprint.clone().unwrap_or_default();
    match state.deduper.decide(&fp, now) {
        Decision::NewEscalate => {
            record_escalate_path(
                state,
                sink,
                job_tx,
                incidents_cfg,
                escalation_enabled,
                now,
                fp,
                inc,
                false,
            );
        }
        Decision::NewSuppressed(reason) => {
            record_suppressed_path(state, sink, now, fp, inc, reason, false);
        }
        Decision::Append { id } => {
            handle_append(state, id, &fp, now);
        }
        Decision::Recurrence {
            prev,
            escalate,
            reason,
        } => {
            inc.recurrence_of = Some(prev);
            if escalate {
                record_escalate_path(
                    state,
                    sink,
                    job_tx,
                    incidents_cfg,
                    escalation_enabled,
                    now,
                    fp,
                    inc,
                    true,
                );
            } else {
                record_suppressed_path(
                    state,
                    sink,
                    now,
                    fp,
                    inc,
                    reason.unwrap_or("recurrence"),
                    true,
                );
            }
        }
    }
}

fn handle_escalation_done(
    state: &mut IntakeState,
    sink: &dyn Sink,
    id: String,
    fp: String,
    result: Result<EscalationReport, String>,
) {
    state.pending.remove(&id);
    let Some(inc) = state.active.get_mut(&id) else {
        return;
    };
    match result {
        Ok(report) => {
            inc.status = if report.final_resolved {
                IncidentStatus::Mitigated
            } else {
                IncidentStatus::Open
            };
            inc.status_reason = None;
            inc.escalation = Some(report);
        }
        Err(e) => {
            inc.status = IncidentStatus::Open;
            inc.status_reason = Some("escalation_failed".to_string());
            eprintln!("[artemis] 升級處置失敗:{e}");
        }
    }
    state.deduper.set_status(&fp, inc.status);
    write_json(sink, inc);
    sink.render_markdown(inc);
    sink.push_central(inc);
}

/// 每次 Tick 最多執行「標記 Resolved」(含跑一次分析器)的筆數上限,見
/// `handle_tick` 內對應那段的說明。
const MAX_RESOLVES_PER_TICK: usize = 16;

fn handle_tick(
    state: &mut IntakeState,
    sink: &dyn Sink,
    overflow: &Arc<Mutex<HashMap<String, u64>>>,
    rewrite_interval: ChronoDuration,
    now: DateTime<Utc>,
) {
    let drained: Vec<(String, u64)> = {
        let mut map = overflow.lock().unwrap();
        map.drain().collect()
    };
    for (fp, count) in drained {
        if count == 0 {
            continue;
        }
        if let Some(id) = state.fp_active.get(&fp).cloned() {
            if let Some(inc) = state.active.get_mut(&id) {
                inc.occurrence_count += count;
                inc.last_seen = Some(now);
                state.dirty.insert(id);
            }
        }
    }

    let due: Vec<String> = state
        .dirty
        .iter()
        .filter(|id| {
            state
                .last_flush
                .get(id.as_str())
                .is_none_or(|t| now - *t >= rewrite_interval)
        })
        .cloned()
        .collect();
    for id in due {
        if let Some(inc) = state.active.get(&id) {
            write_json(sink, inc);
        }
        state.dirty.remove(&id);
        state.last_flush.insert(id, now);
    }

    // Resolve tick(Milestone 1 step 5):`Deduper::resolve_due` 掃描出所有
    // 閒置超過 `resolve_after_secs` 的 Open/Mitigated/Suppressed 指紋(見
    // `resolve_due` 文件對 Suppressed 也一併處理的說明)。這裡同時也是
    // `active`(以及 `dirty`/`last_flush`)的唯一清除點——一個 incident 一旦
    // 被標記 Resolved 就不會再被 Append,留在 `active` 裡沒有意義,清掉才能
    // 讓這個 map 隨時間有界。
    //
    // 每個 Tick 最多處理 `MAX_RESOLVES_PER_TICK` 筆——`render_markdown`
    // 每筆都會 fork 一個 `uv` 子行程跑分析器,重啟後如果一次到期一大批
    // (例如長時間停機、幾百個指紋同時逾期),不設上限會讓這次 Tick(進而
    // 整個 intake 執行緒,因為它是單執行緒依序處理)被卡住一段不小的時間,
    // 期間新進來的 Detected 事件全部要等。沒處理到的那些不會遺失——
    // `resolve_due` 每次都重新計算,狀態沒被改掉的 entry 下一次 Tick
    // 一樣會被選中,而且因為 `resolve_due` 保證回傳順序是 `last_seen`
    // 由舊到新(見它的文件),優先被處理的一定是逾期最久的那些,不會有
    // entry 因為 HashMap 迭代順序不固定而被無限期跳過。
    let resolved_ids = state.deduper.resolve_due(now);
    for id in resolved_ids.into_iter().take(MAX_RESOLVES_PER_TICK) {
        let fp = state
            .active
            .get(&id)
            .and_then(|inc| inc.fingerprint.clone());
        if let Some(inc) = state.active.get_mut(&id) {
            inc.status = IncidentStatus::Resolved;
            inc.status_reason = Some("no_recurrence".to_string());
            write_json(sink, inc);
            sink.render_markdown(inc);
            sink.push_central(inc);
        }
        if let Some(fp) = fp {
            state.deduper.set_status(&fp, IncidentStatus::Resolved);
        }
        state.active.remove(&id);
        state.dirty.remove(&id);
        state.last_flush.remove(&id);
    }

    // 把久到不再影響任何未來決策的 Resolved entry 從 Deduper 修剪掉,見
    // `Deduper::prune_expired` 的文件。
    state.deduper.prune_expired(now);
}

/// Milestone 1 step 5:行程啟動、intake 開始處理任何 Detected 事件之前,
/// 掃描 `incidents_dir` 底下既有的事件 JSON,重建 `Deduper` 的去重/冷卻/
/// storm guard 狀態與 intake 的 `active` map——否則每次重啟,舊事件的
/// 去重狀態就全部歸零,cooldown/dedup window 形同虛設。
///
/// 詳細規則見下方各步驟註解;整體策略是「只用得到、算得便宜的資訊才讀」:
/// **檔案 mtime**(不是檔名前綴!)比
/// `horizon = max(dedup_window_secs, cooldown_secs, resolve_after_secs)`
/// 還舊的檔案直接跳過、連 JSON 都不解析——理由見 `Deduper::prune_expired`
/// 的文件:比這個 horizon 還舊的事件,不管內容是什麼,都不會再改變任何
/// 未來的去重決策。
///
/// **為什麼是 mtime,不是檔名前綴的建立時間**:`write_json` 在每次累加次數
/// 的節流重寫、以及最終狀態更新時都會整份重寫檔案,所以 mtime 會跟著
/// `last_seen` 一起前進;檔名前綴(`Incident::new_id` 的
/// `%Y%m%dT%H%M%S`)則是這個 incident 第一次被建立的時間,終其一生不會變。
/// 用檔名前綴當「這個檔案是不是太舊」的判斷,會把「2 小時前建立、但 1
/// 分鐘前才又出現過(last_seen 很新)、horizon 只有 1 小時」的 incident
/// 誤判成太舊而跳過——結果是重建漏掉它,重啟後同一個指紋被當成全新事件,
/// 產生重複的 incident 並且重新升級處置一次(這正是 verifier 抓到的
/// bug)。mtime 讀不到(理論上不會發生,例如檔案系統不支援 mtime)則保守地
/// 照樣解析整份 JSON,用真正的 `last_seen`(退回 `timestamp`)欄位篩一次
/// ——見下方 `age_basis` 那行,post-parse 的篩選同樣改用 `last_seen`,不是
/// `timestamp`,原因相同。
fn rebuild_state(
    incidents_dir: &str,
    project: &str,
    cfg: &IncidentsConfig,
    sink: &dyn Sink,
    now: DateTime<Utc>,
) -> IntakeState {
    let mut deduper = Deduper::new(cfg.clone());
    let mut active = HashMap::new();
    let mut fp_active = HashMap::new();

    let empty_state = |deduper| IntakeState {
        deduper,
        active: HashMap::new(),
        fp_active: HashMap::new(),
        dirty: HashSet::new(),
        last_flush: HashMap::new(),
        pending: HashSet::new(),
    };

    let dir_entries = match fs::read_dir(incidents_dir) {
        Ok(e) => e,
        // 目錄還不存在(第一次執行)或讀不到:視為沒有任何舊事件,不算錯誤。
        Err(_) => return empty_state(deduper),
    };

    let horizon = ChronoDuration::seconds(
        cfg.dedup_window_secs
            .max(cfg.cooldown_secs)
            .max(cfg.resolve_after_secs) as i64,
    );

    // fingerprint → 這個指紋底下所有(通過篩選的)incident,附帶「落地時
    // 狀態是不是 Escalating(重建前)」這個旗標,供之後判斷 last_escalated
    // 與是否需要把它改標成 interrupted 用。
    let mut by_fp: HashMap<String, Vec<(Incident, bool)>> = HashMap::new();

    for entry in dir_entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        // 見上方文件:用檔案 mtime(不是檔名前綴)判斷是否太舊、值不值得
        // 解析。mtime 讀不到就不 skip,照樣往下讀整份 JSON,用真正的
        // `last_seen` 再篩一次。
        if let Ok(meta) = fs::metadata(&path) {
            if let Ok(modified) = meta.modified() {
                let mtime: DateTime<Utc> = modified.into();
                if now - mtime > horizon {
                    continue;
                }
            }
        }

        let raw = match fs::read_to_string(&path) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[artemis] 重建事件狀態:無法讀取 {}:{e}", path.display());
                continue;
            }
        };
        let mut inc: Incident = match serde_json::from_str(&raw) {
            Ok(i) => i,
            Err(e) => {
                eprintln!("[artemis] 重建事件狀態:無法解析 {}:{e}", path.display());
                continue;
            }
        };

        if inc.project != project {
            continue;
        }
        // 跟前面的 mtime 篩選用同一個原則:一個 incident 是否還「活著」看
        // 的是它最近一次出現的時間(`last_seen`),不是它第一次被建立的
        // 時間(`timestamp`)——沒有 `last_seen`(理論上不會發生,`Incident
        // ::detected` 一律會填)才退回 `timestamp`。
        let age_basis = inc.last_seen.unwrap_or(inc.timestamp);
        if now - age_basis > horizon {
            continue;
        }

        if inc.fingerprint.is_none() {
            let (fp, template) = fingerprint::compute(&inc.source, &inc.message, &inc.frames);
            inc.fingerprint = Some(fp);
            inc.fingerprint_template = Some(template);
        }

        let was_escalating = inc.status == IncidentStatus::Escalating;
        if was_escalating {
            inc.status = IncidentStatus::Open;
            inc.status_reason = Some("interrupted".to_string());
        }

        let fp = inc.fingerprint.clone().unwrap_or_default();
        by_fp.entry(fp).or_default().push((inc, was_escalating));
    }

    let mut storm_seed = Vec::new();

    for (fp, mut group) in by_fp {
        // 中斷在升級處置途中的事件:重建時就地改標成 Open("interrupted")
        // 並落地(JSON + Markdown),但不重新送一次升級處置——見模組文件
        // 「Shutdown」一節,這些 incident 在關機前就已經以 Escalating 狀態
        // 落地過,不是遺失,只是中斷。
        for (inc, was_escalating) in &mut group {
            if *was_escalating {
                write_json(sink, inc);
                sink.render_markdown(inc);
            }
        }

        // 同一指紋只留「最新一筆」(依 last_seen,沒有的話退回 timestamp)
        // 當 Deduper 的代表 entry。
        let rep_idx = group
            .iter()
            .enumerate()
            .max_by_key(|(_, (inc, _))| inc.last_seen.unwrap_or(inc.timestamp))
            .map(|(i, _)| i)
            .expect("by_fp 的每個 group 都至少有一筆,不會是空的");
        let representative = group[rep_idx].0.clone();

        // last_escalated 的估算規則:這個指紋底下,凡是「曾經是 Escalating
        // (不論後來有沒有被中斷改標)、目前是 Mitigated、或帶有非空
        // `escalation` 欄位」的事件,都代表「這個時間點真的呼叫過一次
        // agent_service」,取這些事件裡最新的 last_seen/timestamp 當
        // last_escalated——這是 cooldown 判斷唯一關心的事;純粹只是累加次數
        // 的 Open/Suppressed 事件不計入。
        let last_escalated = group
            .iter()
            .filter(|(inc, was_escalating)| {
                *was_escalating
                    || inc.status == IncidentStatus::Mitigated
                    || inc.escalation.is_some()
            })
            .map(|(inc, _)| inc.last_seen.unwrap_or(inc.timestamp))
            .max();

        if let Some(t) = last_escalated {
            if now - t <= ChronoDuration::hours(1) {
                storm_seed.push(t);
            }
        }

        deduper.restore(
            fp.clone(),
            Entry {
                id: representative.id.clone(),
                status: representative.status,
                first_seen: representative.first_seen.unwrap_or(representative.timestamp),
                last_seen: representative.last_seen.unwrap_or(representative.timestamp),
                last_escalated,
            },
        );

        // 只有非 Resolved 的代表事件需要放進 `active`,讓之後的 `Append`
        // 找得到它更新次數;Resolved 的只留在 Deduper entries 裡供
        // recurrence 判斷用。
        if representative.status != IncidentStatus::Resolved {
            fp_active.insert(fp, representative.id.clone());
            active.insert(representative.id.clone(), representative);
        }
    }

    if !storm_seed.is_empty() {
        deduper.seed_storm(storm_seed, now);
    }

    IntakeState {
        deduper,
        active,
        fp_active,
        dirty: HashSet::new(),
        last_flush: HashMap::new(),
        pending: HashSet::new(),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_intake(
    detected_rx: Receiver<Incident>,
    done_rx: Receiver<IntakeEvent>,
    job_tx: Sender<EscalationJob>,
    overflow: Arc<Mutex<HashMap<String, u64>>>,
    incidents_cfg: IncidentsConfig,
    escalation_enabled: bool,
    sink: Arc<dyn Sink>,
    clock: Clock,
    incidents_dir: String,
    project: String,
) {
    // Milestone 1 step 5:在處理第一筆 Detected 事件之前,先把
    // `incidents_dir` 底下既有的事件狀態重建回來(見 `rebuild_state` 文件)。
    let mut state = rebuild_state(&incidents_dir, &project, &incidents_cfg, sink.as_ref(), clock());
    let rewrite_interval = ChronoDuration::seconds(incidents_cfg.rewrite_interval_secs as i64);
    // Tick 的排程刻意用真實的 `Instant`(wall clock),跟 `clock` 注入的
    // 「業務時間」脫鉤:`clock` 是給去重/storm guard/節流重寫這些「事件發生
    // 在哪個時間點」的判斷用的,測試常會把它凍結在固定時間點以取得決定性
    // 結果 —— 如果 Tick 的排程也綁著它,凍結的測試就永遠等不到一次 Tick。
    // 落地時使用的時間戳仍然是 `clock()`,只有「多久該跑一次 Tick」這件事
    // 才用真實經過的時間。
    let tick_interval = StdDuration::from_millis(1000);
    let poll = StdDuration::from_millis(100);
    let mut last_tick = Instant::now();

    loop {
        let mut shutdown_requested = false;
        match detected_rx.recv_timeout(poll) {
            Ok(inc) => {
                let now = clock();
                handle_detected(
                    &mut state,
                    sink.as_ref(),
                    &job_tx,
                    &incidents_cfg,
                    escalation_enabled,
                    now,
                    inc,
                );
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => shutdown_requested = true,
        }

        loop {
            match done_rx.try_recv() {
                Ok(IntakeEvent::EscalationDone { id, fp, result }) => {
                    handle_escalation_done(&mut state, sink.as_ref(), id, fp, result);
                }
                Ok(IntakeEvent::Shutdown) => {
                    shutdown_requested = true;
                    break;
                }
                Err(_) => break,
            }
        }

        if !shutdown_requested && last_tick.elapsed() >= tick_interval {
            let now = clock();
            handle_tick(&mut state, sink.as_ref(), &overflow, rewrite_interval, now);
            last_tick = Instant::now();
        }

        if shutdown_requested {
            break;
        }
    }

    run_shutdown_sequence(
        &mut state,
        sink.as_ref(),
        job_tx,
        &done_rx,
        &detected_rx,
        &overflow,
        &incidents_cfg,
        escalation_enabled,
        &clock,
    );
}

/// 不論是被顯式的 `IntakeEvent::Shutdown` 觸發、還是所有 `Recorder` clone
/// 都被 drop(`detected_tx` 斷線)觸發,收尾流程完全一樣 —— 見模組文件的
/// 「Shutdown」一節。
#[allow(clippy::too_many_arguments)]
fn run_shutdown_sequence(
    state: &mut IntakeState,
    sink: &dyn Sink,
    job_tx: Sender<EscalationJob>,
    done_rx: &Receiver<IntakeEvent>,
    detected_rx: &Receiver<Incident>,
    overflow: &Arc<Mutex<HashMap<String, u64>>>,
    incidents_cfg: &IncidentsConfig,
    escalation_enabled: bool,
    clock: &Clock,
) {
    // 1) 把 channel 裡剩下尚未處理的 Detected 事件瀝乾,不遺漏最後一批。
    while let Ok(inc) = detected_rx.try_recv() {
        let now = clock();
        handle_detected(
            state,
            sink,
            &job_tx,
            incidents_cfg,
            escalation_enabled,
            now,
            inc,
        );
    }

    // 2) 強制 flush:傳 `Duration::zero()` 蓋掉 rewrite_interval 的節流,
    // 把 overflow 次數攤平、所有 dirty 的 incident 全部落地 —— 不等下一次
    // 自然 Tick(這正是這次修的 bug:process 可能在下一次 Tick 之前就被
    // 整個結束掉)。
    let now = clock();
    handle_tick(state, sink, overflow, ChronoDuration::zero(), now);

    // 3) drop 掉這個唯一的 job_tx,讓 worker 執行緒在處理完手上的 job、且
    // 沒有新 job 可收之後,`job_rx.recv()` 回傳 Err 自然結束。
    drop(job_tx);

    // 4) 最多再等 shutdown_grace_secs 秒,讓在途的升級處置回報結果並落地
    // (每收到一筆 `EscalationDone` 就照常寫入,不用等全部到齊)。
    let deadline = Instant::now() + StdDuration::from_secs(incidents_cfg.shutdown_grace_secs);
    while !state.pending.is_empty() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match done_rx.recv_timeout(remaining.min(StdDuration::from_millis(200))) {
            Ok(IntakeEvent::EscalationDone { id, fp, result }) => {
                handle_escalation_done(state, sink, id, fp, result);
            }
            // 收尾流程本身已經在處理 Shutdown 了,重複的訊息(例如呼叫端
            // 不小心呼叫了兩次 request_shutdown)直接忽略。
            Ok(IntakeEvent::Shutdown) => {}
            Err(_) => {
                if Instant::now() >= deadline {
                    break;
                }
            }
        }
    }

    // 5) 收尾前最後再強制 flush 一次,確保等待期間如果有任何
    // `EscalationDone` 觸發的次數異動(目前 handle_escalation_done 本來就
    // 會自己落地,這裡是保險,cost 幾乎為零)都確實寫到磁碟上,不留在
    // 記憶體裡隨行程結束消失。
    let now = clock();
    handle_tick(state, sink, overflow, ChronoDuration::zero(), now);
}

fn run_escalation_worker(
    job_rx: Receiver<EscalationJob>,
    escalator: Arc<dyn Escalator>,
    done_tx: Sender<IntakeEvent>,
) {
    while let Ok(job) = job_rx.recv() {
        let result = escalator.escalate(&job.incident).map_err(|e| e.to_string());
        if done_tx
            .send(IntakeEvent::EscalationDone {
                id: job.id,
                fp: job.fp,
                result,
            })
            .is_err()
        {
            // intake 已經不在了(理論上不會發生,intake 只有在瀝乾 job_tx
            // 之後才會結束,而那之後 worker 收不到新 job 就會退出迴圈);
            // 保守起見還是跳出,不留下孤兒執行緒。
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::incident::{Severity, Source};
    use std::sync::atomic::AtomicI64 as StdAtomicI64;
    use std::sync::Barrier;

    #[derive(Clone)]
    struct FakeClock {
        ms: Arc<StdAtomicI64>,
    }

    impl FakeClock {
        fn new(start_ms: i64) -> Self {
            Self {
                ms: Arc::new(StdAtomicI64::new(start_ms)),
            }
        }

        fn advance(&self, delta_ms: i64) {
            self.ms.fetch_add(delta_ms, Ordering::SeqCst);
        }

        fn as_clock(&self) -> Clock {
            let ms = self.ms.clone();
            Arc::new(move || {
                DateTime::<Utc>::from_timestamp_millis(ms.load(Ordering::SeqCst)).unwrap()
            })
        }
    }

    #[derive(Default)]
    struct SinkCalls {
        write_json: Vec<Incident>,
        render_markdown: usize,
        push_central: usize,
        snapshot_diagnostics: usize,
    }

    struct FakeSink {
        calls: Mutex<SinkCalls>,
    }

    impl FakeSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(SinkCalls::default()),
            })
        }

        /// 某個 id 最後一次被寫入 JSON 時的內容(依 write_json 呼叫順序)。
        fn last_write(&self, id: &str) -> Option<Incident> {
            self.calls
                .lock()
                .unwrap()
                .write_json
                .iter()
                .rev()
                .find(|i| i.id == id)
                .cloned()
        }

        fn write_json_count(&self) -> usize {
            self.calls.lock().unwrap().write_json.len()
        }
    }

    impl Sink for FakeSink {
        fn write_json(&self, inc: &Incident) -> Result<()> {
            self.calls.lock().unwrap().write_json.push(inc.clone());
            Ok(())
        }
        fn render_markdown(&self, _inc: &Incident) {
            self.calls.lock().unwrap().render_markdown += 1;
        }
        fn push_central(&self, _inc: &Incident) {
            self.calls.lock().unwrap().push_central += 1;
        }
        fn snapshot_diagnostics(&self, _inc: &mut Incident) {
            self.calls.lock().unwrap().snapshot_diagnostics += 1;
        }
    }

    /// 立刻回傳固定結果的假 escalator。
    struct ImmediateEscalator {
        result: Mutex<Box<dyn Fn() -> Result<EscalationReport, String> + Send>>,
        calls: AtomicI64,
    }

    impl ImmediateEscalator {
        fn ok(final_resolved: bool) -> Arc<Self> {
            Arc::new(Self {
                result: Mutex::new(Box::new(move || {
                    Ok(EscalationReport {
                        final_resolved,
                        ..Default::default()
                    })
                })),
                calls: AtomicI64::new(0),
            })
        }

        fn err() -> Arc<Self> {
            Arc::new(Self {
                result: Mutex::new(Box::new(|| Err("mock 呼叫失敗".to_string()))),
                calls: AtomicI64::new(0),
            })
        }

        fn call_count(&self) -> i64 {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl Escalator for ImmediateEscalator {
        fn escalate(&self, _inc: &Incident) -> Result<EscalationReport> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            (self.result.lock().unwrap())().map_err(anyhow::Error::msg)
        }
    }

    /// 卡住直到測試釋放 barrier 才回傳的假 escalator,用來模擬「escalation
    /// 很慢」或「同時只能有一個在途」的情境。
    struct BlockingEscalator {
        barrier: Arc<Barrier>,
        calls: AtomicI64,
    }

    impl BlockingEscalator {
        fn new(barrier: Arc<Barrier>) -> Arc<Self> {
            Arc::new(Self {
                barrier,
                calls: AtomicI64::new(0),
            })
        }
    }

    impl Escalator for BlockingEscalator {
        fn escalate(&self, _inc: &Incident) -> Result<EscalationReport> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.barrier.wait();
            Ok(EscalationReport::default())
        }
    }

    fn incidents_cfg(max_queue: usize, max_pending: usize) -> IncidentsConfig {
        IncidentsConfig {
            dedup_window_secs: 300,
            cooldown_secs: 1800,
            resolve_after_secs: 3600,
            max_escalations_per_hour: 0,
            max_queue,
            max_pending_escalations: max_pending,
            rewrite_interval_secs: 10,
            shutdown_grace_secs: 2,
        }
    }

    fn make_incident(message: &str) -> Incident {
        Incident::detected(
            "demo".to_string(),
            Source::Process,
            message.to_string(),
            vec![],
            message.to_string(),
            "node app.js".to_string(),
            Some(1),
            false,
            0,
            Severity::High,
        )
    }

    /// 大部分測試不測啟動重建,不需要真的有事件目錄 —— 給一個保證不存在的
    /// 路徑,`rebuild_state` 讀不到目錄時視為「沒有舊事件」直接回傳空狀態
    /// (見 `rebuild_state` 對 `fs::read_dir` 失敗的處理),行為等同重建前。
    fn test_incidents_dir() -> String {
        format!(
            "/tmp/artemis-recorder-test-{}-does-not-exist",
            uuid::Uuid::new_v4()
        )
    }

    /// 輪詢直到條件成立或逾時 —— intake 是背景執行緒,斷言前需要等它處理完。
    fn wait_until<F: Fn() -> bool>(cond: F, timeout: StdDuration) {
        let deadline = Instant::now() + timeout;
        while !cond() {
            assert!(Instant::now() < deadline, "等待逾時");
            thread::sleep(StdDuration::from_millis(10));
        }
    }

    #[test]
    fn ten_identical_submissions_dedup_to_one_incident_one_escalation() {
        let sink = FakeSink::new();
        let escalator = ImmediateEscalator::ok(false);
        let clock = FakeClock::new(1_700_000_000_000);
        let (recorder, _handle) = Recorder::with_deps(
            incidents_cfg(1024, 8),
            true,
            sink.clone(),
            escalator.clone(),
            clock.as_clock(),
            test_incidents_dir(),
            "demo".to_string(),
        );

        let mut first_id = None;
        for _ in 0..10 {
            let inc = make_incident("boom: connection refused");
            if first_id.is_none() {
                first_id = Some(inc.id.clone());
            }
            recorder.submit(inc);
        }

        let id = first_id.unwrap();
        wait_until(
            || {
                sink.last_write(&id)
                    .map(|i| i.occurrence_count == 10)
                    .unwrap_or(false)
            },
            StdDuration::from_secs(3),
        );

        assert_eq!(escalator.call_count(), 1, "10 次相同事件只應該升級處置一次");
        let final_inc = sink.last_write(&id).unwrap();
        assert_eq!(final_inc.occurrence_count, 10);
        // 不應該每次都整份重寫:1(建立)+ 1(EscalationDone)+ 少量節流重寫。
        assert!(
            sink.write_json_count() <= 5,
            "次數應該被節流,不是每次 submit 都整份重寫,實際:{}",
            sink.write_json_count()
        );
    }

    #[test]
    fn submit_never_blocks_even_when_escalator_is_stuck() {
        let sink = FakeSink::new();
        let barrier = Arc::new(Barrier::new(2));
        let escalator = BlockingEscalator::new(barrier.clone());
        let clock = FakeClock::new(1_700_000_000_000);
        let (recorder, _handle) =
            Recorder::with_deps(incidents_cfg(1024, 100), true, sink, escalator, clock.as_clock(), test_incidents_dir(), "demo".to_string());

        let start = Instant::now();
        for i in 0..100 {
            recorder.submit(make_incident(&format!("distinct error #{i}")));
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < StdDuration::from_millis(200),
            "submit 不應該被 escalator 卡住,實際耗時:{elapsed:?}"
        );

        // 釋放第一個卡住的 escalate() 呼叫,讓背景執行緒可以正常結束測試。
        barrier.wait();
    }

    #[test]
    fn queue_overflow_counts_are_preserved_after_unblock() {
        let sink = FakeSink::new();
        // FakeSink.write_json 本身不會卡,改用會卡住的 escalator 讓 intake
        // 在處理第一筆事件(NewEscalate → 送 job)之後,job 送出是非阻塞的
        // unbounded channel,intake 很快就會回到迴圈頂端 —— 為了確實讓
        // detected channel 塞滿,這裡改成用一個會在 render_markdown 卡住的
        // Sink 包裝來檔住 intake 本身。
        struct BlockingSink {
            inner: Arc<FakeSink>,
            barrier: Arc<Barrier>,
            blocked_once: AtomicI64,
        }
        impl Sink for BlockingSink {
            fn write_json(&self, inc: &Incident) -> Result<()> {
                self.inner.write_json(inc)
            }
            fn render_markdown(&self, inc: &Incident) {
                if self.blocked_once.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.barrier.wait();
                }
                self.inner.render_markdown(inc);
            }
            fn push_central(&self, inc: &Incident) {
                self.inner.push_central(inc);
            }
            fn snapshot_diagnostics(&self, inc: &mut Incident) {
                self.inner.snapshot_diagnostics(inc);
            }
        }

        let barrier = Arc::new(Barrier::new(2));
        let blocking_sink = Arc::new(BlockingSink {
            inner: sink.clone(),
            barrier: barrier.clone(),
            blocked_once: AtomicI64::new(0),
        });
        let escalator = ImmediateEscalator::ok(false);
        let clock = FakeClock::new(1_700_000_000_000);
        let (recorder, _handle) = Recorder::with_deps(
            incidents_cfg(1, 8),
            true,
            blocking_sink,
            escalator,
            clock.as_clock(),
            test_incidents_dir(),
            "demo".to_string(),
        );

        let fp_message = "overflow test error";
        let first = make_incident(fp_message);
        let first_id = first.id.clone();
        recorder.submit(first); // intake 立刻撿走這筆,卡在 render_markdown。

        // 給 intake 一點時間真的進到卡住狀態,再繼續灌爆佇列(容量 1)。
        thread::sleep(StdDuration::from_millis(50));
        let total_extra = 20;
        for _ in 0..total_extra {
            recorder.submit(make_incident(fp_message));
        }

        barrier.wait(); // 解除卡住,intake 繼續處理。

        wait_until(
            || {
                sink.last_write(&first_id)
                    .map(|i| i.occurrence_count == 1 + total_extra as u64)
                    .unwrap_or(false)
            },
            StdDuration::from_secs(3),
        );
    }

    #[test]
    fn second_distinct_fingerprint_suppressed_when_pending_escalations_full() {
        let sink = FakeSink::new();
        let barrier = Arc::new(Barrier::new(2));
        let escalator = BlockingEscalator::new(barrier.clone());
        let clock = FakeClock::new(1_700_000_000_000);
        let (recorder, _handle) = Recorder::with_deps(
            incidents_cfg(1024, 1),
            true,
            sink.clone(),
            escalator,
            clock.as_clock(),
            test_incidents_dir(),
            "demo".to_string(),
        );

        let first = make_incident("fingerprint one boom");
        recorder.submit(first);
        // 等 intake 真的把第一筆送進正在卡住的 escalator(pending 已滿)。
        thread::sleep(StdDuration::from_millis(100));

        let second = make_incident("fingerprint two crash");
        let second_id = second.id.clone();
        recorder.submit(second);

        wait_until(
            || {
                sink.last_write(&second_id)
                    .map(|i| i.status_reason.as_deref() == Some("queue_full"))
                    .unwrap_or(false)
            },
            StdDuration::from_secs(3),
        );
        let second_final = sink.last_write(&second_id).unwrap();
        assert_eq!(second_final.status, IncidentStatus::Suppressed);

        barrier.wait();
    }

    #[test]
    fn escalation_done_ok_resolved_marks_mitigated() {
        let sink = FakeSink::new();
        let escalator = ImmediateEscalator::ok(true);
        let clock = FakeClock::new(1_700_000_000_000);
        let (recorder, _handle) = Recorder::with_deps(
            incidents_cfg(1024, 8),
            true,
            sink.clone(),
            escalator,
            clock.as_clock(),
            test_incidents_dir(),
            "demo".to_string(),
        );

        let inc = make_incident("resolved-by-escalation");
        let id = inc.id.clone();
        recorder.submit(inc);

        wait_until(
            || {
                sink.last_write(&id)
                    .map(|i| i.status == IncidentStatus::Mitigated)
                    .unwrap_or(false)
            },
            StdDuration::from_secs(3),
        );
    }

    #[test]
    fn escalation_done_err_marks_open_with_reason() {
        let sink = FakeSink::new();
        let escalator = ImmediateEscalator::err();
        let clock = FakeClock::new(1_700_000_000_000);
        let (recorder, _handle) = Recorder::with_deps(
            incidents_cfg(1024, 8),
            true,
            sink.clone(),
            escalator,
            clock.as_clock(),
            test_incidents_dir(),
            "demo".to_string(),
        );

        let inc = make_incident("escalation-call-fails");
        let id = inc.id.clone();
        recorder.submit(inc);

        wait_until(
            || {
                sink.last_write(&id)
                    .map(|i| {
                        i.status == IncidentStatus::Open
                            && i.status_reason.as_deref() == Some("escalation_failed")
                    })
                    .unwrap_or(false)
            },
            StdDuration::from_secs(3),
        );
    }

    #[test]
    fn dropping_all_senders_lets_intake_thread_exit() {
        let sink = FakeSink::new();
        let escalator = ImmediateEscalator::ok(false);
        let clock = FakeClock::new(1_700_000_000_000);
        let (recorder, handle) = Recorder::with_deps(
            incidents_cfg(1024, 8),
            true,
            sink,
            escalator,
            clock.as_clock(),
            test_incidents_dir(),
            "demo".to_string(),
        );

        drop(recorder);

        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let _ = handle.join();
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(StdDuration::from_secs(5))
            .expect("intake 執行緒應該在所有 sender 掉光後結束");
    }

    #[test]
    fn shutdown_and_join_flushes_final_count_even_with_a_live_extra_clone() {
        // rewrite_interval 故意設很大,確保「一路都不會自然節流重寫觸發」,
        // 這樣如果最後一筆寫入是 10 才算 shutdown 真的有強制 flush,而不是
        // 剛好被某次自然的節流重寫蓋過去。
        let mut cfg = incidents_cfg(1024, 8);
        cfg.rewrite_interval_secs = 3600;
        let sink = FakeSink::new();
        let escalator = ImmediateEscalator::ok(false);
        let clock = FakeClock::new(1_700_000_000_000);
        let (recorder, handle) =
            Recorder::with_deps(cfg, true, sink.clone(), escalator, clock.as_clock(), test_incidents_dir(), "demo".to_string());

        // 這份 clone 模擬 watcher/resource 監看執行緒手上還留著的 Recorder
        // ——這正是這次修的 bug 的關鍵情境:即使還有其他 clone 活著、
        // `detected_tx` 沒有斷線,顯式的 `shutdown_and_join` 也必須照樣觸發
        // 收尾,不能傻等 refcount 歸零。
        let _still_held_clone = recorder.clone();

        let mut first_id = None;
        for _ in 0..10 {
            let inc = make_incident("shutdown flush test");
            if first_id.is_none() {
                first_id = Some(inc.id.clone());
            }
            recorder.submit(inc);
        }
        let id = first_id.unwrap();

        // 等第一筆事件真的被 intake 處理過(至少寫入一次),再觸發關閉,
        // 避免測試本身跟 intake 的處理順序賽跑。
        wait_until(|| sink.last_write(&id).is_some(), StdDuration::from_secs(3));

        recorder.shutdown_and_join(handle);

        let final_inc = sink
            .last_write(&id)
            .expect("shutdown 收尾流程應該至少強制 flush 一次");
        assert_eq!(
            final_inc.occurrence_count, 10,
            "shutdown 應該強制把 overflow 攤平、dirty 的 incident 全部落地,\
             不能等下一次自然 Tick(這正是原本回報的 bug)"
        );
    }

    #[test]
    fn shutdown_and_join_respects_grace_timeout_with_a_stuck_escalator() {
        let mut cfg = incidents_cfg(1024, 8);
        cfg.shutdown_grace_secs = 1;
        let sink = FakeSink::new();
        // 永遠不會被釋放的 barrier:escalate() 會卡住直到行程結束,模擬
        // 「升級處置卡住、grace period 內回不來」的情境。
        let barrier = Arc::new(Barrier::new(2));
        let escalator = BlockingEscalator::new(barrier);
        let clock = FakeClock::new(1_700_000_000_000);
        let (recorder, handle) =
            Recorder::with_deps(cfg, true, sink.clone(), escalator, clock.as_clock(), test_incidents_dir(), "demo".to_string());

        let inc = make_incident("stuck escalation on shutdown");
        recorder.submit(inc);
        // 等 intake 真的把這筆送進卡住的 escalator(pending 已滿),
        // 確保 shutdown 觸發時 state.pending 非空,grace period 的等待
        // 迴圈才有東西可等。
        thread::sleep(StdDuration::from_millis(100));

        let grace = StdDuration::from_secs(1);
        let margin = StdDuration::from_millis(800);
        let start = Instant::now();
        recorder.shutdown_and_join(handle);
        let elapsed = start.elapsed();

        assert!(
            elapsed >= grace,
            "shutdown 不應該無視卡住的升級處置立刻回傳,實際耗時:{elapsed:?}"
        );
        assert!(
            elapsed < grace + margin,
            "grace period 到期後 shutdown 應該盡快回傳,不能無限期卡住,實際耗時:{elapsed:?}"
        );
    }

    // ------------------------------------------------------------------
    // Milestone 1 step 5:啟動重建(rebuild_state)+ Resolve tick 測試。
    // ------------------------------------------------------------------

    /// 測試用的真實暫存目錄,`Drop` 時自動清掉,避免這些測試在磁碟上留下垃圾。
    struct TempIncidentsDir {
        path: std::path::PathBuf,
    }

    impl TempIncidentsDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("artemis-rebuild-test-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(&path).expect("建立測試用暫存目錄失敗");
            Self { path }
        }

        fn path_str(&self) -> String {
            self.path.to_string_lossy().to_string()
        }

        /// 直接把一筆手工建構的 incident 寫成 JSON 檔,檔名依它的 `id` 命名
        /// ——`rebuild_state` 靠檔名前綴判斷年紀,呼叫端必須確保 `inc.id` 的
        /// 時間戳前綴跟 `inc.timestamp` 一致。
        /// 寫入 JSON,並把檔案 mtime 硬設成 `inc.last_seen`(退回
        /// `timestamp`)——`rebuild_state` 現在是用 mtime(不是檔名前綴)
        /// 判斷檔案年紀,單靠 `fs::write` 產生的「真正現在」mtime 沒辦法
        /// 模擬出「很久以前建立、但最近才又出現過」這種情境,測試必須自己
        /// 把 mtime 校正回這個 incident 應該有的年紀。
        fn write(&self, inc: &Incident) {
            let path = self.path.join(format!("{}.json", inc.id));
            fs::write(&path, serde_json::to_string_pretty(inc).unwrap()).expect("寫入測試 incident JSON 失敗");
            let mtime_basis = inc.last_seen.unwrap_or(inc.timestamp);
            let file = fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("重新開啟測試 incident JSON 失敗");
            file.set_modified(mtime_basis.into())
                .expect("設定測試 incident JSON mtime 失敗");
        }
    }

    impl Drop for TempIncidentsDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    /// 手工建構一筆可控制時間戳/狀態/指紋的 incident,供重建測試直接寫進
    /// 暫存目錄。`id` 的時間戳前綴會依 `ts` 產生,確保檔名年紀判斷跟
    /// `timestamp`/`last_seen` 一致。
    #[allow(clippy::too_many_arguments)]
    fn rebuild_test_incident(
        ts: DateTime<Utc>,
        last_seen: DateTime<Utc>,
        message: &str,
        fingerprint: Option<String>,
        status: IncidentStatus,
        escalation: Option<EscalationReport>,
    ) -> Incident {
        let id = format!(
            "{}-{}",
            ts.format("%Y%m%dT%H%M%S"),
            &uuid::Uuid::new_v4().to_string()[..8]
        );
        let fingerprint_template = fingerprint
            .as_ref()
            .map(|_| fingerprint::normalize_template(message));
        Incident {
            id,
            timestamp: ts,
            project: "demo".to_string(),
            source: Source::Process,
            message: message.to_string(),
            frames: vec![],
            raw: message.to_string(),
            command: "node app.js".to_string(),
            exit_code: Some(1),
            restarted: false,
            restart_count: 0,
            escalation,
            diagnostics_history: vec![],
            fingerprint,
            fingerprint_template,
            occurrence_count: 1,
            first_seen: Some(ts),
            last_seen: Some(last_seen),
            status,
            status_reason: None,
            recurrence_of: None,
            severity: Severity::High,
        }
    }

    #[test]
    fn rebuild_recomputes_missing_fingerprint_and_appends_new_submission() {
        let dir = TempIncidentsDir::new();
        let clock = FakeClock::new(1_700_000_000_000);
        let now = DateTime::<Utc>::from_timestamp_millis(1_700_000_000_000).unwrap();

        // 舊格式 JSON:沒有 fingerprint 欄位(None),status Open,最近才發生
        // 過(在 dedup window 內)。
        let old = rebuild_test_incident(
            now - ChronoDuration::seconds(10),
            now - ChronoDuration::seconds(10),
            "boom: connection refused",
            None,
            IncidentStatus::Open,
            None,
        );
        let old_id = old.id.clone();
        dir.write(&old);

        let sink = FakeSink::new();
        let escalator = ImmediateEscalator::ok(false);
        let (recorder, _handle) = Recorder::with_deps(
            incidents_cfg(1024, 8),
            true,
            sink.clone(),
            escalator.clone(),
            clock.as_clock(),
            dir.path_str(),
            "demo".to_string(),
        );

        // 同一則訊息再次出現,應該 Append 到重建回來的既有 incident 上,
        // 而不是建立一筆新的、也不應該重新升級處置。
        recorder.submit(make_incident("boom: connection refused"));

        wait_until(
            || {
                sink.last_write(&old_id)
                    .map(|i| i.occurrence_count == 2)
                    .unwrap_or(false)
            },
            StdDuration::from_secs(3),
        );
        assert_eq!(
            escalator.call_count(),
            0,
            "Append 到既有 incident 不應該觸發升級處置"
        );
    }

    #[test]
    fn rebuild_marks_interrupted_escalating_as_open_without_reescalating() {
        let dir = TempIncidentsDir::new();
        let clock = FakeClock::new(1_700_000_000_000);
        let now = DateTime::<Utc>::from_timestamp_millis(1_700_000_000_000).unwrap();

        let stuck = rebuild_test_incident(
            now - ChronoDuration::seconds(5),
            now - ChronoDuration::seconds(5),
            "interrupted mid-escalation",
            Some("deadbeefdeadbeef".to_string()),
            IncidentStatus::Escalating,
            None,
        );
        let stuck_id = stuck.id.clone();
        dir.write(&stuck);

        let sink = FakeSink::new();
        let escalator = ImmediateEscalator::ok(false);
        let (_recorder, _handle) = Recorder::with_deps(
            incidents_cfg(1024, 8),
            true,
            sink.clone(),
            escalator.clone(),
            clock.as_clock(),
            dir.path_str(),
            "demo".to_string(),
        );

        wait_until(
            || {
                sink.last_write(&stuck_id)
                    .map(|i| {
                        i.status == IncidentStatus::Open
                            && i.status_reason.as_deref() == Some("interrupted")
                    })
                    .unwrap_or(false)
            },
            StdDuration::from_secs(3),
        );
        assert_eq!(
            escalator.call_count(),
            0,
            "重建中斷的 Escalating 事件不應該重新送出升級處置"
        );
    }

    #[test]
    fn rebuild_recurrence_right_after_restart_is_suppressed_by_cooldown() {
        let dir = TempIncidentsDir::new();
        let clock = FakeClock::new(1_700_000_000_000);
        let now = DateTime::<Utc>::from_timestamp_millis(1_700_000_000_000).unwrap();

        // 400 秒前已經 Mitigated 過一次(帶 escalation 結果)——距離現在超過
        // dedup_window(300s,見 incidents_cfg),但還沒超過 cooldown(1800s)。
        let mitigated = rebuild_test_incident(
            now - ChronoDuration::seconds(400),
            now - ChronoDuration::seconds(400),
            "mitigated then recurs",
            Some("cafebabecafebabe".to_string()),
            IncidentStatus::Mitigated,
            Some(EscalationReport {
                final_resolved: true,
                ..Default::default()
            }),
        );
        dir.write(&mitigated);

        let sink = FakeSink::new();
        let escalator = ImmediateEscalator::ok(false);
        let (recorder, _handle) = Recorder::with_deps(
            incidents_cfg(1024, 8),
            true,
            sink.clone(),
            escalator.clone(),
            clock.as_clock(),
            dir.path_str(),
            "demo".to_string(),
        );

        let mut recurrence = make_incident("mitigated then recurs");
        recurrence.fingerprint = Some("cafebabecafebabe".to_string());
        let recurrence_id = recurrence.id.clone();
        recorder.submit(recurrence);

        wait_until(
            || {
                sink.last_write(&recurrence_id)
                    .map(|i| i.status_reason.as_deref() == Some("cooldown"))
                    .unwrap_or(false)
            },
            StdDuration::from_secs(3),
        );
        let final_inc = sink.last_write(&recurrence_id).unwrap();
        assert_eq!(final_inc.status, IncidentStatus::Suppressed);
        assert_eq!(
            escalator.call_count(),
            0,
            "cooldown 內的 recurrence 不應該重新升級處置"
        );
    }

    #[test]
    fn rebuild_skips_files_older_than_horizon() {
        let dir = TempIncidentsDir::new();
        let clock = FakeClock::new(1_700_000_000_000);
        let now = DateTime::<Utc>::from_timestamp_millis(1_700_000_000_000).unwrap();

        // horizon = max(dedup_window=300, cooldown=1800, resolve_after=3600)
        // = 3600s;這筆事件比 horizon 還舊很多,重建時應該直接跳過(不讀取
        // 內容、不佔用 fp_active)。
        let ancient = rebuild_test_incident(
            now - ChronoDuration::seconds(10_000),
            now - ChronoDuration::seconds(10_000),
            "ancient error, long resolved",
            Some("0123456789abcdef".to_string()),
            IncidentStatus::Mitigated,
            Some(EscalationReport {
                final_resolved: true,
                ..Default::default()
            }),
        );
        dir.write(&ancient);

        let sink = FakeSink::new();
        let escalator = ImmediateEscalator::ok(false);
        let (recorder, _handle) = Recorder::with_deps(
            incidents_cfg(1024, 8),
            true,
            sink.clone(),
            escalator.clone(),
            clock.as_clock(),
            dir.path_str(),
            "demo".to_string(),
        );

        let mut fresh = make_incident("ancient error, long resolved");
        fresh.fingerprint = Some("0123456789abcdef".to_string());
        let fresh_id = fresh.id.clone();
        recorder.submit(fresh);

        // 舊指紋被跳過,不影響去重狀態 → 這次出現被當成全新事件,直接升級
        // (escalator 回傳 final_resolved=false,所以最終狀態是 Open,重點是
        // 確實跑過一次 escalation,而不是被 Append 到那筆被跳過的舊事件)。
        wait_until(
            || {
                sink.last_write(&fresh_id)
                    .map(|i| i.escalation.is_some())
                    .unwrap_or(false)
            },
            StdDuration::from_secs(3),
        );
        assert_eq!(
            escalator.call_count(),
            1,
            "太舊被跳過的事件不該影響去重,這次應該被當成全新事件升級處置"
        );
    }

    /// 修 verifier 抓到的 HIGH bug:一個很久以前建立、但最近才又出現過的
    /// incident,不應該因為「建立時間」比 horizon 還舊就被重建跳過——
    /// 用來判斷年紀的必須是它「還活著」的證據(mtime ≈ last_seen),不是
    /// 出生證明(檔名前綴/timestamp)。
    #[test]
    fn rebuild_uses_last_seen_not_creation_time_for_horizon_check() {
        let dir = TempIncidentsDir::new();
        let clock = FakeClock::new(1_700_000_000_000);
        let now = DateTime::<Utc>::from_timestamp_millis(1_700_000_000_000).unwrap();

        // horizon = max(300, 1800, 3600) = 3600s(1 小時)。這筆事件建立於
        // 2 小時前(超過 horizon),但 1 分鐘前才又出現過一次(last_seen 很
        // 新)——不應該被當成太舊跳過。
        let alive = rebuild_test_incident(
            now - ChronoDuration::hours(2),
            now - ChronoDuration::minutes(1),
            "long-lived recurring error",
            Some("abad1deaabad1dea".to_string()),
            IncidentStatus::Open,
            None,
        );
        let alive_id = alive.id.clone();
        dir.write(&alive);

        let sink = FakeSink::new();
        let escalator = ImmediateEscalator::ok(false);
        let (recorder, _handle) = Recorder::with_deps(
            incidents_cfg(1024, 8),
            true,
            sink.clone(),
            escalator.clone(),
            clock.as_clock(),
            dir.path_str(),
            "demo".to_string(),
        );

        let mut recurrence = make_incident("long-lived recurring error");
        recurrence.fingerprint = Some("abad1deaabad1dea".to_string());
        recorder.submit(recurrence);

        // 應該被 Append 到重建回來的既有 incident 上(occurrence_count 從 1
        // 變成 2),而不是被跳過、建立成一筆新的 incident。
        wait_until(
            || {
                sink.last_write(&alive_id)
                    .map(|i| i.occurrence_count == 2)
                    .unwrap_or(false)
            },
            StdDuration::from_secs(3),
        );
        assert_eq!(
            escalator.call_count(),
            0,
            "Append 到既有(未過期)incident 不應該重新升級處置"
        );
        // 也確認真的沒有另外建立一筆新 incident(fingerprint 相同的兩筆
        // 檔案)。
        let distinct_ids: HashSet<String> = sink
            .calls
            .lock()
            .unwrap()
            .write_json
            .iter()
            .map(|i| i.id.clone())
            .collect();
        assert_eq!(
            distinct_ids,
            [alive_id].into_iter().collect::<HashSet<_>>(),
            "不應該因為誤判太舊而建立出第二筆重複的 incident"
        );
    }

    #[test]
    fn resolve_tick_marks_idle_incident_resolved_then_recurrence_creates_new_incident() {
        let mut cfg = incidents_cfg(1024, 8);
        cfg.resolve_after_secs = 5;
        cfg.dedup_window_secs = 1;
        cfg.rewrite_interval_secs = 3600; // 不靠自然節流重寫,只看 tick 邏輯本身。
        let sink = FakeSink::new();
        let escalator = ImmediateEscalator::ok(false);
        let clock = FakeClock::new(1_700_000_000_000);
        let (recorder, _handle) = Recorder::with_deps(
            cfg,
            true,
            sink.clone(),
            escalator.clone(),
            clock.as_clock(),
            test_incidents_dir(),
            "demo".to_string(),
        );

        let inc = make_incident("idles then resolves");
        let id = inc.id.clone();
        recorder.submit(inc);
        wait_until(|| sink.last_write(&id).is_some(), StdDuration::from_secs(3));

        // 把時鐘撥到超過 resolve_after_secs(5s)之後,intake 的 Tick(真實
        // 1 秒一次)之後應該會把它標記 Resolved 並落地。
        clock.advance(6_000);
        wait_until(
            || {
                sink.last_write(&id)
                    .map(|i| {
                        i.status == IncidentStatus::Resolved
                            && i.status_reason.as_deref() == Some("no_recurrence")
                    })
                    .unwrap_or(false)
            },
            StdDuration::from_secs(3),
        );

        // 之後同一個 fingerprint 再出現一次:因為代表 entry 已經是
        // Resolved,應該被判定為 recurrence(建立新 incident、帶
        // recurrence_of),而不是 Append 到已經 Resolved 的舊 incident 上
        // ——這也間接證明 Resolved 之後舊 incident 已經被踢出 `active`
        // (否則 handle_append 會直接改到 id 身上,不會有新 id 誕生)。
        recorder.submit(make_incident("idles then resolves"));
        wait_until(
            || {
                sink.calls
                    .lock()
                    .unwrap()
                    .write_json
                    .iter()
                    .any(|i| i.recurrence_of.as_deref() == Some(id.as_str()))
            },
            StdDuration::from_secs(3),
        );
        let recurrence = sink
            .calls
            .lock()
            .unwrap()
            .write_json
            .iter()
            .rev()
            .find(|i| i.recurrence_of.as_deref() == Some(id.as_str()))
            .cloned()
            .unwrap();
        assert_ne!(recurrence.id, id, "recurrence 應該是一筆新的 incident");
    }

    /// 修 verifier 抓到的 MEDIUM 問題:一次 Tick 到期一大批(例如重啟後
    /// 長時間停機累積下來)不應該一次全部處理完(每筆都要 fork 一個 `uv`
    /// 分析器子行程,會讓 intake 執行緒卡住)——直接呼叫 `handle_tick`
    /// (不透過 Recorder/背景執行緒,結果才不會受真實 1 秒 Tick 排程影響,
    /// 一次呼叫就是一次 Tick)驗證:20 筆到期,第一次 Tick 只處理
    /// `MAX_RESOLVES_PER_TICK`(16)筆,剩下的留到下一次 Tick。
    #[test]
    fn resolve_tick_caps_resolves_per_tick_oldest_first() {
        let sink = FakeSink::new();
        let mut cfg = incidents_cfg(1024, 8);
        cfg.resolve_after_secs = 10;
        let mut deduper = Deduper::new(cfg.clone());
        let mut active = HashMap::new();
        let base = DateTime::<Utc>::from_timestamp_millis(1_700_000_000_000).unwrap();

        const TOTAL: i64 = 20;
        for i in 0..TOTAL {
            let id = format!("id{i}");
            let fp = format!("fp{i}");
            // last_seen 各不相同,全部都遠遠超過 resolve_after_secs(10s)
            // ——用來確認「舊的優先」:last_seen 越小(越舊)的 i 應該先被
            // resolve_due 排到前面、優先在第一次 Tick 被處理掉。
            let last_seen = base - ChronoDuration::seconds(1000 - i);
            deduper.restore(
                fp.clone(),
                Entry {
                    id: id.clone(),
                    status: IncidentStatus::Open,
                    first_seen: last_seen,
                    last_seen,
                    last_escalated: None,
                },
            );
            let mut inc = make_incident(&format!("idle #{i}"));
            inc.id = id.clone();
            inc.fingerprint = Some(fp);
            inc.last_seen = Some(last_seen);
            active.insert(id, inc);
        }

        let mut state = IntakeState {
            deduper,
            active,
            fp_active: HashMap::new(),
            dirty: HashSet::new(),
            last_flush: HashMap::new(),
            pending: HashSet::new(),
        };
        let overflow = Arc::new(Mutex::new(HashMap::new()));
        let now = base;

        handle_tick(&mut state, sink.as_ref(), &overflow, ChronoDuration::seconds(3600), now);
        assert_eq!(
            state.active.len(),
            (TOTAL as usize) - MAX_RESOLVES_PER_TICK,
            "第一次 Tick 之後,`active` 應該只剩下沒被這次處理到的那些"
        );
        let resolved_after_first: Vec<String> = sink
            .calls
            .lock()
            .unwrap()
            .write_json
            .iter()
            .filter(|i| i.status == IncidentStatus::Resolved)
            .map(|i| i.id.clone())
            .collect();
        assert_eq!(
            resolved_after_first.len(),
            MAX_RESOLVES_PER_TICK,
            "第一次 Tick 應該只處理上限筆數"
        );
        // 舊的優先:last_seen 最小(i 最小)的那 MAX_RESOLVES_PER_TICK 筆應該
        // 是這次被處理掉的那些。
        let expected_first: HashSet<String> = (0..MAX_RESOLVES_PER_TICK as i64)
            .map(|i| format!("id{i}"))
            .collect();
        let actual_first: HashSet<String> = resolved_after_first.into_iter().collect();
        assert_eq!(
            actual_first, expected_first,
            "應該優先處理 last_seen 最舊(最逾期)的那些"
        );

        // 第二次 Tick:剩下的 4 筆應該被處理完,沒有任何一筆被無限期跳過。
        handle_tick(&mut state, sink.as_ref(), &overflow, ChronoDuration::seconds(3600), now);
        assert_eq!(state.active.len(), 0, "第二次 Tick 之後應該全部處理完");
        let resolved_total = sink
            .calls
            .lock()
            .unwrap()
            .write_json
            .iter()
            .filter(|i| i.status == IncidentStatus::Resolved)
            .count();
        assert_eq!(resolved_total, TOTAL as usize);
    }
}
