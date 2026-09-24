//! 事件去重/冷卻的純邏輯層(Milestone 1, step 3)。
//!
//! 這個模組刻意不碰任何 I/O、系統時間或 `Store` —— 時間一律由呼叫端以
//! `now: DateTime<Utc>` 參數傳入,方便測試用假時鐘驅動。從 Milestone 1
//! step 4 起,`recorder.rs` 的 intake 執行緒是唯一呼叫這裡程式碼的地方。
//!
//! 核心規則(見 `Deduper::decide`):
//! - 全新指紋,或舊指紋已經「太久沒出現」且從未被標記為 Mitigated/Resolved
//!   (代表它当初就没被处理好,不是真正结束) → 視為新事件,是否可以升級處置
//!   還要再看 storm guard。`dedup_window_secs == 0` 時一律視為新事件。
//! - 舊指紋在去重視窗內(不論目前狀態)→ `Append`,不重新升級處置。
//! - 舊指紋已經 Mitigated/Resolved 且已經過了去重視窗(或 Resolved 即使還在
//!   視窗內,因為它已經被宣告解決過一次)→ `Recurrence`:是否再次升級處置
//!   要看距離上次升級處置有沒有超過 cooldown。
//! - Storm guard 是全域(跨所有指紋)的滑動視窗計數器,任何「原本會升級」的
//!   決策,如果額度用完,一律改成 `NewSuppressed("storm")` /
//!   `Recurrence { escalate: false, reason: Some("storm") }`。只有實際升級時
//!   才會消耗額度。

use crate::config::IncidentsConfig;
use crate::incident::IncidentStatus;
use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;

/// 單一指紋目前的去重狀態。
#[derive(Debug, Clone)]
pub struct Entry {
    pub id: String,
    pub status: IncidentStatus,
    #[allow(dead_code)] // 目前只有寫入端(on_new/on_recurrence/restore),尚無任何讀取端。
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    /// 最近一次「實際升級處置」(呼叫 agent_service)的時間;從未升級過則為 None。
    pub last_escalated: Option<DateTime<Utc>>,
}

/// `Deduper::decide` 的判斷結果。呼叫端根據這個結果建立/更新 Incident,並回呼
/// `Deduper::on_new`/`on_append` 讓內部狀態表跟著同步。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// 全新事件,應該建立新 Incident 並(可以的話)升級處置。
    NewEscalate,
    /// 全新事件,但被 storm guard 擋下,不升級處置(仍應記錄為新 Incident,
    /// 只是 status 設為 Suppressed)。
    NewSuppressed(&'static str),
    /// 同一個既有 Incident 又出現一次,只需要累加次數/更新 last_seen,不建立
    /// 新 Incident、不升級處置。
    Append { id: String },
    /// 判定為既有 Incident 的重新發生(它先前已經 Mitigated/Resolved)。
    /// `escalate` 為 true 時應該建立新 Incident(`recurrence_of = Some(prev)`)
    /// 並升級處置;為 false 時仍建立新 Incident 但標記 Suppressed,
    /// `reason` 說明原因("cooldown" 或 storm guard 的 "storm")。
    Recurrence {
        prev: String,
        escalate: bool,
        reason: Option<&'static str>,
    },
}

/// 全域升級處置的滑動視窗計數器(每小時 `max_per_hour` 次,跨所有指紋共用)。
/// `max_per_hour == 0` 表示不限制。
#[derive(Debug, Clone)]
pub struct StormGuard {
    max_per_hour: u32,
    /// 過去一小時內「實際升級處置」的時間戳,由舊到新排列。
    timestamps: Vec<DateTime<Utc>>,
}

impl StormGuard {
    pub fn new(max_per_hour: u32) -> Self {
        Self {
            max_per_hour,
            timestamps: Vec::new(),
        }
    }

    fn evict_old(&mut self, now: DateTime<Utc>) {
        let cutoff = now - Duration::hours(1);
        self.timestamps.retain(|ts| *ts > cutoff);
    }

    /// 啟動重建(Milestone 1 step 5)用:把重啟前「過去一小時內的升級處置」
    /// 時間戳灌回去,讓 storm guard 的滑動視窗額度不會在重啟瞬間憑空恢復成
    /// 滿額。呼叫端(`recorder.rs::rebuild_state`)負責只傳一小時內的時間戳
    /// 進來;這裡仍然呼叫一次 `evict_old` 保險,不假設呼叫端一定過濾乾淨。
    fn restore(&mut self, timestamps: Vec<DateTime<Utc>>, now: DateTime<Utc>) {
        self.timestamps = timestamps;
        self.evict_old(now);
    }

    /// 目前這個時間點是否還有額度可以升級處置(不消耗額度,純查詢)。
    pub fn has_capacity(&mut self, now: DateTime<Utc>) -> bool {
        if self.max_per_hour == 0 {
            return true;
        }
        self.evict_old(now);
        (self.timestamps.len() as u32) < self.max_per_hour
    }

    /// 消耗一次額度(只在決策確定要升級處置時呼叫)。
    pub fn consume(&mut self, now: DateTime<Utc>) {
        self.evict_old(now);
        self.timestamps.push(now);
    }
}

/// 事件去重/冷卻狀態機。`entries` 以指紋(fingerprint)為 key。
pub struct Deduper {
    entries: HashMap<String, Entry>,
    storm: StormGuard,
    cfg: IncidentsConfig,
}

impl Deduper {
    pub fn new(cfg: IncidentsConfig) -> Self {
        let storm = StormGuard::new(cfg.max_escalations_per_hour);
        Self {
            entries: HashMap::new(),
            storm,
            cfg,
        }
    }

    fn dedup_window(&self) -> Duration {
        Duration::seconds(self.cfg.dedup_window_secs as i64)
    }

    fn cooldown(&self) -> Duration {
        Duration::seconds(self.cfg.cooldown_secs as i64)
    }

    /// 針對某個指紋在時間 `now` 又出現一次,決定該怎麼處理。
    ///
    /// 不改變去重決策本身依賴的狀態(`entries` 不會被這個函式修改)——但
    /// `storm.has_capacity` 內部會順手把過期超過一小時的 storm 時間戳修剪掉,
    /// 這個修剪不影響任何 `Decision` 的計算結果,純粹是內部簿記。
    ///
    /// **呼叫端合約**:`decide` 回傳後,呼叫端必須(且只能)呼叫恰好一個對應
    /// 的 `on_new`/`on_append`/`on_recurrence`,把實際建立/更新 Incident 的
    /// 結果回報回來 —— 否則 `entries`/storm token 不會更新,下一次 `decide`
    /// 會用到過期的狀態(例如同一個 storm 額度被重複判定為可用)。
    pub fn decide(&mut self, fp: &str, now: DateTime<Utc>) -> Decision {
        let dedup_disabled = self.cfg.dedup_window_secs == 0;

        let Some(entry) = self.entries.get(fp).cloned() else {
            return self.decide_new(now);
        };

        // Suppressed 的事件不論在不在去重視窗內都要重新評估是否已經解除
        // 抑制(storm 額度恢復 / cooldown 過了),否則會卡在 Append 永遠
        // 不再升級處置,即使抑制它的原因早就消失了。
        if entry.status == IncidentStatus::Suppressed {
            return self.decide_suppressed(&entry, now);
        }

        let within_window = !dedup_disabled && now - entry.last_seen <= self.dedup_window();
        let never_mitigated = matches!(entry.status, IncidentStatus::Open | IncidentStatus::Escalating);

        if !dedup_disabled && within_window {
            // Resolved 即使在視窗內也視為「又發生了」,因為它已經被宣告解決過。
            if entry.status == IncidentStatus::Resolved {
                return self.decide_recurrence(fp, &entry, now);
            }
            return Decision::Append { id: entry.id };
        }

        // 視窗外(或 dedup 關閉):
        if never_mitigated {
            // 從未被妥善處理過就又消失又出現,視為全新事件重新走一次升級。
            return self.decide_new(now);
        }

        // Mitigated 或 Resolved,視窗外(或 dedup 關閉)→ recurrence。
        self.decide_recurrence(fp, &entry, now)
    }

    /// 目前記錄的代表事件處於 Suppressed 狀態時的決策(不分視窗內外)。
    /// 只有在真的要升級處置時才建立新 Incident(`Recurrence{escalate:true}`),
    /// 否則一律 `Append` 到既有的 suppressed incident 上,累加次數就好
    /// ——避免同一個一直被抑制的指紋每次出現都多生一筆新 Incident。
    fn decide_suppressed(&mut self, entry: &Entry, now: DateTime<Utc>) -> Decision {
        let cooldown_ok = match entry.last_escalated {
            None => true,
            Some(last) => now - last >= self.cooldown(),
        };
        if cooldown_ok && self.storm.has_capacity(now) {
            Decision::Recurrence {
                prev: entry.id.clone(),
                escalate: true,
                reason: None,
            }
        } else {
            Decision::Append { id: entry.id.clone() }
        }
    }

    fn decide_new(&mut self, now: DateTime<Utc>) -> Decision {
        if self.storm.has_capacity(now) {
            Decision::NewEscalate
        } else {
            Decision::NewSuppressed("storm")
        }
    }

    fn decide_recurrence(&mut self, fp: &str, entry: &Entry, now: DateTime<Utc>) -> Decision {
        let _ = fp;
        let cooldown_ok = match entry.last_escalated {
            None => true,
            Some(last) => now - last >= self.cooldown(),
        };
        if !cooldown_ok {
            return Decision::Recurrence {
                prev: entry.id.clone(),
                escalate: false,
                reason: Some("cooldown"),
            };
        }
        if !self.storm.has_capacity(now) {
            return Decision::Recurrence {
                prev: entry.id.clone(),
                escalate: false,
                reason: Some("storm"),
            };
        }
        Decision::Recurrence {
            prev: entry.id.clone(),
            escalate: true,
            reason: None,
        }
    }

    /// 記錄「全新事件」的結果(不論是否升級處置都要呼叫一次)。
    pub fn on_new(&mut self, fp: String, id: String, status: IncidentStatus, now: DateTime<Utc>, escalated: bool) {
        if escalated {
            self.storm.consume(now);
        }
        self.entries.insert(
            fp,
            Entry {
                id,
                status,
                first_seen: now,
                last_seen: now,
                last_escalated: if escalated { Some(now) } else { None },
            },
        );
    }

    /// 記錄一次 `Append`(同一事件又出現)—— 更新 last_seen,狀態不變。
    pub fn on_append(&mut self, fp: &str, now: DateTime<Utc>) {
        if let Some(entry) = self.entries.get_mut(fp) {
            entry.last_seen = now;
        }
    }

    /// 記錄一次 `Recurrence` 的結果:建立新 Incident 後,這個指紋的「目前代表
    /// 事件」換成新的 id/status,`last_escalated` 只在真的升級處置時更新。
    pub fn on_recurrence(&mut self, fp: String, new_id: String, status: IncidentStatus, now: DateTime<Utc>, escalated: bool) {
        let last_escalated = if escalated {
            self.storm.consume(now);
            Some(now)
        } else {
            self.entries.get(&fp).and_then(|e| e.last_escalated)
        };
        self.entries.insert(
            fp,
            Entry {
                id: new_id,
                status,
                first_seen: now,
                last_seen: now,
                last_escalated,
            },
        );
    }

    /// 直接更新某個指紋目前記錄的狀態(例如 stage 驗證後判定 Mitigated,或
    /// resolve_due 判定 Resolved)。找不到該指紋時是 no-op。
    pub fn set_status(&mut self, fp: &str, status: IncidentStatus) {
        if let Some(entry) = self.entries.get_mut(fp) {
            entry.status = status;
        }
    }

    /// 啟動重建(Milestone 1 step 5)專用:直接插入一筆從舊 JSON 重建出來的
    /// entry(蓋掉同指紋既有的,啟動時 `entries` 本來就是空的,不會真的蓋到
    /// 任何東西)。跟 `on_new`/`on_recurrence` 不同,這裡不消耗/影響 storm
    /// guard——storm guard 的額度重建是 `seed_storm` 的職責,兩者分開呼叫。
    pub fn restore(&mut self, fp: String, entry: Entry) {
        self.entries.insert(fp, entry);
    }

    /// 啟動重建專用:見 `StormGuard::restore` 的文件——把重啟前一小時內的
    /// 升級處置時間戳灌回滑動視窗計數器。
    pub fn seed_storm(&mut self, timestamps: Vec<DateTime<Utc>>, now: DateTime<Utc>) {
        self.storm.restore(timestamps, now);
    }

    /// 找出所有處於 Open/Mitigated/Suppressed 狀態、且 last_seen 已經超過
    /// `resolve_after_secs` 的指紋,回傳它們目前的 Incident id,**依
    /// `last_seen` 由舊到新(最逾期的排最前面)排序**——呼叫端(`recorder.rs`
    /// 的 `handle_tick`)如果每個 Tick 只處理得完前面一部分(見那裡的
    /// `MAX_RESOLVES_PER_TICK`),這個順序保證優先處理的一定是逾期最久的,
    /// 不會有某個 entry 因為 HashMap 迭代順序不固定而一直排不到、無限期
    /// 餓死——沒被這次處理到的,下一次 Tick 重新呼叫 `resolve_due` 時仍然
    /// 會在(因為它們的狀態還沒被改成 Resolved),而且排序只會讓它們更靠前。
    /// 呼叫端負責把對應的 Incident 標記為 Resolved,並透過 `set_status`
    /// 回報。
    ///
    /// Milestone 1 step 5 決定:`Suppressed`(不論原因是 storm/cooldown/
    /// queue_full)閒置超過 `resolve_after_secs` 沒有再出現,也視同「自然
    /// 解決」一起標記為 Resolved——理由是它代表的錯誤已經不再發生,继续让
    /// 它以 Suppressed 停留在 `active`/entries 裡沒有任何好處,只會讓這兩個
    /// map 隨時間無限增長;之後如果同一種錯誤真的又出現,會被當成一次全新
    /// 的 recurrence 重新評估是否升級,而不是永遠卡在「被抑制」的狀態。
    pub fn resolve_due(&self, now: DateTime<Utc>) -> Vec<String> {
        let threshold = Duration::seconds(self.cfg.resolve_after_secs as i64);
        let mut due: Vec<&Entry> = self
            .entries
            .values()
            .filter(|e| {
                matches!(
                    e.status,
                    IncidentStatus::Open | IncidentStatus::Mitigated | IncidentStatus::Suppressed
                )
            })
            .filter(|e| now - e.last_seen > threshold)
            .collect();
        due.sort_by_key(|e| e.last_seen);
        due.into_iter().map(|e| e.id.clone()).collect()
    }

    /// 修剪已經 Resolved、且久到不會再影響任何未來決策的 entry,避免
    /// `entries` map 隨行程長時間執行無限增長。
    ///
    /// 為什麼這個 horizon 是安全的:`decide()` 只在兩種情況下用到 Resolved
    /// entry——(a) 還在 dedup window 內又出現 → 強制視為 recurrence(而非
    /// Append);(b) 視窗外 → 一樣走 recurrence,並用 `last_escalated` 判斷
    /// cooldown 是否已過。一旦 `now - last_seen` 超過
    /// `max(cooldown_secs, resolve_after_secs)`,dedup window 必然也早已過
    /// (`resolve_after_secs` 依 config 驗證規則 ≥ `dedup_window_secs`),且
    /// cooldown 必然也早已過——所以就算這筆 entry 被刪掉、之後同指紋再出現
    /// 被 `decide_new` 當成全新事件處理,結果(建立新 incident、視情況升級)
    /// 跟被判成 recurrence 幾乎一樣,唯一差別只是少了 `recurrence_of` 這個
    /// 溯源欄位——可接受的代價換取 entries map 有界。
    pub fn prune_expired(&mut self, now: DateTime<Utc>) {
        let horizon = Duration::seconds(self.cfg.cooldown_secs.max(self.cfg.resolve_after_secs) as i64);
        self.entries
            .retain(|_, e| !(e.status == IncidentStatus::Resolved && now - e.last_seen > horizon));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(dedup_secs: u64, cooldown_secs: u64, resolve_after_secs: u64, max_per_hour: u32) -> IncidentsConfig {
        IncidentsConfig {
            dedup_window_secs: dedup_secs,
            cooldown_secs,
            resolve_after_secs,
            max_escalations_per_hour: max_per_hour,
            max_queue: 1024,
            max_pending_escalations: 8,
            rewrite_interval_secs: 10,
            shutdown_grace_secs: 30,
        }
    }

    fn t(offset_secs: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_700_000_000 + offset_secs, 0).unwrap()
    }

    #[test]
    fn first_occurrence_escalates() {
        let mut d = Deduper::new(cfg(300, 1800, 3600, 6));
        let decision = d.decide("fp1", t(0));
        assert_eq!(decision, Decision::NewEscalate);
    }

    #[test]
    fn repeat_inside_window_appends() {
        let mut d = Deduper::new(cfg(300, 1800, 3600, 6));
        assert_eq!(d.decide("fp1", t(0)), Decision::NewEscalate);
        d.on_new("fp1".into(), "id1".into(), IncidentStatus::Open, t(0), true);

        let decision = d.decide("fp1", t(100));
        assert_eq!(decision, Decision::Append { id: "id1".into() });
    }

    #[test]
    fn repeat_just_past_window_of_unmitigated_opens_new() {
        let mut d = Deduper::new(cfg(300, 1800, 3600, 6));
        d.decide("fp1", t(0));
        d.on_new("fp1".into(), "id1".into(), IncidentStatus::Open, t(0), true);

        // 超過 dedup_window (300s),但從未 Mitigated → 視為新事件。
        let decision = d.decide("fp1", t(301));
        assert_eq!(decision, Decision::NewEscalate);
    }

    #[test]
    fn recurrence_after_mitigated_within_cooldown_suppressed() {
        let mut d = Deduper::new(cfg(300, 1800, 3600, 6));
        d.decide("fp1", t(0));
        d.on_new("fp1".into(), "id1".into(), IncidentStatus::Open, t(0), true);
        d.set_status("fp1", IncidentStatus::Mitigated);

        // 視窗外,但距離上次升級處置(t=0)只過了 301+1000=1301s < cooldown 1800s。
        let decision = d.decide("fp1", t(1301));
        assert_eq!(
            decision,
            Decision::Recurrence {
                prev: "id1".into(),
                escalate: false,
                reason: Some("cooldown"),
            }
        );
    }

    #[test]
    fn recurrence_after_cooldown_escalates_with_recurrence_of() {
        let mut d = Deduper::new(cfg(300, 1800, 3600, 6));
        d.decide("fp1", t(0));
        d.on_new("fp1".into(), "id1".into(), IncidentStatus::Open, t(0), true);
        d.set_status("fp1", IncidentStatus::Mitigated);

        // 距離上次升級處置(t=0)已經過了 1801s >= cooldown 1800s。
        let decision = d.decide("fp1", t(1801));
        assert_eq!(
            decision,
            Decision::Recurrence {
                prev: "id1".into(),
                escalate: true,
                reason: None,
            }
        );
    }

    #[test]
    fn resolved_inside_window_is_still_recurrence() {
        let mut d = Deduper::new(cfg(300, 1800, 3600, 6));
        d.decide("fp1", t(0));
        d.on_new("fp1".into(), "id1".into(), IncidentStatus::Open, t(0), true);
        d.set_status("fp1", IncidentStatus::Resolved);

        // 還在 dedup window 內(t=100 - t=0 = 100s < 300s),但因為已經 Resolved,
        // 視為「宣告解決後又發生」→ Recurrence(不是單純 Append)。
        let decision = d.decide("fp1", t(100));
        assert!(matches!(decision, Decision::Recurrence { .. }));
    }

    /// 這個測試只驗證 `StormGuard` 額度本身會隨滑動視窗恢復 —— fp6 這次
    /// 被抑制之後刻意不呼叫 `on_new`,所以完全沒有留下 entry,下一次
    /// `decide("fp6", ..)` 走的仍然是「全新指紋」路徑(`decide_new`),
    /// 不是「一個被 Suppressed 的既有 incident 重新評估」的路徑,後者由
    /// `suppressed_entry_recovers_after_storm_capacity_frees_up` 覆蓋。
    #[test]
    fn storm_guard_capacity_recovers_after_window_slides_for_fresh_fingerprint() {
        let mut d = Deduper::new(cfg(300, 1800, 3600, 6));
        for i in 0..6 {
            let fp = format!("fp{i}");
            let decision = d.decide(&fp, t(i as i64));
            assert_eq!(decision, Decision::NewEscalate, "第 {i} 次應該還有額度");
            d.on_new(fp, format!("id{i}"), IncidentStatus::Open, t(i as i64), true);
        }

        // 第 7 個不同指紋,額度已經用完,且沒有呼叫 on_new 留下 entry。
        let decision = d.decide("fp6", t(6));
        assert_eq!(decision, Decision::NewSuppressed("storm"));

        // 滑過一小時視窗後,最早的 token 過期,額度恢復;fp6 仍是全新指紋。
        let decision = d.decide("fp6", t(3600 + 1));
        assert_eq!(decision, Decision::NewEscalate);
    }

    /// 修回報的 bug:一個被 storm 抑制的 entry(status = Suppressed)在去重
    /// 視窗內反覆出現時,必須持續重新評估是否解除抑制,而不是永遠 Append
    /// 下去 —— storm 額度恢復後,下一次出現要能升級處置並換成新 incident;
    /// 再之後的出現則 Append 到那個新 incident 上。
    #[test]
    fn suppressed_entry_recovers_after_storm_capacity_frees_up() {
        let mut d = Deduper::new(cfg(300, 1800, 3600, 6));
        // 先用其他 6 個指紋把 storm 額度用滿(t=0..5)。
        for i in 0..6 {
            let fp = format!("fp{i}");
            d.decide(&fp, t(i as i64));
            d.on_new(fp, format!("id{i}"), IncidentStatus::Open, t(i as i64), true);
        }

        // fp6 在額度用完後第一次出現 → NewSuppressed("storm"),持久化成
        // status = Suppressed、從未升級過(last_escalated = None)。
        let decision = d.decide("fp6", t(6));
        assert_eq!(decision, Decision::NewSuppressed("storm"));
        d.on_new("fp6".into(), "sup1".into(), IncidentStatus::Suppressed, t(6), false);

        // 每 60 秒重新出現一次,storm 額度(t=0..5 的 token)還沒過期
        // (未滿一小時)之前應該一直 Append 到同一個 suppressed incident。
        for k in 1..10 {
            let now = t(6 + k * 60);
            let decision = d.decide("fp6", now);
            assert_eq!(decision, Decision::Append { id: "sup1".into() }, "k={k} 應該還在抑制中");
            d.on_append("fp6", now);
        }

        // 一小時後(t > 3600),最早的 token(t=0)過期,額度恢復 → 這次
        // 出現應該升級處置,並且是 Recurrence(帶 recurrence_of),不是
        // Append,因為終於真的建立了一個新 incident。
        let now = t(3601);
        let decision = d.decide("fp6", now);
        assert_eq!(
            decision,
            Decision::Recurrence {
                prev: "sup1".into(),
                escalate: true,
                reason: None,
            }
        );
        d.on_recurrence("fp6".into(), "id6".into(), IncidentStatus::Open, now, true);

        // 之後再出現(仍在新 incident 的去重視窗內)→ Append 到新 incident。
        let decision = d.decide("fp6", t(3601 + 60));
        assert_eq!(decision, Decision::Append { id: "id6".into() });
    }

    /// 同樣的「抑制解除後要能重新升級」規則,對 cooldown 造成的 Suppressed
    /// 也成立(不是只有 storm)。
    #[test]
    fn suppressed_entry_recovers_after_cooldown_elapses() {
        let mut d = Deduper::new(cfg(300, 1800, 3600, 6));
        d.decide("fp1", t(0));
        d.on_new("fp1".into(), "id1".into(), IncidentStatus::Open, t(0), true);
        d.set_status("fp1", IncidentStatus::Mitigated);

        // 視窗外、但距離上次升級處置(t=0)只過了 1000s < cooldown(1800s)
        // → Recurrence{escalate:false, reason:cooldown};呼叫端建立一筆
        // Suppressed 的新 incident,last_escalated 仍然沿用舊的(t=0)。
        let decision = d.decide("fp1", t(1000));
        assert_eq!(
            decision,
            Decision::Recurrence {
                prev: "id1".into(),
                escalate: false,
                reason: Some("cooldown"),
            }
        );
        d.on_recurrence("fp1".into(), "sup1".into(), IncidentStatus::Suppressed, t(1000), false);

        // cooldown 還沒過(last_escalated 仍是 t=0)之前反覆出現 → Append。
        for now_offset in [1200, 1500, 1799] {
            let now = t(now_offset);
            let decision = d.decide("fp1", now);
            assert_eq!(decision, Decision::Append { id: "sup1".into() }, "offset={now_offset}");
            d.on_append("fp1", now);
        }

        // 距離 t=0 已經 >= 1800s(cooldown 過了)且 storm 有額度 → 升級處置。
        let now = t(1800);
        let decision = d.decide("fp1", now);
        assert_eq!(
            decision,
            Decision::Recurrence {
                prev: "sup1".into(),
                escalate: true,
                reason: None,
            }
        );
        d.on_recurrence("fp1".into(), "id2".into(), IncidentStatus::Open, now, true);

        // 新 incident 建立後,再出現就是 Append 到它身上。
        let decision = d.decide("fp1", t(1800 + 60));
        assert_eq!(decision, Decision::Append { id: "id2".into() });
    }

    #[test]
    fn dedup_window_zero_disables_dedup() {
        let mut d = Deduper::new(cfg(0, 1800, 3600, 6));
        d.decide("fp1", t(0));
        d.on_new("fp1".into(), "id1".into(), IncidentStatus::Open, t(0), true);

        // dedup 關閉,即使緊接著再次出現也視為新事件(受 storm guard 影響)。
        let decision = d.decide("fp1", t(1));
        assert_eq!(decision, Decision::NewEscalate);
    }

    #[test]
    fn resolve_due_boundary() {
        let d = Deduper::new(cfg(300, 1800, 3600, 6));
        let mut d = d;
        d.decide("fp1", t(0));
        d.on_new("fp1".into(), "id1".into(), IncidentStatus::Open, t(0), true);

        // 恰好等於 resolve_after_secs (3600s) 不算過期(> 才算)。
        assert!(d.resolve_due(t(3600)).is_empty());
        // 超過 1 秒才算到期。
        assert_eq!(d.resolve_due(t(3601)), vec!["id1".to_string()]);
    }

    #[test]
    fn resolve_due_ignores_resolved_but_includes_suppressed() {
        let mut d = Deduper::new(cfg(300, 1800, 3600, 6));
        d.decide("fp1", t(0));
        d.on_new("fp1".into(), "id1".into(), IncidentStatus::Open, t(0), true);
        d.set_status("fp1", IncidentStatus::Resolved);
        assert!(d.resolve_due(t(10_000)).is_empty());

        // Suppressed 不再被忽略(Milestone 1 step 5 決定,見 `resolve_due`
        // 文件):閒置超過 resolve_after_secs 也要被判定為到期。
        d.decide("fp2", t(0));
        d.on_new("fp2".into(), "id2".into(), IncidentStatus::Suppressed, t(0), false);
        assert_eq!(d.resolve_due(t(10_000)), vec!["id2".to_string()]);
    }

    #[test]
    fn resolve_due_returns_oldest_last_seen_first() {
        let mut d = Deduper::new(cfg(300, 1800, 10, 6));
        d.decide("fp1", t(0));
        d.on_new("fp1".into(), "id1".into(), IncidentStatus::Open, t(0), true);
        d.decide("fp2", t(5));
        d.on_new("fp2".into(), "id2".into(), IncidentStatus::Open, t(5), true);
        d.decide("fp3", t(2));
        d.on_new("fp3".into(), "id3".into(), IncidentStatus::Open, t(2), true);

        // 全部都已逾期(resolve_after_secs = 10),排序應該是 last_seen 由
        // 舊到新:id1(t=0) < id3(t=2) < id2(t=5)——呼叫端只處理得完一部分時
        // (見 recorder.rs 的 MAX_RESOLVES_PER_TICK)才不會有 entry 因為
        // HashMap 迭代順序不固定而被無限期跳過。
        assert_eq!(
            d.resolve_due(t(1000)),
            vec!["id1".to_string(), "id3".to_string(), "id2".to_string()]
        );
    }
}
