mod agent_client;
mod ai;
mod central_client;
mod config;
mod dedup;
mod diagnostics;
mod fingerprint;
mod incident;
mod matcher;
mod onboard;
mod recorder;
mod resource;
mod shutdown;
mod store;
mod supervisor;
mod watcher;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use config::Config;
use recorder::Recorder;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use store::Store;

#[derive(Parser)]
#[command(name = "artemis", about = "Agent 版 autoheal — 即時偵測、記錄與根因分析")]
struct Cli {
    #[arg(long, default_value = "artemis.toml", global = true)]
    config: PathBuf,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// 建立 artemis.toml 設定檔範本
    Init,
    /// 掃描目標 repo 並自動產生設定檔到 configs/<repo-name>.toml
    Onboard {
        /// 目標專案的路徑
        repo: PathBuf,
    },
    /// 啟動監控:接管指定指令的執行,並監看額外的 log 檔案
    Watch,
    /// 列出所有已記錄的事件
    List,
    /// 顯示單一事件的根因分析報告
    Show {
        id: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.cmd {
        Cmd::Init => cmd_init(&cli.config),
        Cmd::Onboard { repo } => cmd_onboard(&repo),
        Cmd::Watch => cmd_watch(&cli.config),
        Cmd::List => cmd_list(&cli.config),
        Cmd::Show { id } => cmd_show(&cli.config, &id),
    }
}

fn cmd_onboard(repo: &PathBuf) -> Result<()> {
    let artemis_root = std::env::current_dir().context("無法取得目前工作目錄")?;
    let result = onboard::onboard(repo, &artemis_root)?;

    println!("已產生設定檔:{}", result.config_path.display());
    println!(
        "{}",
        if result.ai_assisted {
            "(以下監控/處置設定由 AI 實際讀過專案內容後判斷產生,請確認後再啟動監控)"
        } else {
            "(AI 分析未成功執行,以下僅為靜態偵測結果,請務必手動檢查每一項設定)"
        }
    );
    println!("──────────────────────────────");
    println!("{}", result.summary);
    println!("──────────────────────────────");
    println!(
        "確認無誤後執行:\n  artemis watch --config {}",
        result.config_path.display()
    );
    Ok(())
}

fn cmd_init(path: &PathBuf) -> Result<()> {
    if path.exists() {
        anyhow::bail!("設定檔已存在:{}", path.display());
    }
    fs::write(path, Config::EXAMPLE)
        .with_context(|| format!("無法寫入設定檔:{}", path.display()))?;
    println!("已建立 {}\n請編輯 command / log_files 等欄位後執行 `artemis watch`", path.display());
    Ok(())
}

fn cmd_watch(path: &PathBuf) -> Result<()> {
    let cfg = Arc::new(Config::load(path)?);
    // Milestone 1 step 4:偵測執行緒不再直接呼叫 `Store::record`(同步跑診斷
    // /升級處置/分析器,可能卡住數十秒到 timeout_secs)。改成把事件丟給
    // `Recorder::submit`(永不阻塞),實際的去重判斷、落地、升級處置都在
    // recorder.rs 起的獨立 intake/worker 執行緒裡完成。
    let (recorder, intake_handle) = Recorder::new(cfg.clone())?;

    // Milestone 1 step 6:註冊 SIGTERM/SIGINT,收到訊號就把這個旗標設成
    // true。watcher/resource 的監看迴圈跟 supervisor 都會在既有的輪詢週期
    // 順便檢查它並自行收尾;supervisor 正常結束(max_restarts 用完)之後
    // 這裡也會主動 `request` 一次,讓 watcher/resource 執行緒同樣停下來
    // ——這正是修這次 shutdown bug 的關鍵:過去它們是無窮迴圈,永遠不會
    // 自然 drop 掉手上的 `Recorder` clone,intake 收尾的那段程式碼永遠
    // 執行不到。
    let shutdown_flag = shutdown::new_flag();
    shutdown::install(&shutdown_flag)?;

    let watcher_cfg = cfg.clone();
    let watcher_recorder = recorder.clone();
    let watcher_shutdown = shutdown_flag.clone();
    let watcher_handle = std::thread::spawn(move || {
        if let Err(e) = watcher::watch_log_files(watcher_cfg, watcher_recorder, watcher_shutdown) {
            eprintln!("[artemis] log 檔案監看執行緒結束:{e}");
        }
    });

    let resource_cfg = cfg.clone();
    let resource_recorder = recorder.clone();
    let resource_shutdown = shutdown_flag.clone();
    let resource_handle = std::thread::spawn(move || {
        if let Err(e) = resource::watch_resources(resource_cfg, resource_recorder, resource_shutdown) {
            eprintln!("[artemis] 系統資源監看執行緒結束:{e}");
        }
    });

    let diagnostics_cfg = cfg.clone();
    std::thread::spawn(move || {
        if let Err(e) = diagnostics::watch_diagnostics(diagnostics_cfg) {
            eprintln!("[artemis] 診斷數值記錄執行緒結束:{e}");
        }
    });

    let result = supervisor::watch(&cfg, &recorder, &shutdown_flag);

    // supervisor 可能是因為正常結束(max_restarts 用完)才 return 的,不一定
    // 是收到訊號——不管哪種情況都要讓 watcher/resource 一起收尾,再讓
    // recorder 的 intake 執行緒瀝乾、flush、等在途升級處置。
    shutdown::request(&shutdown_flag);
    let _ = watcher_handle.join();
    let _ = resource_handle.join();
    recorder.shutdown_and_join(intake_handle);

    result
}

fn cmd_list(path: &PathBuf) -> Result<()> {
    let cfg = Config::load(path)?;
    let store = Store::new(&cfg.incidents_dir)?;
    let incidents = store.list()?;

    if incidents.is_empty() {
        println!("目前沒有任何已記錄的事件。");
        return Ok(());
    }

    println!("{:<24}  {:<20}  {}", "ID", "時間", "訊息");
    for inc in incidents {
        println!(
            "{:<24}  {:<20}  {}",
            inc.id,
            inc.timestamp.format("%Y-%m-%d %H:%M:%S"),
            inc.message
        );
    }
    Ok(())
}

fn cmd_show(path: &PathBuf, id: &str) -> Result<()> {
    let cfg = Config::load(path)?;
    let store = Store::new(&cfg.incidents_dir)?;
    println!("{}", store.show_report(id)?);
    Ok(())
}
