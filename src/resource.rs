use crate::config::Config;
use crate::incident::{Incident, Severity, Source};
use crate::recorder::Recorder;
use crate::shutdown::{self, ShutdownFlag};
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::{Disks, System};

/// 定期輪詢系統資源(CPU/記憶體/磁碟),超過設定門檻就記錄一個事件並觸發處置。
pub fn watch_resources(cfg: Arc<Config>, recorder: Recorder, shutdown: ShutdownFlag) -> Result<()> {
    if !cfg.resources.enabled {
        return Ok(());
    }

    let mut sys = System::new_all();
    let mut last_disk_alert = false;
    let mut last_cpu_alert = false;
    let mut last_mem_alert = false;

    loop {
        if shutdown::is_set(&shutdown) {
            return Ok(());
        }
        sys.refresh_cpu_usage();
        sys.refresh_memory();
        let cpu = sys.global_cpu_usage();
        let mem_percent = if sys.total_memory() > 0 {
            (sys.used_memory() as f32 / sys.total_memory() as f32) * 100.0
        } else {
            0.0
        };

        let disks = Disks::new_with_refreshed_list();
        let disk_percent = disks
            .iter()
            .map(|d| {
                let total = d.total_space() as f32;
                if total == 0.0 {
                    0.0
                } else {
                    (1.0 - d.available_space() as f32 / total) * 100.0
                }
            })
            .fold(0.0_f32, f32::max);

        if cpu >= cfg.resources.cpu_percent && !last_cpu_alert {
            emit(&cfg, &recorder, format!("CPU 使用率過高:{cpu:.1}% (門檻 {:.1}%)", cfg.resources.cpu_percent));
        }
        if mem_percent >= cfg.resources.memory_percent && !last_mem_alert {
            emit(
                &cfg,
                &recorder,
                format!("記憶體使用率過高:{mem_percent:.1}% (門檻 {:.1}%)", cfg.resources.memory_percent),
            );
        }
        if disk_percent >= cfg.resources.disk_percent && !last_disk_alert {
            emit(
                &cfg,
                &recorder,
                format!("磁碟使用率過高:{disk_percent:.1}% (門檻 {:.1}%)", cfg.resources.disk_percent),
            );
        }

        last_cpu_alert = cpu >= cfg.resources.cpu_percent;
        last_mem_alert = mem_percent >= cfg.resources.memory_percent;
        last_disk_alert = disk_percent >= cfg.resources.disk_percent;

        shutdown::sleep_interruptible(&shutdown, Duration::from_millis(cfg.resources.poll_interval_ms));
    }
}

fn emit(cfg: &Config, recorder: &Recorder, message: String) {
    let incident = Incident::detected(
        cfg.name.clone(),
        Source::LogFile("system-resources".into()),
        message,
        vec![],
        String::new(),
        cfg.command.clone(),
        None,
        false,
        0,
        Severity::Medium,
    );
    recorder.submit(incident);
}
