#!/usr/bin/env python3
"""Artemis autoheal 根因分析腳本。

由 Rust 核心(src/store.rs)在每次記錄事件時,透過
`uv run --project analyzer analyze.py <incident.json> ...` 呼叫。
負責:讀取事件 JSON,補上原始碼片段與 git blame 資訊,
把 Rust 端執行的四層 AI 分級處置結果整理成人類可讀的 Markdown 報告。
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


def read_context(abs_path: Path, line: int, context_lines: int):
    try:
        lines = abs_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    start = max(0, line - 1 - context_lines)
    end = min(len(lines), line + context_lines)
    return [
        {"line_no": start + i + 1, "code": code, "is_target": start + i + 1 == line}
        for i, code in enumerate(lines[start:end])
    ]


def git_blame(project_root: Path, rel_path: str, line: int):
    try:
        out = subprocess.run(
            ["git", "-C", str(project_root), "blame", "-L", f"{line},{line}", "--porcelain", rel_path],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if out.returncode != 0:
        return None

    text = out.stdout
    commit = text.split(" ", 1)[0] if text else None
    author = None
    summary = None
    for raw_line in text.splitlines():
        if raw_line.startswith("author "):
            author = raw_line[len("author "):]
        elif raw_line.startswith("summary "):
            summary = raw_line[len("summary "):]
    if not commit:
        return None
    return {"commit": commit[:12], "author": author, "summary": summary}


def render_frame(frame: dict, project_root: Path, context_lines: int) -> str:
    file_rel = frame.get("file", "")
    line_no = frame.get("line", 0)
    abs_path = (project_root / file_rel).resolve()

    parts = [f"- `{frame.get('function', '<anonymous>')}` — {file_rel}:{line_no}"]

    if abs_path.exists() and "node_modules" not in file_rel:
        blame = git_blame(project_root, file_rel, line_no)
        if blame:
            parts.append(
                f"  - 最後異動:`{blame['commit']}` by {blame.get('author') or '?'} — {blame.get('summary') or ''}"
            )
        ctx = read_context(abs_path, line_no, context_lines)
        if ctx:
            parts.append("  ```")
            for row in ctx:
                marker = ">>" if row["is_target"] else "  "
                parts.append(f"  {marker} {row['line_no']:>5} | {row['code']}")
            parts.append("  ```")
    return "\n".join(parts)


def render_stage(title: str, stage: dict | None) -> str:
    if not stage or not stage.get("ran"):
        return f"### {title}\n\n(未執行)\n"

    lines = [f"### {title}", ""]
    lines.append(f"- 執行動作:{stage.get('action_taken') or '(無)'}")
    if stage.get("reasoning"):
        lines.append(f"- 判斷理由:{stage['reasoning']}")
    if stage.get("root_cause_hypothesis"):
        lines.append(f"- 根因假設:{stage['root_cause_hypothesis']}")
    if stage.get("files_changed"):
        lines.append(f"- 變更檔案:{', '.join(stage['files_changed'])}")
    if stage.get("test_result"):
        lines.append(f"- 測試結果:{stage['test_result']}")
    if stage.get("verified_resolved") is not None:
        status = "已解決 ✅" if stage["verified_resolved"] else "未解決,繼續下一層 ⏭️"
        lines.append(f"- 驗證結果:{status}")
    lines.append("")
    return "\n".join(lines)


ROLE_LABELS = {
    "risk_analysis": "風險分析",
    "security_analysis": "安全性分析",
    "quick_fix_analysis": "快速修復分析",
    "log_analysis": "log 分析",
    "root_cause_analysis": "根因分析",
}


def render_multi_agent_analysis(analysis: dict | None) -> str:
    if not analysis or not analysis.get("selected_agents"):
        return ""

    lines = ["## 多模型 Agent Harness 分析", ""]
    if analysis.get("dispatch_reasoning"):
        lines.append(f"- 派工理由:{analysis['dispatch_reasoning']}")
    lines.append(f"- 派出的分析 agent:{', '.join(analysis['selected_agents'])}")
    lines.append("")

    for finding in analysis.get("findings") or []:
        role = finding.get("role", "")
        label = ROLE_LABELS.get(role, role)
        risk = finding.get("risk_level") or "unknown"
        lines.append(f"### {label}（risk_level: {risk}）")
        lines.append("")
        lines.append(finding.get("summary") or "(無)")
        lines.append("")

    lines.append("### 彙整結論")
    lines.append("")
    if analysis.get("root_cause_hypothesis"):
        lines.append(f"- 根因假設:{analysis['root_cause_hypothesis']}")
    if analysis.get("risk_level"):
        lines.append(f"- 整體風險等級:{analysis['risk_level']}")
    lines.append(f"- 建議處置層級:{analysis.get('recommended_stage', 'unknown')}")
    lines.append("")
    return "\n".join(lines)


# 跟 agent_service/tools.py::READ_FILE_MAX_CHARS 同樣的理由:單一診斷指令的
# 輸出理論上很小(docker stats/free -m/nvidia-smi 一行),但避免有人塞了會
# 印很多行的指令進 [diagnostics].commands,還是設個上限保護報告可讀性。
DIAGNOSTIC_OUTPUT_MAX_CHARS = 2000


def render_diagnostics_history(samples: list) -> str:
    if not samples:
        return ""

    from collections import OrderedDict
    from datetime import datetime, timezone

    by_command = OrderedDict()
    for sample in sorted(samples, key=lambda s: s.get("ts_ms", 0)):
        by_command.setdefault(sample.get("command", ""), []).append(sample)

    lines = ["## 診斷數值(事故前歷史)", ""]
    lines.append(
        "以下是事故發生前(以及觸發當下補跑一次)的原始診斷指令輸出,"
        "依指令分組、按時間排序,方便直接看出惡化過程,不需要另外查儀表板。"
    )
    lines.append("")

    for command, entries in by_command.items():
        lines.append(f"### `{command}`")
        lines.append("")
        for entry in entries:
            ts = datetime.fromtimestamp(entry.get("ts_ms", 0) / 1000, tz=timezone.utc)
            output = (entry.get("output") or "").strip()
            if len(output) > DIAGNOSTIC_OUTPUT_MAX_CHARS:
                output = output[:DIAGNOSTIC_OUTPUT_MAX_CHARS] + "\n[已截斷]"
            exit_code = entry.get("exit_code")
            lines.append(f"- **{ts.strftime('%Y-%m-%d %H:%M:%S UTC')}** (exit {exit_code})")
            if output:
                lines.append("  ```")
                for out_line in output.splitlines():
                    lines.append(f"  {out_line}")
                lines.append("  ```")
        lines.append("")

    return "\n".join(lines)


def build_report(incident: dict, project_root: Path, context_lines: int) -> str:
    lines = []
    lines.append(f"# 事件報告:{incident['id']}")
    lines.append("")
    lines.append(f"- 專案:{incident.get('project')}")
    lines.append(f"- 時間:{incident.get('timestamp')}")
    lines.append(f"- 來源:{incident.get('source')}")
    lines.append(f"- 訊息:**{incident.get('message')}**")
    if incident.get("exit_code") is not None:
        lines.append(f"- Exit code:{incident['exit_code']}")
    lines.append("")

    diagnostics_section = render_diagnostics_history(incident.get("diagnostics_history") or [])
    if diagnostics_section:
        lines.append(diagnostics_section)

    frames = incident.get("frames") or []
    if frames:
        lines.append("## 堆疊追蹤與原始碼上下文")
        lines.append("")
        for frame in frames:
            lines.append(render_frame(frame, project_root, context_lines))
            lines.append("")

    if incident.get("raw"):
        lines.append("## 原始輸出")
        lines.append("```")
        lines.append(incident["raw"])
        lines.append("```")
        lines.append("")

    escalation = incident.get("escalation")
    if escalation:
        multi_agent = render_multi_agent_analysis(escalation.get("multi_agent_analysis"))
        if multi_agent:
            lines.append(multi_agent)

        lines.append("## AI 分級自主處置紀錄")
        lines.append("")
        lines.append(render_stage("第一層:即時處置 + 根因初判", escalation.get("stage1_immediate")))
        lines.append(render_stage("第二層:伺服器/應用參數調整", escalation.get("stage2_parameter")))
        lines.append(render_stage("第三層:程式碼層級暫時修復(未自動 commit)", escalation.get("stage3_code_fix")))

        resolved = escalation.get("final_resolved")
        lines.append(f"**最終狀態:{'已解決 ✅' if resolved else '尚未解決,需要開發者介入 ⚠️'}**")
        lines.append("")

        if escalation.get("code_diff"):
            lines.append("## 程式碼變更(尚未 commit,請開發者審查)")
            lines.append("```diff")
            lines.append(escalation["code_diff"])
            lines.append("```")
            lines.append("")
    else:
        lines.append("## 給開發者的參考")
        lines.append("")
        lines.append("此事件未啟用 AI 分級自主處置,請依上方堆疊/原始碼上下文自行排查根因。")
        lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Artemis 事件根因分析")
    parser.add_argument("incident_json", type=Path)
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--context-lines", type=int, default=6)
    args = parser.parse_args()

    incident = json.loads(args.incident_json.read_text(encoding="utf-8"))
    report = build_report(incident, args.project_root.resolve(), args.context_lines)

    report_path = args.incident_json.with_suffix(".md")
    report_path.write_text(report, encoding="utf-8")
    print(f"[analyzer] 已產生報告:{report_path}")


if __name__ == "__main__":
    sys.exit(main())
