"""Self-built file/bash tools + the permission whitelisting that replaces
what Claude Code CLI's --allowedTools/--disallowedTools/--permission-mode
used to provide for free.

Each stage gets its OWN Agent instance built with only the tool functions
appropriate for that stage (see agents_def.py) — the permission model here
is "don't hand the agent a tool it shouldn't have" plus in-tool guards for
the cases that need finer-grained scoping than "have it or not"
(stage1's bash whitelist, stage2's config-file whitelist, stage3's git
commit/push ban).
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from agents import RunContextWrapper, function_tool

BASH_TIMEOUT_SECS = 120
READ_FILE_MAX_CHARS = 20000


@dataclass
class StageContext:
    """Per-run permission scope, threaded through as the Agents SDK run context."""

    cwd: Path
    stage: str
    bash_whitelist: list[str] = field(default_factory=list)
    editable_files: list[str] = field(default_factory=list)  # empty = unrestricted
    forbid_git_commit_push: bool = False


class PermissionDenied(Exception):
    pass


def _resolve_within_cwd(ctx: StageContext, path: str) -> Path:
    resolved = (ctx.cwd / path).resolve() if not Path(path).is_absolute() else Path(path).resolve()
    try:
        resolved.relative_to(ctx.cwd.resolve())
    except ValueError as e:
        raise PermissionDenied(f"路徑超出專案目錄,拒絕存取:{path}") from e
    return resolved


def _check_editable(ctx: StageContext, path: str) -> None:
    if not ctx.editable_files:
        return  # unrestricted (stage3)
    resolved = str(_resolve_within_cwd(ctx, path))
    allowed = {str(Path(p).resolve()) for p in ctx.editable_files}
    if resolved not in allowed:
        raise PermissionDenied(
            f"此階段只能編輯以下檔案:{', '.join(ctx.editable_files)},拒絕存取:{path}"
        )


def _bash_command_allowed(ctx: StageContext, command: str) -> bool:
    if not ctx.bash_whitelist:
        return False
    stripped = command.strip()
    for entry in ctx.bash_whitelist:
        # 設定檔沿用舊的 Claude CLI "Bash(實際指令)" 語法,這裡把包裝去掉來比對。
        allowed_cmd = entry.strip()
        if allowed_cmd.startswith("Bash(") and allowed_cmd.endswith(")"):
            allowed_cmd = allowed_cmd[len("Bash(") : -1]
        if stripped == allowed_cmd.strip():
            return True
    return False


@function_tool
def read_file(wrapper: RunContextWrapper[StageContext], path: str) -> str:
    """讀取檔案內容。

    Args:
        path: 要讀取的檔案路徑(相對於專案目錄或絕對路徑)。
    """
    ctx = wrapper.context
    resolved = _resolve_within_cwd(ctx, path)
    if not resolved.exists():
        return f"[錯誤] 檔案不存在:{path}"
    try:
        content = resolved.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        return f"[錯誤] 無法讀取檔案:{e}"
    if len(content) > READ_FILE_MAX_CHARS:
        truncated = content[:READ_FILE_MAX_CHARS]
        return (
            f"{truncated}\n\n[已截斷,檔案共 {len(content)} 字元,"
            f"只顯示前 {READ_FILE_MAX_CHARS} 字元]"
        )
    return content


@function_tool
def edit_file(wrapper: RunContextWrapper[StageContext], path: str, old_text: str, new_text: str) -> str:
    """對檔案做精確的字串取代(old_text 必須在檔案中唯一出現)。

    Args:
        path: 要編輯的檔案路徑。
        old_text: 要被取代的原始文字片段。
        new_text: 取代後的新文字。
    """
    ctx = wrapper.context
    try:
        _check_editable(ctx, path)
    except PermissionDenied as e:
        return f"[權限拒絕] {e}"
    resolved = _resolve_within_cwd(ctx, path)
    if not resolved.exists():
        return f"[錯誤] 檔案不存在:{path}"
    content = resolved.read_text(encoding="utf-8", errors="replace")
    count = content.count(old_text)
    if count == 0:
        return "[錯誤] old_text 在檔案中找不到,未做任何修改"
    if count > 1:
        return f"[錯誤] old_text 在檔案中出現 {count} 次,不是唯一匹配,請提供更精確的片段"
    resolved.write_text(content.replace(old_text, new_text, 1), encoding="utf-8")
    return f"已修改 {path}"


@function_tool
def write_file(wrapper: RunContextWrapper[StageContext], path: str, content: str) -> str:
    """建立新檔案或完整覆寫既有檔案。

    Args:
        path: 目標檔案路徑。
        content: 完整的檔案內容。
    """
    ctx = wrapper.context
    try:
        _check_editable(ctx, path)
    except PermissionDenied as e:
        return f"[權限拒絕] {e}"
    resolved = _resolve_within_cwd(ctx, path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(content, encoding="utf-8")
    return f"已寫入 {path}"


@function_tool
def run_bash(wrapper: RunContextWrapper[StageContext], command: str) -> str:
    """在專案目錄下執行一個 shell 指令。

    Args:
        command: 要執行的 shell 指令。
    """
    ctx = wrapper.context

    if ctx.stage == "stage1_immediate":
        if not _bash_command_allowed(ctx, command):
            return f"[權限拒絕] 此階段只能執行白名單內的指令,拒絕:{command}"
    elif ctx.forbid_git_commit_push and ("git commit" in command or "git push" in command):
        return f"[權限拒絕] 此階段禁止 git commit / git push:{command}"

    try:
        result = subprocess.run(
            command,
            shell=True,
            cwd=ctx.cwd,
            capture_output=True,
            text=True,
            timeout=BASH_TIMEOUT_SECS,
        )
    except subprocess.TimeoutExpired:
        return f"[錯誤] 指令逾時({BASH_TIMEOUT_SECS}s):{command}"

    out = f"exit_code={result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    return out[:8000]
