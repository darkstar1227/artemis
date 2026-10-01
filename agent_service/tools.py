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

import re
import subprocess
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from agents import RunContextWrapper, function_tool

BASH_TIMEOUT_SECS = 120
READ_FILE_MAX_CHARS = 20000
LIST_DIR_MAX_ENTRIES = 500
GREP_MAX_MATCHES = 200
GREP_MAX_CHARS = 8000
REMOTE_EXEC_MAX_CHARS = 8000
_SKIP_DIR_NAMES = {".git", "node_modules", ".venv", "__pycache__", "target", ".mypy_cache"}


@dataclass
class StageContext:
    """Per-run permission scope, threaded through as the Agents SDK run context."""

    cwd: Path
    stage: str
    bash_whitelist: list[str] = field(default_factory=list)
    editable_files: list[str] = field(default_factory=list)  # empty = unrestricted
    forbid_git_commit_push: bool = False

    # SessAnchor (`sanc`) 遠端執行後端,詳見 remote_exec。remote_enabled=False
    # 或 remote_device 未設定時,remote_exec 直接回報錯誤,不會嘗試呼叫 sanc。
    remote_enabled: bool = False
    remote_device: str | None = None
    sanc_bin: str = "sanc"
    remote_state_dir: str | None = None
    remote_timeout_secs: int = 120
    remote_allowed_commands: list[str] = field(default_factory=list)

    # 這個 stage 讀過/改過/grep 過的檔案路徑(只留路徑,不留內容——內容本來就在
    # 這次 run 的對話歷史裡,重複存一份只會多花記憶體不會省 token)。用來組出
    # 交給下一個 stage 的 handoff 摘要(見 pipeline.py::handoff_context),讓後面
    # 的 stage 不用重新探索前一個 stage 已經找過的檔案。
    files_touched: list[str] = field(default_factory=list)

    def note_file_touched(self, path: str) -> None:
        if path not in self.files_touched:
            self.files_touched.append(path)


class PermissionDenied(Exception):
    pass


def _resolve_within_cwd(ctx: StageContext, path: str) -> Path:
    resolved = (ctx.cwd / path).resolve() if not Path(path).is_absolute() else Path(path).resolve()
    try:
        resolved.relative_to(ctx.cwd.resolve())
    except ValueError as e:
        raise PermissionDenied(f"路徑超出專案目錄,拒絕存取:{path}") from e
    return resolved


def _resolve_config_entry(ctx: StageContext, entry: str) -> Path:
    """Resolve a stage3_config_files entry the same way an incoming edit path
    is resolved: relative entries (e.g. "./config/app.toml") are relative to
    the target repo's cwd, not the agent_service process's own cwd; absolute
    entries are used as-is."""
    p = Path(entry)
    return p.resolve() if p.is_absolute() else (ctx.cwd / p).resolve()


def _check_editable(ctx: StageContext, path: str) -> None:
    if not ctx.editable_files:
        return  # unrestricted (stage3)
    resolved = str(_resolve_within_cwd(ctx, path))
    allowed = {str(_resolve_config_entry(ctx, p)) for p in ctx.editable_files}
    if resolved not in allowed:
        raise PermissionDenied(
            f"此階段只能編輯以下檔案:{', '.join(ctx.editable_files)},拒絕存取:{path}"
        )


def _command_in_whitelist(whitelist: list[str], command: str) -> bool:
    if not whitelist:
        return False
    stripped = command.strip()
    for entry in whitelist:
        # 設定檔沿用舊的 Claude CLI "Bash(實際指令)" 語法,這裡把包裝去掉來比對。
        allowed_cmd = entry.strip()
        if allowed_cmd.startswith("Bash(") and allowed_cmd.endswith(")"):
            allowed_cmd = allowed_cmd[len("Bash(") : -1]
        if stripped == allowed_cmd.strip():
            return True
    return False


def _bash_command_allowed(ctx: StageContext, command: str) -> bool:
    return _command_in_whitelist(ctx.bash_whitelist, command)


@function_tool
def read_file(wrapper: RunContextWrapper[StageContext], path: str) -> str:
    """讀取檔案內容。

    Args:
        path: 要讀取的檔案路徑(相對於專案目錄或絕對路徑)。
    """
    ctx = wrapper.context
    resolved = _resolve_within_cwd(ctx, path)
    ctx.note_file_touched(path)
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
def list_dir(wrapper: RunContextWrapper[StageContext], path: str = ".") -> str:
    """列出目錄內容(不遞迴列出子目錄內部,但會標示哪些項目是子目錄)。

    Args:
        path: 要列出的目錄路徑(相對於專案目錄),預設為專案根目錄。
    """
    ctx = wrapper.context
    resolved = _resolve_within_cwd(ctx, path)
    if not resolved.exists() or not resolved.is_dir():
        return f"[錯誤] 目錄不存在:{path}"
    try:
        entries = sorted(resolved.iterdir())
    except OSError as e:
        return f"[錯誤] 無法列出目錄:{e}"
    lines = [f"{e.name}/" if e.is_dir() else e.name for e in entries[:LIST_DIR_MAX_ENTRIES]]
    out = "\n".join(lines) or "(空目錄)"
    if len(entries) > LIST_DIR_MAX_ENTRIES:
        out += f"\n\n[已截斷,目錄共 {len(entries)} 項,只顯示前 {LIST_DIR_MAX_ENTRIES} 項]"
    return out


def _grep_scan(ctx: StageContext, pattern: str, path: str) -> str:
    """Pure implementation behind grep_files, kept separate from the
    @function_tool wrapper so it can be unit-tested directly (the SDK's
    FunctionTool wrapping makes the decorated function itself awkward to
    invoke outside a real agent run)."""
    resolved = _resolve_within_cwd(ctx, path)
    if not resolved.exists():
        return f"[錯誤] 路徑不存在:{path}"
    try:
        regex = re.compile(pattern)
    except re.error as e:
        return f"[錯誤] 正規表示式錯誤:{e}"

    root = ctx.cwd.resolve()
    candidates = [resolved] if resolved.is_file() else sorted(resolved.rglob("*"))
    matches: list[str] = []
    for f in candidates:
        if not f.is_file() or _SKIP_DIR_NAMES & set(f.relative_to(root).parts[:-1]):
            continue
        # f may be reached through a symlink inside the tree that points
        # outside cwd (e.g. a symlinked file or an ancestor symlinked dir) —
        # resolve it and re-check confinement before reading its content.
        try:
            real = f.resolve()
            real.relative_to(root)
        except (OSError, ValueError):
            continue
        try:
            text = real.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for i, line in enumerate(text.splitlines(), start=1):
            if regex.search(line):
                matches.append(f"{f.relative_to(root)}:{i}:{line.strip()}")
                if len(matches) >= GREP_MAX_MATCHES:
                    break
        if len(matches) >= GREP_MAX_MATCHES:
            break

    if not matches:
        return "(無符合結果)"
    out = "\n".join(matches)[:GREP_MAX_CHARS]
    if len(matches) >= GREP_MAX_MATCHES:
        out += f"\n\n[已截斷,已達最多 {GREP_MAX_MATCHES} 筆符合結果上限]"
    return out


@function_tool
def grep_files(wrapper: RunContextWrapper[StageContext], pattern: str, path: str = ".") -> str:
    """在專案目錄下搜尋符合正規表示式的檔案內容(類似 grep -rn),協助在不知道確切檔名時定位相關程式碼。

    Args:
        pattern: 要搜尋的正規表示式。
        path: 搜尋範圍,可以是單一檔案或目錄(相對於專案目錄),預設為專案根目錄。
    """
    wrapper.context.note_file_touched(path)
    return _grep_scan(wrapper.context, pattern, path)


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
    ctx.note_file_touched(path)
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
    ctx.note_file_touched(path)
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


def _sanc(ctx: StageContext, *args: str) -> subprocess.CompletedProcess:
    cmd = [ctx.sanc_bin]
    if ctx.remote_state_dir:
        cmd += ["--state-dir", ctx.remote_state_dir]
    cmd += list(args)
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=ctx.remote_timeout_secs
    )


@function_tool
def remote_exec(wrapper: RunContextWrapper[StageContext], command: str) -> str:
    """在設定好的遠端主機上執行指令,透過 SessAnchor(`sanc`)。每次呼叫都會帶一個新的
    request_id,執行紀錄(指令、輸出)之後任何人都可以用同一個 session/request_id 透過
    `sanc output`/`sanc task` 查回來,不需要重新測試 SSH 是否能連線 — 交接時可以直接看
    這個 session 之前跑過什麼。注意:目前的 sanc 版本還不保證連線中斷後遠端任務會繼續
    (`sanc exec` 自己的說明是「No remote persistence on SSH loss yet」),這裡只是把
    「這台主機之前執行過什麼」留下可查的紀錄,不是斷線續傳。

    Args:
        command: 要在遠端主機執行的 shell 指令。
    """
    ctx = wrapper.context
    if not ctx.remote_enabled or not ctx.remote_device:
        return "[錯誤] 此事件未啟用/設定遠端主機(remote.enabled / remote.device_id),無法執行 remote_exec"

    if ctx.stage == "stage1_immediate":
        if not _command_in_whitelist(ctx.remote_allowed_commands, command):
            return f"[權限拒絕] 此階段只能對遠端主機執行白名單內的指令,拒絕:{command}"
    elif ctx.forbid_git_commit_push and ("git commit" in command or "git push" in command):
        return f"[權限拒絕] 此階段禁止 git commit / git push:{command}"

    session_id = f"artemis-{ctx.remote_device}"
    request_id = f"{ctx.stage}-{uuid.uuid4().hex[:12]}"

    try:
        # session create 若該 session 已存在會回傳非 0(prototype 階段沒有明確的
        # "already exists" 訊息可比對),因此這裡採用「盡量建立,失敗就假設已存在並
        # 繼續往下執行」的寬鬆策略,和 agent_client::escalate 對 agent_service
        # 連不到時的 fire-and-forget/degrade-gracefully 風格一致。
        _sanc(ctx, "session", "create", "--device", ctx.remote_device, session_id)
    except (OSError, subprocess.TimeoutExpired) as e:
        return f"[錯誤] 無法呼叫 sanc(檢查 sanc 是否已安裝並在 PATH 上):{e}"

    try:
        result = _sanc(
            ctx, "exec", "--request-id", request_id, "--command", command, session_id
        )
    except subprocess.TimeoutExpired:
        return (
            f"[錯誤] 遠端指令逾時({ctx.remote_timeout_secs}s):{command}"
            f"(session={session_id}, request_id={request_id})"
        )
    except OSError as e:
        return f"[錯誤] 無法呼叫 sanc:{e}"

    out = (
        f"device={ctx.remote_device} session={session_id} request_id={request_id}\n"
        f"exit_code={result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}\n\n"
        f"[之後可用 `sanc task {request_id}` / `sanc output {request_id}` 查回這次執行的狀態與輸出]"
    )
    return out[:REMOTE_EXEC_MAX_CHARS]
