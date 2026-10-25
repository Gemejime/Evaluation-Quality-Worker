"""Application entrypoints package.

Provides compatibility aliases from Python standard library cmd to allow
tools like pdb to function transparently alongside the cmd/ project structure.
"""

import importlib.util
import sys
import sysconfig
from pathlib import Path

_stdlib_cmd_file = Path(sysconfig.get_path("stdlib")) / "cmd.py"
if not _stdlib_cmd_file.exists():
    _stdlib_cmd_file = Path(sys.base_prefix) / "Lib" / "cmd.py"
if not _stdlib_cmd_file.exists():
    _stdlib_cmd_file = Path(sys.prefix) / "Lib" / "cmd.py"

if _stdlib_cmd_file.exists():
    _spec = importlib.util.spec_from_file_location("_stdlib_cmd", str(_stdlib_cmd_file))
    if _spec and _spec.loader:
        _stdlib_cmd = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_stdlib_cmd)
        Cmd = getattr(_stdlib_cmd, "Cmd", None)
        IDENTCHARS = getattr(_stdlib_cmd, "IDENTCHARS", None)
        PROMPT = getattr(_stdlib_cmd, "PROMPT", None)
