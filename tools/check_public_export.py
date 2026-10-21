"""Check a public source tree without printing potentially sensitive matches."""

import ipaddress
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {
    ".git", ".venv", "venv", "__pycache__", ".pytest_cache", ".eggs",
    "build", "dist", "runtime", "backups", "htmlcov",
}
PATTERNS = {
    "private-key": re.compile(r"-----BEGIN (?:[A-Z0-9]+ )?PRIVATE KEY-----"),
    "aws-access-key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "cloud-access-key": re.compile(r"\bLTAI[A-Za-z0-9]{16,}\b"),
    "github-token": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{24,}|github_pat_[A-Za-z0-9_]{24,})\b"),
    "cloud-instance-host": re.compile(r"\b(?:pgr|pgm|rm|rds)-[a-z0-9-]+\.[a-z0-9.-]+\.aliyuncs\.com\b", re.I),
    "personal-windows-path": re.compile(r"[A-Z]:[\\/]Users[\\/][^\s/\\]+", re.I),
}
IPV4 = re.compile(r"(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w.])")
PRIVATE_NETWORKS = [ipaddress.ip_network(v) for v in ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]]


def scan(root=ROOT):
    findings = []
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root)
        if any(part in SKIP_DIRS or part.endswith(".egg-info") for part in rel.parts):
            continue
        if not path.is_file():
            continue
        if path.name == ".env" or path.suffix.lower() in {".pem", ".key", ".p12", ".pfx"}:
            findings.append((str(rel), 0, "credential-file"))
            continue
        raw = path.read_bytes()
        if b"\x00" in raw:
            continue
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        for number, line in enumerate(text.splitlines(), 1):
            for label, pattern in PATTERNS.items():
                if pattern.search(line):
                    findings.append((str(rel), number, label))
            # Synthetic fixture addresses and this policy's CIDRs are permitted.
            if rel.parts[0] not in {"tests", "tools"}:
                for candidate in IPV4.findall(line):
                    try:
                        addr = ipaddress.ip_address(candidate)
                    except ValueError:
                        continue
                    if any(addr in network for network in PRIVATE_NETWORKS):
                        findings.append((str(rel), number, "private-network-address"))
    return findings


if __name__ == "__main__":
    findings = scan()
    for path, line, label in findings:
        print(f"{path}:{line}: {label}")
    if findings:
        raise SystemExit(f"Public-export check failed: {len(findings)} finding(s). Values were redacted.")
    print("Public-export check passed. Review all changes before publishing.")
