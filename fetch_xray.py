"""Build-time fetch of one official, hash-pinned Xray executable."""
import hashlib
import io
import os
import sys
import urllib.request
import zipfile

VERSION = "v26.3.27"
URL = "https://github.com/XTLS/Xray-core/releases/download/v26.3.27/Xray-linux-64.zip"
SHA256 = "23cd9af937744d97776ee35ecad4972cf4b2109d1e0fe6be9930467608f7c8ae"


def install(destination):
    with urllib.request.urlopen(URL, timeout=90) as response:
        archive = response.read(32 * 1024 * 1024 + 1)
    if len(archive) > 32 * 1024 * 1024:
        raise RuntimeError("Official release archive exceeds size limit")
    if hashlib.sha256(archive).hexdigest() != SHA256:
        raise RuntimeError("Official release archive digest mismatch")
    with zipfile.ZipFile(io.BytesIO(archive)) as z:
        executable = z.read("xray")
    if not executable.startswith(b"\x7fELF"):
        raise RuntimeError("Release does not contain the expected Linux executable")
    with open(destination, "xb") as f:
        f.write(executable)
    os.chmod(destination, 0o555)
    print("Installed official Xray " + VERSION + "; release SHA256 verified")


if __name__ == "__main__":
    install(sys.argv[1])
