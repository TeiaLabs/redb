from __future__ import annotations

import shlex
import subprocess
from pathlib import Path

import setuptools


def read_multiline_as_list(file_path: Path | str) -> list[str]:
    with open(file_path) as req_file:
        contents = req_file.read().split("\n")
        if contents[-1] == "":
            contents.pop()
        return contents


def get_optional_requirements() -> dict[str, list[str]]:
    """Get dict of suffix -> list of requirements."""
    requirements_files = Path(".").glob(r"requirements-*.txt")
    requirements = {
        p.stem.split("-")[-1]: read_multiline_as_list(p) for p in requirements_files
    }
    return requirements


def get_version() -> str:
    raw_git_cmd = "git describe --tags"
    git_cmd = shlex.split(raw_git_cmd)
    fmt_cmd = shlex.split("cut -d '-' -f 1,2")
    git = subprocess.Popen(git_cmd, stdout=subprocess.PIPE)
    cut = subprocess.check_output(fmt_cmd, stdin=git.stdout)
    ret_code = git.wait()
    assert ret_code == 0, f"{raw_git_cmd!r} failed with exit code {ret_code}."
    return cut.decode().strip()


requirements = read_multiline_as_list("requirements.txt")

opt_requirements = get_optional_requirements()
opt_requirements["all"] = [
    value
    for key, values in opt_requirements.items()
    if key != "systems"
    for value in values
]

with open("README.md") as f:
    long_description = f.read()

setuptools.setup(
    name="redb-odm",
    version=get_version(),
    author="Teia Labs",
    author_email="contato@teialabs.com",
    description="A python ODM for JSON, Mongo, and Mongo+Milvus.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/teialabs/redb",
    packages=setuptools.find_namespace_packages(exclude=["tests"]),
    keywords="database milvus mongo json interface",
    python_requires=">=3.10",
    install_requires=requirements,
    extras_require=opt_requirements,
)
