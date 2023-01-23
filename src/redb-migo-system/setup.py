from __future__ import annotations

from pathlib import Path

import setuptools


def read_multiline_as_list(file_path: Path | str) -> list[str]:
    with open(file_path) as req_file:
        contents = req_file.read().split("\n")
        if contents[-1] == "":
            contents.pop()
        return contents


requirements_teia = read_multiline_as_list("requirements-teia.txt")

setuptools.setup(
    name="redb_migo_system",
    version="1.0.0",
    packages=setuptools.find_namespace_packages(),
    python_requires="<=3.10.9",
    install_requires=requirements_teia,
)
