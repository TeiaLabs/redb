from __future__ import annotations

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
    version="1.1.0",
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
