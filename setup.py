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
requirements_teia = read_multiline_as_list("requirements-teia.txt")

opt_requirements = get_optional_requirements()

with open("README.md", "r") as readme_file:
    long_description = readme_file.read()

setuptools.setup(
    name="redb",
    version="0.1.0",
    author="Nei Cardoso de Oliveira Neto",
    author_email="nei.neto@hotmail.com",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/teialabs/redb",
    packages=setuptools.find_packages(where="src"),
    package_dir={"": "src"},
    keywords="",
    python_requires=">=3.10",
    install_requires=requirements + requirements_teia,
    extras_require=opt_requirements,
)
