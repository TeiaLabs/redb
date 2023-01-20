from __future__ import annotations

from pathlib import Path

import setuptools

# BASE_URL = "git+ssh://git@github.com/TeiaLabs/redb.git#subdirectory=src"
BASE_URL = "file://localhost/home/severo/Documents/Projects/redb/src"


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


requirements = [f"redb_interface @ {BASE_URL}/redb-interface"]

opt_requirements = get_optional_requirements()
opt_requirements["json"] = [f"redb_json_system @ {BASE_URL}/redb-json-system"]
opt_requirements["mongo"] = [f"redb_mongo_system @ {BASE_URL}/redb-mongo-system"]
opt_requirements["migo"] = [f"redb_migo_system @ {BASE_URL}/redb-migo-system"]
opt_requirements["all"] = [value[0] for value in opt_requirements.values()]

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
    keywords="database milvus mongo json interface",
    python_requires=">=3.10",
    install_requires=requirements,
    extras_require=opt_requirements,
)
