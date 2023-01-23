from __future__ import annotations

import os
from pathlib import Path

import setuptools

if os.environ.get("REDB_LOCAL"):
    # For local development only
    root = Path(__file__).parent.absolute().as_posix()
    BASE_URL = f"file://localhost/{root}/src"
else:
    BASE_URL = "git+ssh://git@github.com/TeiaLabs/redb.git#subdirectory=src"

systems = {
    "json": f"redb_json_system @ {BASE_URL}/redb-json-system",
    "mongo": f"redb_mongo_system @ {BASE_URL}/redb-mongo-system",
    "migo": f"redb_migo_system @ {BASE_URL}/redb-migo-system",
}


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
requirements.append(f"redb_interface @ {BASE_URL}/redb-interface")

opt_requirements = get_optional_requirements()
opt_requirements["schema"] = [f"redb_teia_schema @ {BASE_URL}/redb-teia-schema"]
opt_requirements["json"] = [systems["json"]]
opt_requirements["mongo"] = [systems["mongo"]]
opt_requirements["migo"] = [systems["migo"]]
opt_requirements["systems"] = list(systems.values())
opt_requirements["all"] = [
    value
    for key, values in opt_requirements.items()
    if key != "systems"
    for value in values
]

with open("README.md", "r") as readme_file:
    long_description = readme_file.read()

setuptools.setup(
    name="redb",
    version="1.0.0",
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
