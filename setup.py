"""Build the independent PGSync Studio distribution."""

import re
from pathlib import Path

from setuptools import find_namespace_packages, setup

ROOT = Path(__file__).resolve().parent
version_text = (ROOT / "pgsync/web/__init__.py").read_text(encoding="utf-8")
VERSION = re.search(r'^__version__ = "([^"]+)"$', version_text, re.M).group(1)
requirements = [
    line.strip()
    for line in (ROOT / "requirements/base.txt").read_text(encoding="utf-8").splitlines()
    if line.strip() and not line.lstrip().startswith("#")
]

setup(
    name="pgsync-studio",
    version=VERSION,
    description="A web console and cross-database document sync engine for PostgreSQL and Elasticsearch",
    long_description=(ROOT / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    author="PGSync Studio contributors",
    license="MIT",
    license_files=["LICENSE", "AUTHORS.rst"],
    python_requires=">=3.10",
    packages=find_namespace_packages(
        include=["pgsync", "pgsync.*"],
        exclude=["*.__pycache__", "*.__pycache__.*"],
    ),
    include_package_data=True,
    package_data={"pgsync.web": ["static/*.html", "static/*.css", "static/*.js", "static/*.ico"]},
    install_requires=requirements,
    scripts=["bin/bootstrap", "bin/parallel_sync"],
    entry_points={
        "console_scripts": [
            "pgsync=pgsync.sync:main",
            "pgsync-ui=pgsync.web.cli:main",
        ]
    },
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.12",
    ],
    project_urls={"Upstream PGSync": "https://github.com/toluaina/pgsync"},
    zip_safe=False,
)
