"""
Stage a dbt project for deployment to dbt Projects on Snowflake.

A deployed dbt project object is an immutable versioned snapshot. Snowflake reads only
the files inside the deployed folder, and it does not support a local package outside
the project root. The test projects declare the package under test as `- local: ../../`,
which points at the repository root, so they cannot be deployed as they stand.

This module builds a deployable copy:

1. Copy the dbt project into a stage directory.
2. Copy the repository root into `<stage>/local_packages/dbt_constraints/`.
3. Rewrite `packages.yml` to read `- local: local_packages/dbt_constraints`.
4. Run `dbt deps` in the stage, so `dbt_packages/` ships inside the deployed object.

Step 4 matters for two reasons. Snowflake needs an external access integration to reach
hub.getdbt.com, and a staged `dbt_packages/` folder removes that need. It also stops the
implicit `dbt deps` that Fusion runs when `packages.yml` lists dependencies and no
`dbt_packages/` folder is present, which would fail with a network error.

The stage always tests the current working tree, not a published release.
"""

import shutil
import subprocess
from pathlib import Path

# The folder name inside the stage that holds the package under test.
LOCAL_PACKAGES_DIR = "local_packages"
PACKAGE_NAME = "dbt_constraints"

# The `- local:` entry that the test projects declare. Snowflake rejects a path that
# leaves the project root, so the stage replaces this exact line.
LOCAL_PACKAGE_SOURCE = "- local: ../../"
LOCAL_PACKAGE_STAGED = f"- local: {LOCAL_PACKAGES_DIR}/{PACKAGE_NAME}"

# Generated directories. dbt rebuilds each one, and a deployed object counts every file
# against its 100,000 file limit.
GENERATED_DIRS = (
    "target",
    "logs",
    "dbt_packages",
    "dbt_internal_packages",
    "dbt_modules",
    "__pycache__",
)

# Directories to leave out of the package copy.
#
# `integration_tests` is the critical entry. The stage lives inside it, so copying it
# would recurse. Leaving it out also keeps the file count small: Snowflake only needs
# the macros, the dbt_project.yml, and the license of the package.
PACKAGE_EXCLUDE_DIRS = GENERATED_DIRS + (
    ".git",
    ".github",
    ".venvs",
    ".venv",
    ".vscode",
    ".cursor",
    ".snowflake",
    ".pytest_cache",
    ".ruff_cache",
    "integration_tests",
    "dist",
    "build",
)

# Files to leave out of both copies.
#   .env               holds live credentials and must never reach a deployed object
#   package-lock.yml   pins the old `- local: ../../` entry; `dbt deps` writes a new one
#   .user.yml          a local dbt anonymous id, of no use in the object
#   profiles.yml       dbt Projects on Snowflake prefers dbt_projects_profiles.yml when
#                      both are present, but the local profiles.yml calls env_var() with
#                      no default for the account, role, database and warehouse. Leaving
#                      it out removes any chance that a run reads it and fails.
EXCLUDE_FILES = (".env", "package-lock.yml", ".user.yml", "profiles.yml")

# Directories that `dbt deps` installs but a run never reads. Each hub package ships its
# own test suite. dbt_utils alone contributes over 150 files, including a directory named
# .env that holds its CI templates. Pruning them keeps the deployed object small and
# leaves no file whose name suggests it holds credentials.
PACKAGE_PRUNE_DIRS = ("integration_tests", ".github", ".circleci")


def _ignore(*extra_dirs: str):
    """Return a shutil.copytree ignore callable that drops generated content."""
    skip_dirs = set(GENERATED_DIRS) | set(extra_dirs)

    def _ignore_names(directory: str, names: list[str]) -> set[str]:
        drop = set()
        for name in names:
            if name in skip_dirs or name in EXCLUDE_FILES:
                drop.add(name)
        return drop

    return _ignore_names


def rewrite_packages_yml(packages_yml: Path) -> None:
    """
    Point the local package entry at the staged copy.

    Raise RuntimeError if the expected entry is absent. A silent no-op here would
    deploy a project whose package resolves to nothing, and the tests would then
    report missing constraints as a package bug.
    """
    text = packages_yml.read_text()
    if LOCAL_PACKAGE_SOURCE not in text:
        if LOCAL_PACKAGE_STAGED in text:
            return
        raise RuntimeError(
            f"{packages_yml} does not contain '{LOCAL_PACKAGE_SOURCE}'. "
            "The staging step cannot find the package under test."
        )
    packages_yml.write_text(text.replace(LOCAL_PACKAGE_SOURCE, LOCAL_PACKAGE_STAGED))


def stage_project(
    project_dir: Path,
    package_root: Path,
    stage_dir: Path,
    dbt_bin: Path,
) -> Path:
    """
    Build a deployable copy of one dbt project and return the stage directory.

    Args:
        project_dir: The dbt project to deploy, for example integration_tests/dbt-core.
        package_root: The repository root, which holds the package under test.
        stage_dir: The output directory. This function removes and recreates it.
        dbt_bin: A dbt executable on the host, used only to run `dbt deps`.

    Raise RuntimeError if the rewrite or `dbt deps` fails.
    """
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    stage_dir.parent.mkdir(parents=True, exist_ok=True)

    shutil.copytree(project_dir, stage_dir, ignore=_ignore())

    package_dest = stage_dir / LOCAL_PACKAGES_DIR / PACKAGE_NAME
    shutil.copytree(package_root, package_dest, ignore=_ignore(*PACKAGE_EXCLUDE_DIRS))

    rewrite_packages_yml(stage_dir / "packages.yml")

    deps = subprocess.run(
        [str(dbt_bin), "deps"],
        cwd=str(stage_dir),
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    if deps.returncode != 0:
        raise RuntimeError(
            f"dbt deps failed in the stage at {stage_dir}\n{deps.stdout}\n{deps.stderr}"
        )

    staged_package = stage_dir / "dbt_packages" / PACKAGE_NAME
    if not staged_package.exists():
        raise RuntimeError(
            f"dbt deps did not install {PACKAGE_NAME} into {stage_dir / 'dbt_packages'}. "
            "The deployed object would not contain the package under test."
        )

    prune_installed_packages(stage_dir / "dbt_packages")

    return stage_dir


def prune_installed_packages(dbt_packages: Path) -> int:
    """
    Remove the test suites that `dbt deps` installed. Return the directory count removed.

    `dbt deps` runs after the copy, so the copytree filter cannot reach these files.
    """
    removed = 0
    for package in sorted(dbt_packages.iterdir()):
        if not package.is_dir():
            continue
        for name in PACKAGE_PRUNE_DIRS:
            unwanted = package / name
            if unwanted.is_dir():
                shutil.rmtree(unwanted)
                removed += 1
    return removed


def count_files(directory: Path) -> int:
    """Return the number of files under a directory, for the object file limit."""
    return sum(1 for path in directory.rglob("*") if path.is_file())
