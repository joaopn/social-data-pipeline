"""Shared fixture for the control-boundary tests: a temp config/sources/ tree, so no test reads the real one."""

import pytest

from social_data_pipeline.setup import utils


@pytest.fixture
def source_tree(tmp_path, monkeypatch):
    """A temp project root that `utils.SOURCES_DIR` points into.

    Layout under config/sources/: ok/ (a source with platform.yaml), bare/ (a folder without platform.yaml), a/b/ (a nested folder), linked (a symlink to a real source folder outside config/sources/), plainfile (a file). Next to sources/: config/x/ with a platform.yaml, which is what `../x` would address.
    """
    config_dir = tmp_path / "config"
    sources_dir = config_dir / "sources"
    outside = tmp_path / "elsewhere" / "linked-src"
    for folder in (sources_dir / "ok", sources_dir / "a" / "b", config_dir / "x", outside):
        folder.mkdir(parents=True)
        (folder / "platform.yaml").write_text("data_types: [posts]\n")
    (sources_dir / "bare").mkdir()
    (sources_dir / "plainfile").write_text("")
    (sources_dir / "linked").symlink_to(outside, target_is_directory=True)

    monkeypatch.setattr(utils, "ROOT", tmp_path)
    monkeypatch.setattr(utils, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(utils, "SOURCES_DIR", sources_dir)
    return tmp_path
