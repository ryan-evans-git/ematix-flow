"""``flow init`` — scaffold a new project."""
from __future__ import annotations

import pytest

from ematix_flow.cli import main
from ematix_flow.init_scaffold import scaffold_project


class TestScaffoldProject:
    def test_writes_expected_files(self, tmp_path) -> None:
        target = tmp_path / "myproject"
        written = scaffold_project(target)
        names = {p.name for p in written}
        assert names == {
            "pipelines.py",
            "connections.toml",
            "Dockerfile",
            "flow.service",
            ".gitignore",
            "README.md",
        }

    def test_pipelines_py_is_importable(self, tmp_path) -> None:
        target = tmp_path / "p"
        scaffold_project(target)
        content = (target / "pipelines.py").read_text()
        # The scaffold must include a working @pipeline.register stub.
        assert "@pipeline.register" in content
        assert "example_sync" in content

    def test_dockerfile_references_ematix_flow(self, tmp_path) -> None:
        target = tmp_path / "p"
        scaffold_project(target)
        content = (target / "Dockerfile").read_text()
        assert "ematix-flow" in content

    def test_flow_service_has_unit_section(self, tmp_path) -> None:
        target = tmp_path / "p"
        scaffold_project(target)
        content = (target / "flow.service").read_text()
        assert "[Unit]" in content
        assert "[Service]" in content
        assert "[Install]" in content

    def test_refuses_to_overwrite_without_force(self, tmp_path) -> None:
        target = tmp_path / "p"
        scaffold_project(target)
        # Re-running without --force must fail loudly.
        with pytest.raises(FileExistsError):
            scaffold_project(target)

    def test_force_overwrites(self, tmp_path) -> None:
        target = tmp_path / "p"
        scaffold_project(target)
        (target / "pipelines.py").write_text("# user edit")
        # --force restores the scaffold.
        scaffold_project(target, force=True)
        assert "example_sync" in (target / "pipelines.py").read_text()


class TestInitCli:
    def test_init_creates_files(self, tmp_path, capsys) -> None:
        target = tmp_path / "new-project"
        rc = main(["init", str(target)])
        assert rc == 0
        assert (target / "pipelines.py").exists()
        out = capsys.readouterr().out
        assert "scaffolded 6 files" in out
        assert "pipelines.py" in out

    def test_init_refuses_overwrite_exits_1(self, tmp_path, capsys) -> None:
        target = tmp_path / "p"
        target.mkdir()
        (target / "pipelines.py").write_text("# already here")
        rc = main(["init", str(target)])
        assert rc == 1
        assert "already exists" in capsys.readouterr().err

    def test_init_force_overwrites(self, tmp_path) -> None:
        target = tmp_path / "p"
        target.mkdir()
        (target / "pipelines.py").write_text("# stale")
        rc = main(["init", str(target), "--force"])
        assert rc == 0
        assert "example_sync" in (target / "pipelines.py").read_text()
