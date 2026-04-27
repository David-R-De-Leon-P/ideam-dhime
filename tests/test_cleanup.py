# -*- coding: utf-8 -*-
"""Tests para limpieza temporal pendiente sin abrir Selenium."""

from __future__ import annotations

from ideam_dhime.session import cleanup_pending_file_for, sweep_failed_cleanup_dirs


def test_cleanup_pending_file_is_isolated_by_pid(tmp_path):
    assert cleanup_pending_file_for(tmp_path, pid=100).name == "cleanup_failed_dirs_100.txt"
    assert cleanup_pending_file_for(tmp_path, pid=200).name == "cleanup_failed_dirs_200.txt"


def test_sweep_failed_cleanup_dirs_removes_pending_dirs_and_files(tmp_path):
    pending_a = tmp_path / "chunk_a"
    pending_b = tmp_path / "chunk_b"
    pending_a.mkdir()
    pending_b.mkdir()
    file_a = cleanup_pending_file_for(tmp_path, pid=100)
    file_b = cleanup_pending_file_for(tmp_path, pid=200)
    file_a.write_text(f"{pending_a}\n", encoding="utf-8")
    file_b.write_text(f"{pending_b}\n", encoding="utf-8")

    still_pending = sweep_failed_cleanup_dirs(tmp_path)

    assert still_pending == set()
    assert not pending_a.exists()
    assert not pending_b.exists()
    assert not file_a.exists()
    assert not file_b.exists()
