"""Startup must not migrate every plant database when the operator opts out."""

from __future__ import annotations

import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from p0.api.main import _skip_startup_migration

VAR = "P0_SKIP_STARTUP_MIGRATION"


def test_unset_means_migration_runs(monkeypatch):
    monkeypatch.delenv(VAR, raising=False)
    assert _skip_startup_migration() is False


def test_empty_means_migration_runs(monkeypatch):
    monkeypatch.setenv(VAR, "")
    assert _skip_startup_migration() is False


def test_zero_means_migration_runs(monkeypatch):
    monkeypatch.setenv(VAR, "0")
    assert _skip_startup_migration() is False


def test_one_skips_the_migration(monkeypatch):
    monkeypatch.setenv(VAR, "1")
    assert _skip_startup_migration() is True


def test_true_skips_the_migration(monkeypatch):
    monkeypatch.setenv(VAR, "true")
    assert _skip_startup_migration() is True


def test_yes_skips_the_migration(monkeypatch):
    monkeypatch.setenv(VAR, "yes")
    assert _skip_startup_migration() is True


def test_value_is_case_and_space_insensitive(monkeypatch):
    monkeypatch.setenv(VAR, "  TRUE  ")
    assert _skip_startup_migration() is True


def test_an_unrelated_value_does_not_skip(monkeypatch):
    monkeypatch.setenv(VAR, "later")
    assert _skip_startup_migration() is False
