from pathlib import Path

from privacy_audit import audit_repository


def test_tracked_tree_has_no_common_secrets_or_mutable_actions():
    assert audit_repository(Path(__file__).parents[1]) == []
