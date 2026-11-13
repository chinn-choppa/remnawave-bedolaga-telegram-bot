"""Unreleased split migrations must extend the already published schema."""

import ast
from pathlib import Path


def test_split_migrations_follow_released_renewal_revision():
    root = Path(__file__).resolve().parents[2] / 'migrations/alembic/versions'
    revisions = {}
    split = set()
    for path in root.glob('*.py'):
        values = {}
        for node in ast.parse(path.read_text()).body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id in {'revision', 'down_revision'}:
                        values[target.id] = ast.literal_eval(node.value)
        if 'revision' not in values:
            continue
        revision = values['revision']
        assert revision not in revisions, f'Duplicate revision {revision}: {path.name}'
        revisions[revision] = values['down_revision']
        if '_bs_' in path.name:
            split.add(revision)
    assert revisions['0141'] == '0140'
    assert len(split) == 24
    for revision in split:
        visited = set()
        while revision != '0141':
            assert revision not in visited, 'Migration cycle'
            visited.add(revision)
            assert revision in split, 'Split chain bypasses released renewal migration'
            revision = revisions[revision]
