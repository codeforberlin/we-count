#!/usr/bin/env python3
# Copyright (c) 2024-2026 Berlin zählt Mobilität
# SPDX-License-Identifier: MIT

# @file    scenarios.py
# @author  Egbert Klaassen
# @date    2026-09-27

"""Named dashboard scenarios.

A scenario is a snapshot of the dashboard controls stored under a name, so a
colleague can reopen exactly the view somebody else was looking at.  Only the
scenario id travels in the URL (``?scenario=<id>``); the control values live in
a small SQLite database of their own.

The database is deliberately not the DuckDB file the dashboard queries, because
:func:`we_count.frontend.app.retrieve_data` deletes and rebuilds that one on
every start.  Nothing here knows about Dash components either: a stored state
is re-validated by the app before it seeds the layout, so a scenario can only
ever become component values and never reaches a query as text.

Every call opens its own connection.  Dash serves callbacks on a thread pool,
and a short lived connection avoids sharing one cursor between requests the way
the DuckDB connection is shared.
"""
import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime

#: Bumped whenever the meaning of a stored state changes.  A state written by
#: another version is ignored rather than guessed at, so an old link can never
#: half-apply a layout it no longer describes.
STATE_VERSION = 1

#: Database file inside the data directory, overridable with ``BZM_SCENARIO_DB``
#: so a test or a second dashboard can keep its scenarios apart.
DEFAULT_DB_NAME = 'scenarios.db'
DB_ENV_VAR = 'BZM_SCENARIO_DB'

#: Names and author notes are free text from the user, so they are truncated
#: before they reach the database rather than rejected.
MAX_NAME_LENGTH = 80
MAX_SAVED_BY_LENGTH = 40
TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M'


def database_path(data_dir):
    """Return the scenario database to use for ``data_dir``."""
    return os.environ.get(DB_ENV_VAR) or os.path.join(data_dir, DEFAULT_DB_NAME)


def connect(data_dir):
    """Open the scenario database, creating the table on first use."""
    path = database_path(data_dir)
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    connection = sqlite3.connect(path, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("""
        CREATE TABLE IF NOT EXISTS scenarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            state TEXT NOT NULL,
            saved_by TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
    """)
    return connection


def _fetch_one(data_dir, query, params):
    with closing(connect(data_dir)) as connection:
        row = connection.execute(query, params).fetchone()
    return dict(row) if row else None


def _decode(scenario):
    """Add the parsed state to ``scenario``, or drop it if it is unreadable."""
    if scenario is None:
        return None
    try:
        scenario['state'] = json.loads(scenario['state'])
    except (json.JSONDecodeError, TypeError):
        return None
    return scenario


def list_scenarios(data_dir):
    """Return every scenario, ordered by name, without their states."""
    with closing(connect(data_dir)) as connection:
        rows = connection.execute(
            'SELECT id, name, saved_by, updated_at FROM scenarios ORDER BY name COLLATE NOCASE'
        ).fetchall()
    return [dict(row) for row in rows]


def get_scenario(data_dir, scenario_id):
    """Return one scenario with its state decoded, or ``None`` if unknown."""
    if scenario_id is None:
        return None
    return _decode(_fetch_one(data_dir, 'SELECT * FROM scenarios WHERE id = ?', (scenario_id,)))


def find_by_name(data_dir, name):
    """Return the scenario called ``name``, or ``None``."""
    return _decode(_fetch_one(data_dir, 'SELECT * FROM scenarios WHERE name = ?', (name,)))


def save_scenario(data_dir, name, state, saved_by='', scenario_id=None):
    """Insert or update a scenario and return it with its state decoded.

    ``scenario_id`` of ``None`` creates a new scenario, anything else overwrites
    that one and keeps its id, so links to it stay valid.  Raises ``ValueError``
    for an empty name and for an id that no longer exists, and
    ``sqlite3.IntegrityError`` if the name is taken.
    """
    name = (name or '').strip()[:MAX_NAME_LENGTH]
    if not name:
        raise ValueError('a scenario needs a name')
    saved_by = (saved_by or '').strip()[:MAX_SAVED_BY_LENGTH]
    payload = json.dumps(state, sort_keys=True)
    stamp = datetime.now().strftime(TIMESTAMP_FORMAT)

    with closing(connect(data_dir)) as connection:
        with connection:
            if scenario_id is None:
                cursor = connection.execute(
                    'INSERT INTO scenarios (name, state, saved_by, updated_at) VALUES (?, ?, ?, ?)',
                    (name, payload, saved_by, stamp))
                scenario_id = cursor.lastrowid
            else:
                connection.execute(
                    'UPDATE scenarios SET name = ?, state = ?, saved_by = ?, updated_at = ?'
                    ' WHERE id = ?', (name, payload, saved_by, stamp, scenario_id))
        row = connection.execute('SELECT * FROM scenarios WHERE id = ?', (scenario_id,)).fetchone()
        if row is None:
            # Somebody deleted the scenario from another tab in the meantime
            raise ValueError(f'no scenario with id {scenario_id}')

    return _decode(dict(row))


def delete_scenario(data_dir, scenario_id):
    """Remove one scenario and report whether it was there to remove."""
    with closing(connect(data_dir)) as connection:
        with connection:
            cursor = connection.execute('DELETE FROM scenarios WHERE id = ?', (scenario_id,))
    return cursor.rowcount > 0
