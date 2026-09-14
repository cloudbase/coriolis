# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""add unique constraints

Revision ID: 025
Revises: 024
Create Date: 2026-08-27 17:21:00.000000
"""

import collections

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision = "025"
down_revision = "024"
branch_labels = None
depends_on = None

# These constraints have been declared on the SQLAlchemy models, but no migration
# script has actually created them in the database.
_CONSTRAINTS = (
    (
        "uniq_task_progress_update0task_id0index0deleted",
        "task_progress_update",
        ["task_id", "index", "deleted"],
    ),
    (
        "uniq_minion_pool_progress_update0pool_id0index0deleted",
        "minion_pool_progress_update",
        ["pool_id", "index", "deleted"],
    ),
    ("uniq_services0host0topic0deleted", "service", ["host", "topic", "deleted"]),
    ("uniq_services0host0binary0deleted", "service", ["host", "binary", "deleted"]),
)


def _existing_constraint_names(inspector, table):
    # On MySQL, unique constraints surface as unique indexes rather than through
    # get_unique_constraints(), so we need to check both.
    names = {c['name'] for c in inspector.get_unique_constraints(table)}
    names.update(i['name'] for i in inspector.get_indexes(table) if i.get('unique'))
    return names


def _deduplicate_rows(bind, table, columns):
    # The constraint we are about to add might be violated by multiple rows representing
    # the same entity (e.g.: a duplicate service). Soft-delete all except the most
    # recent row of each duplicate group, the same way oslo.db already soft-deletes
    # rows (`deleted` set to the row's own id), which keeps them out of the way of the
    # new unique constraint without hard-deleting anything.
    cols_sql = ", ".join("`%s`" % c for c in columns)
    duplicate_groups = bind.execute(
        sqlalchemy.text(
            "SELECT %s FROM `%s` GROUP BY %s HAVING COUNT(*) > 1"
            % (cols_sql, table, cols_sql)
        )
    ).mappings()

    for group in duplicate_groups:
        where_sql = " AND ".join("`%s` = :%s" % (c, c) for c in columns)
        rows = bind.execute(
            sqlalchemy.text(
                "SELECT id FROM `%s` WHERE %s ORDER BY created_at DESC, id DESC"
                % (table, where_sql)
            ),
            dict(group),
        ).fetchall()

        # keep the most recent row (rows[0]) active, soft-delete the rest.
        for row in rows[1:]:
            bind.execute(
                sqlalchemy.text(
                    "UPDATE `%s` SET deleted = :new_deleted, "
                    "deleted_at = NOW() WHERE id = :id" % table
                ),
                {"new_deleted": row.id, "id": row.id},
            )


# Unlike 'service' rows, colliding rows in these tables are not duplicates of the same
# event: 'index' is assigned by reading the current max and adding one, so a collision
# means two distinct progress messages raced for the same index. Soft-deleting one would
# permanently hide real history, so instead we renumber the duplicate row(s) into the
# position right after the row that kept the index, shifting every later row forward to
# make room. This preserves arrival order for the whole sequence.
_RENUMBER_GROUP_COLUMN = {
    "task_progress_update": "task_id",
    "minion_pool_progress_update": "pool_id",
}


def _renumber_duplicate_index_rows(bind, table, group_column, index_column="index"):
    duplicate_groups = (
        bind.execute(
            sqlalchemy.text(
                "SELECT `%s`, `%s`, deleted FROM `%s` "
                "GROUP BY `%s`, `%s`, deleted HAVING COUNT(*) > 1"
                % (group_column, index_column, table, group_column, index_column)
            )
        )
        .mappings()
        .all()
    )

    # group by task / pool, and within each, resolve collisions highest index first.
    groups_by_owner = collections.defaultdict(list)
    for group in duplicate_groups:
        groups_by_owner[group[group_column]].append(group)

    for owner, groups in groups_by_owner.items():
        groups.sort(key=lambda g: g[index_column], reverse=True)
        for group in groups:
            index_value = group[index_column]
            rows = bind.execute(
                sqlalchemy.text(
                    "SELECT id FROM `%s` WHERE `%s` = :%s AND `%s` = :%s "
                    "AND deleted = :deleted ORDER BY created_at ASC, id ASC"
                    % (
                        table,
                        group_column,
                        group_column,
                        index_column,
                        index_column,
                    )
                ),
                dict(group),
            ).fetchall()

            extra_count = len(rows) - 1
            if extra_count <= 0:
                continue

            # shift every row after the collision forward to make room for the extra
            # row(s) that will be inserted right after it.
            bind.execute(
                sqlalchemy.text(
                    "UPDATE `%s` SET `%s` = `%s` + :shift "
                    "WHERE `%s` = :owner AND `%s` > :index_value"
                    % (table, index_column, index_column, group_column, index_column)
                ),
                {"shift": extra_count, "owner": owner, "index_value": index_value},
            )

            # earliest row (rows[0]) keeps the original index; later ones, in arrival
            # order, take the newly freed slots right after it.
            for offset, row in enumerate(rows[1:], start=1):
                bind.execute(
                    sqlalchemy.text(
                        "UPDATE `%s` SET `%s` = :new_index WHERE id = :id"
                        % (table, index_column)
                    ),
                    {"new_index": index_value + offset, "id": row.id},
                )


def upgrade():
    bind = op.get_bind()
    inspector = sqlalchemy.inspect(bind)
    for name, table, columns in _CONSTRAINTS:
        if name in _existing_constraint_names(inspector, table):
            continue

        if table in _RENUMBER_GROUP_COLUMN:
            _renumber_duplicate_index_rows(bind, table, _RENUMBER_GROUP_COLUMN[table])
        else:
            _deduplicate_rows(bind, table, columns)

        op.create_unique_constraint(name, table, columns)


def downgrade():
    raise NotImplementedError()
