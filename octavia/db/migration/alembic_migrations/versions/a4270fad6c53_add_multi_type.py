# Copyright 2020 Yovole
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#

"""add multi type field
Revision ID: a4270fad6c53
Revises: 6ac558d7fc21
Create Date: 2022-06-10 10:53:55.762631
"""

# revision identifiers, used by Alembic.
revision = 'a4270fad6c53'
down_revision = '6ac558d7fc21'

from alembic import op
import sqlalchemy as sa
from sqlalchemy import sql


def upgrade():

    insert_table = sql.table(
        u'lb_topology',
        sql.column(u'name', sa.String),
        sql.column(u'description', sa.String)
    )

    op.bulk_insert(
        insert_table,
        [
            {'name': 'MULTI_ACTIVE'},
        ]
    )

    insert_table = sql.table(
        u'amphora_roles',
        sql.column(u'name', sa.String),
        sql.column(u'description', sa.String)
    )

    op.bulk_insert(
        insert_table,
        [
            {'name': 'MULTI'}
        ]
    )
