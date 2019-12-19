# coding: utf-8
"""
MySQL database backend for Django with connection pool support.

"""
import re

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import utils
from django.db.backends import utils as backend_utils
from django.db.backends.base.base import BaseDatabaseWrapper
from django.utils.functional import cached_property

try:
    import MySQLdb as Database
    import sqlalchemy.pool as pool
    from sqlalchemy.pool import QueuePool
    from sqlalchemy.exc import TimeoutError
    Database = pool.manage(Database, poolclass=QueuePool, **settings.SQLALCHEMY_QUEUEPOOL)
except ImportError as err:
    raise ImproperlyConfigured(
        'Error loading MySQLdb module.\n'
        'Did you install mysqlclient and SQLAlchemy?'
    ) from err

from MySQLdb.constants import CLIENT, FIELD_TYPE                # isort:skip
from MySQLdb.converters import conversions                      # isort:skip

# Some of these import MySQLdb, so import them after checking if it's installed.
from django.db.backends.mysql.client import DatabaseClient                          # isort:skip
from django.db.backends.mysql.creation import DatabaseCreation                      # isort:skip
from django.db.backends.mysql.features import DatabaseFeatures                      # isort:skip
from django.db.backends.mysql.introspection import DatabaseIntrospection            # isort:skip
from django.db.backends.mysql.operations import DatabaseOperations                  # isort:skip
from django.db.backends.mysql.schema import DatabaseSchemaEditor                    # isort:skip
from django.db.backends.mysql.validation import DatabaseValidation                  # isort:skip

version = Database.version_info
if version < (1, 3, 3):
    raise ImproperlyConfigured("mysqlclient 1.3.3 or newer is required; you have %s" % Database.__version__)


# MySQLdb returns TIME columns as timedelta -- they are more like timedelta in
# terms of actual behavior as they are signed and include days -- and Django
# expects time.
django_conversions = conversions.copy()
django_conversions.update({
    FIELD_TYPE.TIME: backend_utils.typecast_time,
})

# This should match the numerical portion of the version numbers (we can treat
# versions like 5.0.24 and 5.0.24a as the same).
server_version_re = re.compile(r'(\d{1,2})\.(\d{1,2})\.(\d{1,2})')


from django.db.backends.mysql.base import CursorWrapper
from django.db.backends.mysql.base import DatabaseWrapper as _DatabaseWrapper

class DatabaseWrapper(_DatabaseWrapper):

    def get_new_connection(self, conn_params):
        # return a mysql connection
        alias = self._get_alias_by_params(conn_params)
        new_params = settings.DATABASES[alias]
        options = new_params.get('OPTIONS') or {}
        return Database.connect(
            host=new_params['HOST'],
            port=int(new_params['PORT']),
            user=new_params['USER'],
            db=new_params['NAME'],
            passwd=new_params['PASSWORD'],
            use_unicode=True,
            charset=options.get('charset') or 'utf8mb4',
            client_flag=conn_params['client_flag'],
            sql_mode=options.get('sql_mode') or 'STRICT_TRANS_TABLES',
        )

    def _get_alias_by_params(self, conn_params):
        target_str = ''.join([str(conn_params[_]) for _ in ['host', 'port', 'db', 'user', 'passwd']])
        for k, v in settings.DATABASES.items():
            _str = ''.join([str(v[_]) for _ in ['HOST', 'PORT', 'NAME', 'USER', 'PASSWORD']])
            if _str == target_str:
                return k
        return 'default'
