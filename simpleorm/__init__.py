"""
An ORM for SQLite3.

~~ PRE-ALPHA ~~

"""

import sqlite3
import inspect
from datetime import datetime

con = sqlite3.connect(':memory:')

def log_sql(sql):
  print(sql)

con.set_trace_callback(log_sql)

con.execute('PRAGMA journal_mode = WAL')
con.execute('PRAGMA foreign_keys = ON;')

def now():
  return datetime.utcnow()

class Field(dict):
  data_type = None

  def get_attribs(self, field_name):
    attribs = []

    if self.get('required'):
      attribs.append('NOT NULL')

    if self.get('unique'):
      attribs.append('UNIQUE')

    default = self.get('default')
    if default is not None and not callable(default):
      if type(default) is str:
        attribs.append('DEFAULT "%s"' % default)
      else:
        attribs.append('DEFAULT %s' % default)

    if self.get('primary_key'):
      attribs.append('PRIMARY KEY')

    return attribs

class TextField(Field):
  data_type = 'text'

  def get_attribs(self, field_name):
    attribs = Field.get_attribs(self, field_name)

    min, max = self.get('min'), self.get('max')
    if min is not None and max is not None:
      if min == max:
        attribs.append('CHECK(LENGTH(%s)=%s)' % (field_name, min))
      else:
        attribs.append('CHECK(LENGTH(%s) >= %s AND LENGTH(%s) <= %s)' % (
          field_name, min, field_name, max))

    elif min is not None:
      attribs.append('CHECK(LENGTH(%s) >= %s)' % (field_name, min))

    elif max is not None:
      attribs.append('CHECK(LENGTH(%s) <= %s)' % (field_name, max))

    return attribs

class ForeignKey(Field):
  data_type = 'integer'

class IntField(Field):
  data_type = 'integer'

  def get_attribs(self, field_name):
    attribs = Field.get_attribs(self, field_name)

    if self.get('autoincrement'):
      attribs.append('AUTOINCREMENT')

    min, max = self.get('min'), self.get('max')
    if min is not None and max is not None:
      attribs.append('CHECK(%s >= %s AND %s <= %s)' % (
        field_name, min, field_name, max))
    elif min is not None:
      attribs.append('CHECK(%s >= %s)' % (field_name, min))
    elif max is not None:
      attribs.append('CHECK(%s <= %s)' % (field_name, max))

    return attribs

class EnumField(TextField):
  data_type = 'text'

  def get_attribs(self, field_name):
    attribs = Field.get_attribs(self, field_name)

    choices = self.get('choices')
    if len(choices) > 0:
      attribs.append('CHECK(%s IN (%s))' % (
        field_name,
        ','.join(map(lambda x: '"%s"' % x, choices))))

    return attribs

class Storable:
  class Meta:
    auto_timestamps = True

  @classmethod
  def metadata(cls):
    if hasattr(cls, 'Meta'):
      return cls.Meta
    return {}

  @classmethod
  def table_name(cls):
    tbl = getattr(cls.metadata(), 'table', None)
    return tbl if tbl else cls.__name__.lower()

  @classmethod
  def pk(cls):
    for field_name, field_meta in cls.schema().items():
      if field_meta.get('primary_key'):
        return field_name
    return getattr(cls.metadata(), 'pk', None)

  @classmethod
  def auto_timestamps(cls):
    return getattr(cls.metadata(), 'auto_timestamps', False)

  @classmethod
  def schema(cls):
    if hasattr(cls, 'Schema'):
      return { m[0] : m[1] for m in inspect.getmembers(cls.Schema)
        if not m[0].startswith('_') }
    return {}

  @classmethod
  def create_table(cls):
    sql = ['CREATE TABLE IF NOT EXISTS "%s"\n(' % cls.table_name()]

    fields = []
    for field_name, field_meta in cls.schema().items():
      attribs = field_meta.get_attribs(field_name)
      fields.append('    "%s" %s %s' % (
        field_name,
        field_meta.data_type.upper(),
        ' '.join(attribs)))

    if cls.auto_timestamps():
      fields.append('    "created_at" DATETIME DEFAULT CURRENT_TIMESTAMP')
      fields.append('    "updated_at" DATETIME')

    sql.append(',\n'.join(fields))
    sql.append(')')
    sql = '\n'.join(sql)
    with con:
      con.executescript(sql)

  def save(self, use_transaction=True):
    self.before_save()

    if not hasattr(self, 'id'):
      self._insert(use_transaction)
    else:
      self._update(use_transaction)

    self.after_save()
    return self

  def _insert(self, use_transaction):
    self.before_insert()

    sql = ['INSERT INTO %s(' % self.table_name()]

    schema = self.schema()

    field_names = []
    values = []
    generated_pk = False
    pk_field_name = None

    for field_name, field_meta in schema.items():
      field_names.append(field_name)
      if field_meta.get('primary_key') and field_meta.get('autoincrement'):
        generated_pk = True
        pk_field_name = field_name
        values.append(None)
      else:
        value = None
        if hasattr(self, field_name):
          value = getattr(self, field_name, None)
        if value is None:
          default = field_meta.get('default')
          if default is not None:
            value = default(self) if callable(default) else default
            setattr(self, field_name, value)
          elif field_meta.get('required'):
            raise ValueError('%s is required' % field_name)
        values.append(value)
    sql.append(','.join(field_names))

    sql.append(') VALUES(')
    sql.append(','.join(['?'] * len(schema)))
    sql.append(')')
    sql = ''.join(sql)

    cur = con.cursor()
    if use_transaction:
      with con:
        cur.execute(sql, values)
    else:
      cur.execute(sql, values)
    id = cur.lastrowid
    if generated_pk:
      setattr(self, pk_field_name, id)

    self.after_insert()

  def _update(self, use_transaction):
    self.before_update()

    sql = ['UPDATE %s SET ' % self.table_name()]

    schema = self.schema()

    updates = []
    values = []

    pk_field_name = self.pk()

    for field_name, field_meta in schema.items():
      if field_name == pk_field_name: next
      updates.append('%s=?' % field_name)
      values.append(getattr(self, field_name, None))

    if self.auto_timestamps():
      updates.append('updated_at=CURRENT_TIMESTAMP')

    sql.append(', '.join(updates))

    sql.append(' WHERE %s=?' % pk_field_name)
    values.append(getattr(self, pk_field_name))

    sql = ''.join(sql)

    cur = con.cursor()
    if use_transaction:
      with con:
        cur.execute(sql, values)
    else:
      cur.execute(sql, values)
    n = cur.rowcount
    if n == 0:
      raise RuntimeError('nothing updated')

    self.after_update()

  def delete(self, use_transaction=True):
    self.before_delete()

    pk_field_name = self.pk()
    pk = getattr(self, pk_field_name)

    sql = 'DELETE FROM %s WHERE %s=?' % (self.table_name(), pk_field_name)

    cur = con.cursor()

    if use_transaction:
      with con:
        cur.execute(sql, (pk,))
    else:
      cur.execute(sql, (pk,))

    if cur.rowcount == 0:
      raise RuntimeError('nothing deleted')

    self.after_delete()
    return self

  def before_save(self): pass
  def after_save(self): pass

  def before_insert(self): pass
  def after_insert(self): pass

  def before_update(self): pass
  def after_update(self): pass

  def before_delete(self): pass
  def after_delete(self): pass

class Foo(Storable):
  class Schema:
    id = IntField(autoincrement=True, primary_key=True)
    name = TextField(min=2, max=2, required=True, unique=True)
    sex = EnumField(choices=['m', 'f'], default='m')
    bars = ForeignKey(cls='Bar', field='id', many=True)

class B(Storable):
  class Schema:
    id = IntField(autoincrement=True, primary_key=True)
    bar = TextField(required=True)

  class Meta:
    table = 'bar'

A.create_table()
B.create_table()

a = A()
a.name = 'br'
a.save()

a.name = 'zz'
a.save()
