import warnings

from .fields import Field


class _Model(type):
    def __new__(cls, *args, **kwargs):
        name, bases, attrs = args
        if name == 'Model':
            return super().__new__(cls, *args, **kwargs)

        # handle total_fields, total_fields = fields + tags(if stable)
        total_fields = {k: v for k, v in attrs.items() if isinstance(v, Field)}

        # handle meta info
        meta = attrs.get('Meta', None)
        _meta = dict()
        _tags = dict()
        _tags_value = None
        _type = None
        _stable_name = None
        _table_name = name
        if meta is not None:
            # handle table_type
            table_type = getattr(meta, 'table_type', None)
            if table_type is not None:
                _type = table_type

            # handle table name
            table_name = getattr(meta, 'table_name', None)
            if table_name is not None:
                _table_name = table_name.lower()

            # handle stable_name
            stable_name = getattr(meta, 'stable_name', None)
            if stable_name:
                _stable_name = stable_name

            # handle tags
            tags = getattr(meta, 'tags_name', None)
            if tags is not None:
                if isinstance(tags, str):
                    tags = tags.split(',')
                if isinstance(tags, (tuple, list)):
                    tags = [o.strip(' ') for o in tags]
                    for _tag in tags:
                        if _tag not in total_fields:
                            raise ValueError('Tags contains invalid field: %s' % _tag)
                        _tags[_tag] = total_fields[_tag]
                else:
                    raise ValueError('Tags type must be string, tuple or list')

            # handle tags_value
            tags_value = getattr(meta, 'tags_value', None)
            if tags_value:
                _tags_value = tags_value

        # only fields, no tags
        fields = {k: v for k, v in total_fields.items() if k not in _tags}

        _meta['table_name'] = _table_name
        _meta['tags_name'] = _tags
        _meta['tags_value'] = _tags_value
        _meta['stable_name'] = _stable_name
        _meta['table_type'] = _type

        attrs['meta'] = _meta
        attrs['total_fields'] = total_fields
        attrs['fields'] = fields
        del attrs['Meta']
        return super().__new__(cls, *args, **kwargs)


class Model(metaclass=_Model):
    _created_models = []
    _created_names = []

    def __init__(self, *args, **kwargs):
        # for _field_name, _field in self.total_fields.items():
        #     value = kwargs.get(_field_name)
        #     if value:
        #         setattr(self, _field_name, value)
        #         continue
        #     _default = _field.default
        #     if _default is not None:
        #         try:
        #             value = _default()
        #         except TypeError:
        #             value = _default
        #         setattr(self, _field_name, value)
        #         continue
        #     raise ValueError('Field %s must have a value' % _field_name)
        pass

    def __init_subclass__(cls, **kwargs):
        table_name = cls.meta['table_name']
        if table_name in cls._created_names:
            raise ValueError('model(%s) has been created' % table_name)
        cls._created_models.append(cls)
        cls._created_names.append(table_name)

    # @classmethod
    # def describe(cls):
    #     table_type = cls.meta['table_type']
    #     if table_type == 'table':
    #         return [
    #             (
    #                 field.name,
    #                 field._field_type,
    #                 field._field_length,
    #                 ''
    #             )
    #             for field in cls.total_fields
    #         ]
    #     if table_type == 'stable':
    #         tags_name = cls.meta.tags_name
    #         return [
    #             (
    #                 field.name,
    #                 field._field_type,
    #                 field._field_length,
    #                 'tag' if field in tags_name else ''
    #             )
    #             for field in cls.total_fields
    #         ]
    #     if table_type == 'sub_table':
    #         pass

    @classmethod
    def create_all(cls, session):
        if cls._created_models:
            _existed_tables = [o[0] for o in session._execute('show tables')]
            _existed_stables = [o[0] for o in session._execute('show stables')]

            for _model in cls._created_models:
                table_type = _model.meta['table_type']
                if not table_type:
                    raise ValueError('Model %s must have table_type' % _model.__name__)

                table_name = _model.meta['table_name']
                if table_name in _existed_tables:
                    warnings.warn('table %s already exists' % table_name)
                    continue

                stable_name = _model.meta['stable_name']
                if stable_name in _existed_stables and table_type != 'sub_table':
                    warnings.warn('table %s already exists' % stable_name)
                    continue

                total_fields = _model.total_fields
                fields = _model.fields
                tags = _model.meta['tags_name']
                tags_value = _model.meta['tags_value']

                if table_type == 'table':
                    if not table_name:
                        raise ValueError('Model %s must have table_name' % _model.__name__)
                    if not total_fields:
                        raise ValueError('Model %s must have field' % _model.__name__)
                    sql1 = 'create table %s ' % table_name
                    sql2 = ' (' + ', '.join(_field._create_sql for _field in total_fields.values()) + ') '
                    sql = sql1 + sql2

                elif table_type == 'stable':
                    if not stable_name:
                        raise ValueError('Model %s must have stable_name' % _model.__name__)
                    if not tags:
                        raise ValueError('Model %s must have tag' % _model.__name__)
                    sql1 = 'create table %s ' % stable_name
                    sql2 = ' (' + ', '.join(_field._create_sql for _field in fields.values()) + ') '
                    sql3 = ' tags (' + ', '.join(_tag._create_sql for _tag in tags.values()) + ') '
                    sql = sql1 + sql2 + sql3

                elif table_type == 'sub_table':
                    if not table_name:
                        raise ValueError('Model %s must have table_name' % _model.__name__)
                    if not stable_name:
                        raise ValueError('Model %s must have stable_name' % _model.__name__)
                    if not tags_value:
                        raise ValueError('Model %s must have tags_value' % _model.__name__)
                    sql1 = ' create table %s ' % table_name
                    sql2 = ' using %s ' % stable_name
                    table_desc = session._execute('describe %s' % stable_name)
                    _tags = tuple('{%s}' % o[0] for o in table_desc if o[3] == 'tag')
                    sql3 = 'tags' + _tags.__str__().format(**tags_value)
                    sql = sql1 + sql2 + sql3
                else:
                    raise ValueError('Model %s model_type error' % _model.__name__)
                session._execute(sql)
