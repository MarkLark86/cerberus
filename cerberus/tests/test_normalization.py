# -*- coding: utf-8 -*-

from copy import deepcopy
from tempfile import NamedTemporaryFile

from pytest import mark

from cerberus import Validator, errors
from cerberus.tests import (
    assert_fail,
    assert_has_error,
    assert_normalized,
    assert_success,
)


def must_not_be_called(*args, **kwargs):
    raise RuntimeError('This shall not be called.')


@mark.asyncio
async def test_coerce():
    schema = {'amount': {'coerce': int}}
    document = {'amount': '1'}
    expected = {'amount': 1}
    await assert_normalized(document, expected, schema)


@mark.asyncio
async def test_coerce_in_dictschema():
    schema = {'thing': {'type': 'dict', 'schema': {'amount': {'coerce': int}}}}
    document = {'thing': {'amount': '2'}}
    expected = {'thing': {'amount': 2}}
    await assert_normalized(document, expected, schema)


@mark.asyncio
async def test_coerce_in_listschema():
    schema = {'things': {'type': 'list', 'schema': {'coerce': int}}}
    document = {'things': ['1', '2', '3']}
    expected = {'things': [1, 2, 3]}
    await assert_normalized(document, expected, schema)


@mark.asyncio
async def test_coerce_in_listitems():
    schema = {'things': {'type': 'list', 'items': [{'coerce': int}, {'coerce': str}]}}
    document = {'things': ['1', 2]}
    expected = {'things': [1, '2']}
    await assert_normalized(document, expected, schema)

    validator = Validator(schema)
    document['things'].append(3)
    assert not await validator(document)
    assert validator.document['things'] == document['things']


@mark.asyncio
async def test_coerce_in_dictschema_in_listschema():
    item_schema = {'type': 'dict', 'schema': {'amount': {'coerce': int}}}
    schema = {'things': {'type': 'list', 'schema': item_schema}}
    document = {'things': [{'amount': '2'}]}
    expected = {'things': [{'amount': 2}]}
    await assert_normalized(document, expected, schema)


@mark.asyncio
async def test_coerce_not_destructive():
    schema = {'amount': {'coerce': int}}
    v = Validator(schema)
    doc = {'amount': '1'}
    await v.validate(doc)
    assert v.document is not doc


@mark.asyncio
async def test_coerce_catches_ValueError():
    schema = {'amount': {'coerce': int}}
    _errors = await assert_fail({'amount': 'not_a_number'}, schema)
    _errors[0].info = ()  # ignore exception message here
    assert_has_error(
        _errors, 'amount', ('amount', 'coerce'), errors.COERCION_FAILED, int
    )


@mark.asyncio
async def test_coerce_in_listitems_catches_ValueError():
    schema = {'things': {'type': 'list', 'items': [{'coerce': int}, {'coerce': str}]}}
    document = {'things': ['not_a_number', 2]}
    _errors = await assert_fail(document, schema)
    _errors[0].info = ()  # ignore exception message here
    assert_has_error(
        _errors,
        ('things', 0),
        ('things', 'items', 'coerce'),
        errors.COERCION_FAILED,
        int,
    )


@mark.asyncio
async def test_coerce_catches_TypeError():
    schema = {'name': {'coerce': str.lower}}
    _errors = await assert_fail({'name': 1234}, schema)
    _errors[0].info = ()  # ignore exception message here
    assert_has_error(
        _errors, 'name', ('name', 'coerce'), errors.COERCION_FAILED, str.lower
    )


@mark.asyncio
async def test_coerce_in_listitems_catches_TypeError():
    schema = {
        'things': {'type': 'list', 'items': [{'coerce': int}, {'coerce': str.lower}]}
    }
    document = {'things': ['1', 2]}
    _errors = await assert_fail(document, schema)
    _errors[0].info = ()  # ignore exception message here
    assert_has_error(
        _errors,
        ('things', 1),
        ('things', 'items', 'coerce'),
        errors.COERCION_FAILED,
        str.lower,
    )


@mark.asyncio
async def test_coerce_unknown():
    schema = {'foo': {'schema': {}, 'allow_unknown': {'coerce': int}}}
    document = {'foo': {'bar': '0'}}
    expected = {'foo': {'bar': 0}}
    await assert_normalized(document, expected, schema)


@mark.asyncio
async def test_custom_coerce_and_rename():
    class MyNormalizer(Validator):
        def __init__(self, multiplier, *args, **kwargs):
            super(MyNormalizer, self).__init__(*args, **kwargs)
            self.multiplier = multiplier

        def _normalize_coerce_multiply(self, value):
            return value * self.multiplier

    v = MyNormalizer(2, {'foo': {'coerce': 'multiply'}})
    assert (await v.normalized({'foo': 2}))['foo'] == 4

    v = MyNormalizer(3, allow_unknown={'rename_handler': 'multiply'})
    assert (await v.normalized({3: None})) == {9: None}


@mark.asyncio
async def test_coerce_chain():
    drop_prefix = lambda x: x[2:]  # noqa: E731
    upper = lambda x: x.upper()  # noqa: E731
    schema = {'foo': {'coerce': [hex, drop_prefix, upper]}}
    await assert_normalized({'foo': 15}, {'foo': 'F'}, schema)


@mark.asyncio
async def test_coerce_chain_aborts(validator):
    def dont_do_me(value):
        raise AssertionError('The coercion chain did not abort after an ' 'error.')

    schema = {'foo': {'coerce': [hex, dont_do_me]}}
    await validator({'foo': '0'}, schema)
    assert errors.COERCION_FAILED in validator._errors


@mark.asyncio
async def test_coerce_non_digit_in_sequence(validator):
    # https://github.com/pyeve/cerberus/issues/211
    schema = {'data': {'type': 'list', 'schema': {'type': 'integer', 'coerce': int}}}
    document = {'data': ['q']}
    assert (await validator.validated(document, schema)) is None
    assert (
        (await validator.validated(document, schema, always_return_document=True)) == document
    )  # noqa: W503


@mark.asyncio
async def test_nullables_dont_fail_coerce():
    schema = {'foo': {'coerce': int, 'nullable': True, 'type': 'integer'}}
    document = {'foo': None}
    await assert_normalized(document, document, schema)


@mark.asyncio
async def test_nullables_fail_coerce_on_non_null_values(validator):
    def failing_coercion(value):
        raise Exception("expected to fail")

    schema = {'foo': {'coerce': failing_coercion, 'nullable': True, 'type': 'integer'}}
    document = {'foo': None}
    await assert_normalized(document, document, schema)

    await validator({'foo': 2}, schema)
    assert errors.COERCION_FAILED in validator._errors


@mark.asyncio
async def test_normalized():
    schema = {'amount': {'coerce': int}}
    document = {'amount': '2'}
    expected = {'amount': 2}
    await assert_normalized(document, expected, schema)


@mark.asyncio
async def test_rename(validator):
    schema = {'foo': {'rename': 'bar'}}
    document = {'foo': 0}
    expected = {'bar': 0}
    # We cannot use assertNormalized here since there is bug where
    # Cerberus says that the renamed field is an unknown field:
    # {'bar': 'unknown field'}
    await validator(document, schema, False)
    assert validator.document == expected


@mark.asyncio
async def test_rename_handler():
    validator = Validator(allow_unknown={'rename_handler': int})
    schema = {}
    document = {'0': 'foo'}
    expected = {0: 'foo'}
    await assert_normalized(document, expected, schema, validator)


@mark.asyncio
async def test_purge_unknown():
    validator = Validator(purge_unknown=True)
    schema = {'foo': {'type': 'string'}}
    document = {'bar': 'foo'}
    expected = {}
    await assert_normalized(document, expected, schema, validator)


@mark.asyncio
async def test_purge_unknown_in_subschema():
    schema = {
        'foo': {
            'type': 'dict',
            'schema': {'foo': {'type': 'string'}},
            'purge_unknown': True,
        }
    }
    document = {'foo': {'bar': ''}}
    expected = {'foo': {}}
    await assert_normalized(document, expected, schema)


@mark.asyncio
async def test_issue_147_complex():
    schema = {'revision': {'coerce': int}}
    document = {'revision': '5', 'file': NamedTemporaryFile(mode='w+')}
    document['file'].write(r'foobar')
    document['file'].seek(0)
    normalized = await Validator(schema, allow_unknown=True).normalized(document)
    assert normalized['revision'] == 5
    assert normalized['file'].read() == 'foobar'
    document['file'].close()
    normalized['file'].close()


@mark.asyncio
async def test_issue_147_nested_dict():
    schema = {'thing': {'type': 'dict', 'schema': {'amount': {'coerce': int}}}}
    ref_obj = '2'
    document = {'thing': {'amount': ref_obj}}
    normalized = await Validator(schema).normalized(document)
    assert document is not normalized
    assert normalized['thing']['amount'] == 2
    assert ref_obj == '2'
    assert document['thing']['amount'] is ref_obj


@mark.asyncio
async def test_coerce_in_valuesrules():
    # https://github.com/pyeve/cerberus/issues/155
    schema = {
        'thing': {'type': 'dict', 'valuesrules': {'coerce': int, 'type': 'integer'}}
    }
    document = {'thing': {'amount': '2'}}
    expected = {'thing': {'amount': 2}}
    await assert_normalized(document, expected, schema)


@mark.asyncio
async def test_coerce_in_keysrules():
    # https://github.com/pyeve/cerberus/issues/155
    schema = {
        'thing': {'type': 'dict', 'keysrules': {'coerce': int, 'type': 'integer'}}
    }
    document = {'thing': {'5': 'foo'}}
    expected = {'thing': {5: 'foo'}}
    await assert_normalized(document, expected, schema)


@mark.asyncio
async def test_coercion_of_sequence_items(validator):
    # https://github.com/pyeve/cerberus/issues/161
    schema = {'a_list': {'type': 'list', 'schema': {'type': 'float', 'coerce': float}}}
    document = {'a_list': [3, 4, 5]}
    expected = {'a_list': [3.0, 4.0, 5.0]}
    await assert_normalized(document, expected, schema, validator)
    for x in validator.document['a_list']:
        assert isinstance(x, float)


@mark.parametrize(
    'default', ({'default': 'bar_value'}, {'default_setter': lambda doc: 'bar_value'})
)
@mark.asyncio
async def test_default_missing(default):
    bar_schema = {'type': 'string'}
    bar_schema.update(default)
    schema = {'foo': {'type': 'string'}, 'bar': bar_schema}
    document = {'foo': 'foo_value'}
    expected = {'foo': 'foo_value', 'bar': 'bar_value'}
    await assert_normalized(document, expected, schema)


@mark.parametrize(
    'default', ({'default': 'bar_value'}, {'default_setter': must_not_be_called})
)
@mark.asyncio
async def test_default_existent(default):
    bar_schema = {'type': 'string'}
    bar_schema.update(default)
    schema = {'foo': {'type': 'string'}, 'bar': bar_schema}
    document = {'foo': 'foo_value', 'bar': 'non_default'}
    await assert_normalized(document, document.copy(), schema)


@mark.parametrize(
    'default', ({'default': 'bar_value'}, {'default_setter': must_not_be_called})
)
@mark.asyncio
async def test_default_none_nullable(default):
    bar_schema = {'type': 'string', 'nullable': True}
    bar_schema.update(default)
    schema = {'foo': {'type': 'string'}, 'bar': bar_schema}
    document = {'foo': 'foo_value', 'bar': None}
    await assert_normalized(document, document.copy(), schema)


@mark.parametrize(
    'default', ({'default': 'bar_value'}, {'default_setter': lambda doc: 'bar_value'})
)
@mark.asyncio
async def test_default_none_nonnullable(default):
    bar_schema = {'type': 'string', 'nullable': False}
    bar_schema.update(default)
    schema = {'foo': {'type': 'string'}, 'bar': bar_schema}
    document = {'foo': 'foo_value', 'bar': None}
    expected = {'foo': 'foo_value', 'bar': 'bar_value'}
    await assert_normalized(document, expected, schema)


@mark.asyncio
async def test_default_none_default_value():
    schema = {
        'foo': {'type': 'string'},
        'bar': {'type': 'string', 'nullable': True, 'default': None},
    }
    document = {'foo': 'foo_value'}
    expected = {'foo': 'foo_value', 'bar': None}
    await assert_normalized(document, expected, schema)


@mark.parametrize(
    'default', ({'default': 'bar_value'}, {'default_setter': lambda doc: 'bar_value'})
)
@mark.asyncio
async def test_default_missing_in_subschema(default):
    bar_schema = {'type': 'string'}
    bar_schema.update(default)
    schema = {
        'thing': {
            'type': 'dict',
            'schema': {'foo': {'type': 'string'}, 'bar': bar_schema},
        }
    }
    document = {'thing': {'foo': 'foo_value'}}
    expected = {'thing': {'foo': 'foo_value', 'bar': 'bar_value'}}
    await assert_normalized(document, expected, schema)


@mark.asyncio
async def test_depending_default_setters():
    schema = {
        'a': {'type': 'integer'},
        'b': {'type': 'integer', 'default_setter': lambda d: d['a'] + 1},
        'c': {'type': 'integer', 'default_setter': lambda d: d['b'] * 2},
        'd': {'type': 'integer', 'default_setter': lambda d: d['b'] + d['c']},
    }
    document = {'a': 1}
    expected = {'a': 1, 'b': 2, 'c': 4, 'd': 6}
    await assert_normalized(document, expected, schema)


@mark.asyncio
async def test_circular_depending_default_setters(validator):
    schema = {
        'a': {'type': 'integer', 'default_setter': lambda d: d['b'] + 1},
        'b': {'type': 'integer', 'default_setter': lambda d: d['a'] + 1},
    }
    await validator({}, schema)
    assert errors.SETTING_DEFAULT_FAILED in validator._errors


@mark.asyncio
async def test_issue_250():
    # https://github.com/pyeve/cerberus/issues/250
    schema = {
        'list': {
            'type': 'list',
            'schema': {
                'type': 'dict',
                'allow_unknown': True,
                'schema': {'a': {'type': 'string'}},
            },
        }
    }
    document = {'list': {'is_a': 'mapping'}}
    await assert_fail(
        document,
        schema,
        error=('list', ('list', 'type'), errors.BAD_TYPE, schema['list']['type']),
    )


@mark.asyncio
async def test_issue_250_no_type_pass_on_list():
    # https://github.com/pyeve/cerberus/issues/250
    schema = {
        'list': {
            'schema': {
                'allow_unknown': True,
                'type': 'dict',
                'schema': {'a': {'type': 'string'}},
            }
        }
    }
    document = {'list': [{'a': 'known', 'b': 'unknown'}]}
    await assert_normalized(document, document, schema)


@mark.asyncio
async def test_issue_250_no_type_fail_on_dict():
    # https://github.com/pyeve/cerberus/issues/250
    schema = {
        'list': {'schema': {'allow_unknown': True, 'schema': {'a': {'type': 'string'}}}}
    }
    document = {'list': {'a': {'a': 'known'}}}
    await assert_fail(
        document,
        schema,
        error=(
            'list',
            ('list', 'schema'),
            errors.BAD_TYPE_FOR_SCHEMA,
            schema['list']['schema'],
        ),
    )


@mark.asyncio
async def test_issue_250_no_type_fail_pass_on_other():
    # https://github.com/pyeve/cerberus/issues/250
    schema = {
        'list': {'schema': {'allow_unknown': True, 'schema': {'a': {'type': 'string'}}}}
    }
    document = {'list': 1}
    await assert_normalized(document, document, schema)


@mark.asyncio
async def test_allow_unknown_with_of_rules():
    # https://github.com/pyeve/cerberus/issues/251
    schema = {
        'test': {
            'oneof': [
                {
                    'type': 'dict',
                    'allow_unknown': True,
                    'schema': {'known': {'type': 'string'}},
                },
                {'type': 'dict', 'schema': {'known': {'type': 'string'}}},
            ]
        }
    }
    # check regression and that allow unknown does not cause any different
    # than expected behaviour for one-of.
    document = {'test': {'known': 's'}}
    await assert_fail(
        document,
        schema,
        error=('test', ('test', 'oneof'), errors.ONEOF, schema['test']['oneof']),
    )
    # check that allow_unknown is actually applied
    document = {'test': {'known': 's', 'unknown': 'asd'}}
    await assert_success(document, schema)


@mark.asyncio
async def test_271_normalising_tuples():
    # https://github.com/pyeve/cerberus/issues/271
    schema = {
        'my_field': {'type': 'list', 'schema': {'type': ('string', 'number', 'dict')}}
    }
    document = {'my_field': ('foo', 'bar', 42, 'albert', 'kandinsky', {'items': 23})}
    await assert_success(document, schema)

    normalized = await Validator(schema).normalized(document)
    assert normalized['my_field'] == (
        'foo',
        'bar',
        42,
        'albert',
        'kandinsky',
        {'items': 23},
    )


@mark.asyncio
async def test_allow_unknown_wo_schema():
    # https://github.com/pyeve/cerberus/issues/302
    v = Validator({'a': {'type': 'dict', 'allow_unknown': True}})
    await v({'a': {}})


@mark.asyncio
async def test_allow_unknown_with_purge_unknown():
    validator = Validator(purge_unknown=True)
    schema = {'foo': {'type': 'dict', 'allow_unknown': True}}
    document = {'foo': {'bar': True}, 'bar': 'foo'}
    expected = {'foo': {'bar': True}}
    await assert_normalized(document, expected, schema, validator)


@mark.asyncio
async def test_allow_unknown_with_purge_unknown_subdocument():
    validator = Validator(purge_unknown=True)
    schema = {
        'foo': {
            'type': 'dict',
            'schema': {'bar': {'type': 'string'}},
            'allow_unknown': True,
        }
    }
    document = {'foo': {'bar': 'baz', 'corge': False}, 'thud': 'xyzzy'}
    expected = {'foo': {'bar': 'baz', 'corge': False}}
    await assert_normalized(document, expected, schema, validator)


@mark.asyncio
async def test_purge_readonly():
    schema = {
        'description': {'type': 'string', 'maxlength': 500},
        'last_updated': {'readonly': True},
    }
    validator = Validator(schema=schema, purge_readonly=True)
    document = {'description': 'it is a thing'}
    expected = deepcopy(document)
    document['last_updated'] = 'future'
    await assert_normalized(document, expected, validator=validator)


@mark.asyncio
async def test_defaults_in_allow_unknown_schema():
    schema = {'meta': {'type': 'dict'}, 'version': {'type': 'string'}}
    allow_unknown = {
        'type': 'dict',
        'schema': {
            'cfg_path': {'type': 'string', 'default': 'cfg.yaml'},
            'package': {'type': 'string'},
        },
    }
    validator = Validator(schema=schema, allow_unknown=allow_unknown)

    document = {'version': '1.2.3', 'plugin_foo': {'package': 'foo'}}
    expected = {
        'version': '1.2.3',
        'plugin_foo': {'package': 'foo', 'cfg_path': 'cfg.yaml'},
    }
    await assert_normalized(document, expected, schema, validator)
