# -*- coding: utf-8 -*-

import itertools
import re
import sys
from datetime import datetime, date
from random import choice
from string import ascii_lowercase

from pytest import mark

from cerberus import errors, Validator
from cerberus.tests import (
    assert_bad_type,
    assert_document_error,
    assert_fail,
    assert_has_error,
    assert_not_has_error,
    assert_success,
)
from cerberus.tests.conftest import sample_schema


@mark.asyncio
async def test_empty_document():
    await assert_document_error(None, sample_schema, None, errors.DOCUMENT_MISSING)


@mark.asyncio
async def test_bad_document_type():
    document = "not a dict"
    await assert_document_error(
        document, sample_schema, None, errors.DOCUMENT_FORMAT.format(document)
    )


@mark.asyncio
async def test_unknown_field(validator):
    field = 'surname'
    await assert_fail(
        {field: 'doe'},
        validator=validator,
        error=(field, (), errors.UNKNOWN_FIELD, None),
    )
    assert validator.errors == {field: ['unknown field']}


@mark.asyncio
async def test_empty_field_definition(document):
    field = 'name'
    schema = {field: {}}
    await assert_success(document, schema)


@mark.asyncio
async def test_required_field(schema):
    field = 'a_required_string'
    required_string_extension = {
        'a_required_string': {
            'type': 'string',
            'minlength': 2,
            'maxlength': 10,
            'required': True,
        }
    }
    schema.update(required_string_extension)
    await assert_fail(
        {'an_integer': 1},
        schema,
        error=(field, (field, 'required'), errors.REQUIRED_FIELD, True),
    )


@mark.asyncio
async def test_nullable_field():
    await assert_success({'a_nullable_integer': None})
    await assert_success({'a_nullable_integer': 3})
    await assert_success({'a_nullable_field_without_type': None})
    await assert_fail({'a_nullable_integer': "foo"})
    await assert_fail({'an_integer': None})
    await assert_fail({'a_not_nullable_field_without_type': None})


@mark.asyncio
async def test_nullable_skips_allowed():
    schema = {'role': {'allowed': ['agent', 'client', 'supplier'], 'nullable': True}}
    await assert_success({'role': None}, schema)


@mark.asyncio
async def test_readonly_field():
    field = 'a_readonly_string'
    await assert_fail(
        {field: 'update me if you can'},
        error=(field, (field, 'readonly'), errors.READONLY_FIELD, True),
    )


@mark.asyncio
async def test_readonly_field_first_rule():
    # test that readonly rule is checked before any other rule, and blocks.
    # See #63.
    schema = {'a_readonly_number': {'type': 'integer', 'readonly': True, 'max': 1}}
    v = Validator(schema)
    await v.validate({'a_readonly_number': 2})
    # it would be a list if there's more than one error; we get a dict
    # instead.
    assert 'read-only' in v.errors['a_readonly_number'][0]


@mark.asyncio
async def test_readonly_field_with_default_value():
    schema = {
        'created': {'type': 'string', 'readonly': True, 'default': 'today'},
        'modified': {
            'type': 'string',
            'readonly': True,
            'default_setter': lambda d: d['created'],
        },
    }
    await assert_success({}, schema)
    expected_errors = [
        (
            'created',
            ('created', 'readonly'),
            errors.READONLY_FIELD,
            schema['created']['readonly'],
        ),
        (
            'modified',
            ('modified', 'readonly'),
            errors.READONLY_FIELD,
            schema['modified']['readonly'],
        ),
    ]
    await assert_fail(
        {'created': 'tomorrow', 'modified': 'today'}, schema, errors=expected_errors
    )
    await assert_fail(
        {'created': 'today', 'modified': 'today'}, schema, errors=expected_errors
    )


@mark.asyncio
async def test_nested_readonly_field_with_default_value():
    schema = {
        'some_field': {
            'type': 'dict',
            'schema': {
                'created': {'type': 'string', 'readonly': True, 'default': 'today'},
                'modified': {
                    'type': 'string',
                    'readonly': True,
                    'default_setter': lambda d: d['created'],
                },
            },
        }
    }
    await assert_success({'some_field': {}}, schema)
    expected_errors = [
        (
            ('some_field', 'created'),
            ('some_field', 'schema', 'created', 'readonly'),
            errors.READONLY_FIELD,
            schema['some_field']['schema']['created']['readonly'],
        ),
        (
            ('some_field', 'modified'),
            ('some_field', 'schema', 'modified', 'readonly'),
            errors.READONLY_FIELD,
            schema['some_field']['schema']['modified']['readonly'],
        ),
    ]
    await assert_fail(
        {'some_field': {'created': 'tomorrow', 'modified': 'now'}},
        schema,
        errors=expected_errors,
    )
    await assert_fail(
        {'some_field': {'created': 'today', 'modified': 'today'}},
        schema,
        errors=expected_errors,
    )


@mark.asyncio
async def test_repeated_readonly(validator):
    # https://github.com/pyeve/cerberus/issues/311
    validator.schema = {'id': {'readonly': True}}
    await assert_fail({'id': 0}, validator=validator)
    await assert_fail({'id': 0}, validator=validator)


@mark.asyncio
async def test_not_a_string():
    await assert_bad_type('a_string', 'string', 1)


@mark.asyncio
async def test_not_a_binary():
    # 'u' literal prefix produces type `str` in Python 3
    await assert_bad_type('a_binary', 'binary', u"i'm not a binary")


@mark.asyncio
async def test_not_a_integer():
    await assert_bad_type('an_integer', 'integer', "i'm not an integer")


@mark.asyncio
async def test_not_a_boolean():
    await assert_bad_type('a_boolean', 'boolean', "i'm not a boolean")


@mark.asyncio
async def test_not_a_datetime():
    await assert_bad_type('a_datetime', 'datetime', "i'm not a datetime")


@mark.asyncio
async def test_not_a_float():
    await assert_bad_type('a_float', 'float', "i'm not a float")


@mark.asyncio
async def test_not_a_number():
    await assert_bad_type('a_number', 'number', "i'm not a number")


@mark.asyncio
async def test_not_a_list():
    await assert_bad_type('a_list_of_values', 'list', "i'm not a list")


@mark.asyncio
async def test_not_a_dict():
    await assert_bad_type('a_dict', 'dict', "i'm not a dict")


@mark.asyncio
async def test_bad_max_length(schema):
    field = 'a_string'
    max_length = schema[field]['maxlength']
    value = "".join(choice(ascii_lowercase) for i in range(max_length + 1))
    await assert_fail(
        {field: value},
        error=(
            field,
            (field, 'maxlength'),
            errors.MAX_LENGTH,
            max_length,
            (len(value),),
        ),
    )


@mark.asyncio
async def test_bad_max_length_binary(schema):
    field = 'a_binary'
    max_length = schema[field]['maxlength']
    value = b'\x00' * (max_length + 1)
    await assert_fail(
        {field: value},
        error=(
            field,
            (field, 'maxlength'),
            errors.MAX_LENGTH,
            max_length,
            (len(value),),
        ),
    )


@mark.asyncio
async def test_bad_min_length(schema):
    field = 'a_string'
    min_length = schema[field]['minlength']
    value = "".join(choice(ascii_lowercase) for i in range(min_length - 1))
    await assert_fail(
        {field: value},
        error=(
            field,
            (field, 'minlength'),
            errors.MIN_LENGTH,
            min_length,
            (len(value),),
        ),
    )


@mark.asyncio
async def test_bad_min_length_binary(schema):
    field = 'a_binary'
    min_length = schema[field]['minlength']
    value = b'\x00' * (min_length - 1)
    await assert_fail(
        {field: value},
        error=(
            field,
            (field, 'minlength'),
            errors.MIN_LENGTH,
            min_length,
            (len(value),),
        ),
    )


@mark.asyncio
async def test_bad_max_value(schema):
    async def assert_bad_max_value(field, inc):
        max_value = schema[field]['max']
        value = max_value + inc
        await assert_fail(
            {field: value}, error=(field, (field, 'max'), errors.MAX_VALUE, max_value)
        )

    field = 'an_integer'
    await assert_bad_max_value(field, 1)
    field = 'a_float'
    await assert_bad_max_value(field, 1.0)
    field = 'a_number'
    await assert_bad_max_value(field, 1)


@mark.asyncio
async def test_bad_min_value(schema):
    async def assert_bad_min_value(field, inc):
        min_value = schema[field]['min']
        value = min_value - inc
        await assert_fail(
            {field: value}, error=(field, (field, 'min'), errors.MIN_VALUE, min_value)
        )

    field = 'an_integer'
    await assert_bad_min_value(field, 1)
    field = 'a_float'
    await assert_bad_min_value(field, 1.0)
    field = 'a_number'
    await assert_bad_min_value(field, 1)


@mark.asyncio
async def test_bad_schema():
    field = 'a_dict'
    subschema_field = 'address'
    schema = {
        field: {
            'type': 'dict',
            'schema': {
                subschema_field: {'type': 'string'},
                'city': {'type': 'string', 'required': True},
            },
        }
    }
    document = {field: {subschema_field: 34}}
    validator = Validator(schema)

    await assert_fail(
        document,
        validator=validator,
        error=(
            field,
            (field, 'schema'),
            errors.MAPPING_SCHEMA,
            validator.schema['a_dict']['schema'],
        ),
        child_errors=[
            (
                (field, subschema_field),
                (field, 'schema', subschema_field, 'type'),
                errors.BAD_TYPE,
                'string',
            ),
            (
                (field, 'city'),
                (field, 'schema', 'city', 'required'),
                errors.REQUIRED_FIELD,
                True,
            ),
        ],
    )

    handler = errors.BasicErrorHandler
    assert field in validator.errors
    assert subschema_field in validator.errors[field][-1]
    assert (
        handler.messages[errors.BAD_TYPE.code].format(constraint='string')
        in validator.errors[field][-1][subschema_field]
    )
    assert 'city' in validator.errors[field][-1]
    assert (
        handler.messages[errors.REQUIRED_FIELD.code]
        in validator.errors[field][-1]['city']
    )


@mark.asyncio
async def test_bad_valuesrules():
    field = 'a_dict_with_valuesrules'
    schema_field = 'a_string'
    value = {schema_field: 'not an integer'}

    exp_child_errors = [
        (
            (field, schema_field),
            (field, 'valuesrules', 'type'),
            errors.BAD_TYPE,
            'integer',
        )
    ]
    await assert_fail(
        {field: value},
        error=(field, (field, 'valuesrules'), errors.VALUESRULES, {'type': 'integer'}),
        child_errors=exp_child_errors,
    )


@mark.asyncio
async def test_bad_list_of_values(validator):
    field = 'a_list_of_values'
    value = ['a string', 'not an integer']
    await assert_fail(
        {field: value},
        validator=validator,
        error=(
            field,
            (field, 'items'),
            errors.BAD_ITEMS,
            [{'type': 'string'}, {'type': 'integer'}],
        ),
        child_errors=[
            ((field, 1), (field, 'items', 1, 'type'), errors.BAD_TYPE, 'integer')
        ],
    )

    assert (
        errors.BasicErrorHandler.messages[errors.BAD_TYPE.code].format(
            constraint='integer'
        )
        in validator.errors[field][-1][1]
    )

    value = ['a string', 10, 'an extra item']
    await assert_fail(
        {field: value},
        error=(
            field,
            (field, 'items'),
            errors.ITEMS_LENGTH,
            [{'type': 'string'}, {'type': 'integer'}],
            (2, 3),
        ),
    )


@mark.asyncio
async def test_bad_list_of_integers():
    field = 'a_list_of_integers'
    value = [34, 'not an integer']
    await assert_fail({field: value})


@mark.asyncio
async def test_bad_list_of_dicts():
    field = 'a_list_of_dicts'
    map_schema = {
        'sku': {'type': 'string'},
        'price': {'type': 'integer', 'required': True},
    }
    seq_schema = {'type': 'dict', 'schema': map_schema}
    schema = {field: {'type': 'list', 'schema': seq_schema}}
    validator = Validator(schema)
    value = [{'sku': 'KT123', 'price': '100'}]
    document = {field: value}

    await assert_fail(
        document,
        validator=validator,
        error=(field, (field, 'schema'), errors.SEQUENCE_SCHEMA, seq_schema),
        child_errors=[
            ((field, 0), (field, 'schema', 'schema'), errors.MAPPING_SCHEMA, map_schema)
        ],
    )

    assert field in validator.errors
    assert 0 in validator.errors[field][-1]
    assert 'price' in validator.errors[field][-1][0][-1]
    exp_msg = errors.BasicErrorHandler.messages[errors.BAD_TYPE.code].format(
        constraint='integer'
    )
    assert exp_msg in validator.errors[field][-1][0][-1]['price']

    value = ["not a dict"]
    exp_child_errors = [
        ((field, 0), (field, 'schema', 'type'), errors.BAD_TYPE, 'dict', ())
    ]
    await assert_fail(
        {field: value},
        error=(field, (field, 'schema'), errors.SEQUENCE_SCHEMA, seq_schema),
        child_errors=exp_child_errors,
    )


@mark.asyncio
async def test_array_unallowed():
    field = 'an_array'
    value = ['agent', 'client', 'profit']
    await assert_fail(
        {field: value},
        error=(
            field,
            (field, 'allowed'),
            errors.UNALLOWED_VALUES,
            ['agent', 'client', 'vendor'],
            (('profit',),),
        ),
    )


@mark.asyncio
async def test_string_unallowed():
    field = 'a_restricted_string'
    value = 'profit'
    await assert_fail(
        {field: value},
        error=(
            field,
            (field, 'allowed'),
            errors.UNALLOWED_VALUE,
            ['agent', 'client', 'vendor'],
            value,
        ),
    )


@mark.asyncio
async def test_integer_unallowed():
    field = 'a_restricted_integer'
    value = 2
    await assert_fail(
        {field: value},
        error=(field, (field, 'allowed'), errors.UNALLOWED_VALUE, [-1, 0, 1], value),
    )


@mark.asyncio
async def test_integer_allowed():
    await assert_success({'a_restricted_integer': -1})


@mark.asyncio
async def test_validate_update():
    await assert_success(
        {
            'an_integer': 100,
            'a_dict': {'address': 'adr'},
            'a_list_of_dicts': [{'sku': 'let'}],
        },
        update=True,
    )


@mark.asyncio
async def test_string():
    await assert_success({'a_string': 'john doe'})


@mark.asyncio
async def test_string_allowed():
    await assert_success({'a_restricted_string': 'client'})


@mark.asyncio
async def test_integer():
    await assert_success({'an_integer': 50})


@mark.asyncio
async def test_boolean():
    await assert_success({'a_boolean': True})


@mark.asyncio
async def test_datetime():
    await assert_success({'a_datetime': datetime.now()})


@mark.asyncio
async def test_float():
    await assert_success({'a_float': 3.5})
    await assert_success({'a_float': 1})


@mark.asyncio
async def test_number():
    await assert_success({'a_number': 3.5})
    await assert_success({'a_number': 3})


@mark.asyncio
async def test_array():
    await assert_success({'an_array': ['agent', 'client']})


@mark.asyncio
async def test_set():
    await assert_success({'a_set': set(['hello', 1])})


@mark.asyncio
async def test_one_of_two_types(validator):
    field = 'one_or_more_strings'
    await assert_success({field: 'foo'})
    await assert_success({field: ['foo', 'bar']})
    exp_child_errors = [
        ((field, 1), (field, 'schema', 'type'), errors.BAD_TYPE, 'string')
    ]
    await assert_fail(
        {field: ['foo', 23]},
        validator=validator,
        error=(field, (field, 'schema'), errors.SEQUENCE_SCHEMA, {'type': 'string'}),
        child_errors=exp_child_errors,
    )
    await assert_fail(
        {field: 23},
        error=((field,), (field, 'type'), errors.BAD_TYPE, ['string', 'list']),
    )
    assert validator.errors == {field: [{1: ['must be of string type']}]}


@mark.asyncio
async def test_regex(validator):
    field = 'a_regex_email'
    await assert_success({field: 'valid.email@gmail.com'}, validator=validator)
    await assert_fail(
        {field: 'invalid'},
        update=True,
        error=(
            field,
            (field, 'regex'),
            errors.REGEX_MISMATCH,
            r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$',
        ),
    )


@mark.asyncio
async def test_regex_with_flag():
    await assert_success({"item": "hOly grAil"}, {"item": {"regex": "(?i)holy grail"}})
    await assert_fail({"item": "hOly grAil"}, {"item": {"regex": "holy grail"}})


@mark.asyncio
async def test_a_list_of_dicts():
    await assert_success(
        {
            'a_list_of_dicts': [
                {'sku': 'AK345', 'price': 100},
                {'sku': 'YZ069', 'price': 25},
            ]
        }
    )


@mark.asyncio
async def test_a_list_of_values():
    await assert_success({'a_list_of_values': ['hello', 100]})


@mark.asyncio
async def test_an_array_from_set():
    await assert_success({'an_array_from_set': ['agent', 'client']})


@mark.asyncio
async def test_a_list_of_integers():
    await assert_success({'a_list_of_integers': [99, 100]})


@mark.asyncio
async def test_a_dict(schema):
    await assert_success({'a_dict': {'address': 'i live here', 'city': 'in my own town'}})
    await assert_fail(
        {'a_dict': {'address': 8545}},
        error=(
            'a_dict',
            ('a_dict', 'schema'),
            errors.MAPPING_SCHEMA,
            schema['a_dict']['schema'],
        ),
        child_errors=[
            (
                ('a_dict', 'address'),
                ('a_dict', 'schema', 'address', 'type'),
                errors.BAD_TYPE,
                'string',
            ),
            (
                ('a_dict', 'city'),
                ('a_dict', 'schema', 'city', 'required'),
                errors.REQUIRED_FIELD,
                True,
            ),
        ],
    )


@mark.asyncio
async def test_a_dict_with_valuesrules(validator):
    await assert_success(
        {'a_dict_with_valuesrules': {'an integer': 99, 'another integer': 100}}
    )

    error = (
        'a_dict_with_valuesrules',
        ('a_dict_with_valuesrules', 'valuesrules'),
        errors.VALUESRULES,
        {'type': 'integer'},
    )
    child_errors = [
        (
            ('a_dict_with_valuesrules', 'a string'),
            ('a_dict_with_valuesrules', 'valuesrules', 'type'),
            errors.BAD_TYPE,
            'integer',
        )
    ]

    await assert_fail(
        {'a_dict_with_valuesrules': {'a string': '99'}},
        validator=validator,
        error=error,
        child_errors=child_errors,
    )

    assert 'valuesrules' in validator.schema_error_tree['a_dict_with_valuesrules']
    v = validator.schema_error_tree
    assert len(v['a_dict_with_valuesrules']['valuesrules'].descendants) == 1


# TODO remove 'keyschema' as rule with the next major release
@mark.parametrize('rule', ('keysrules', 'keyschema'))
@mark.asyncio
async def test_keysrules(rule):
    schema = {
        'a_dict_with_keysrules': {
            'type': 'dict',
            rule: {'type': 'string', 'regex': '[a-z]+'},
        }
    }
    await assert_success({'a_dict_with_keysrules': {'key': 'value'}}, schema=schema)
    await assert_fail({'a_dict_with_keysrules': {'KEY': 'value'}}, schema=schema)


@mark.asyncio
async def test_a_list_length(schema):
    field = 'a_list_length'
    min_length = schema[field]['minlength']
    max_length = schema[field]['maxlength']

    await assert_fail(
        {field: [1] * (min_length - 1)},
        error=(
            field,
            (field, 'minlength'),
            errors.MIN_LENGTH,
            min_length,
            (min_length - 1,),
        ),
    )

    for i in range(min_length, max_length):
        value = [1] * i
        await assert_success({field: value})

    await assert_fail(
        {field: [1] * (max_length + 1)},
        error=(
            field,
            (field, 'maxlength'),
            errors.MAX_LENGTH,
            max_length,
            (max_length + 1,),
        ),
    )


@mark.asyncio
async def test_custom_datatype():
    class MyValidator(Validator):
        async def _validate_type_objectid(self, value):
            if re.match('[a-f0-9]{24}', value):
                return True

    schema = {'test_field': {'type': 'objectid'}}
    validator = MyValidator(schema)
    await assert_success({'test_field': '50ad188438345b1049c88a28'}, validator=validator)
    await assert_fail(
        {'test_field': 'hello'},
        validator=validator,
        error=('test_field', ('test_field', 'type'), errors.BAD_TYPE, 'objectid'),
    )


@mark.asyncio
async def test_custom_datatype_rule():
    class MyValidator(Validator):
        async def _validate_min_number(self, min_number, field, value):
            """{'type': 'number'}"""
            if value < min_number:
                self._error(field, 'Below the min')

        # TODO replace with TypeDefintion in next major release
        async def _validate_type_number(self, value):
            if isinstance(value, int):
                return True

    schema = {'test_field': {'min_number': 1, 'type': 'number'}}
    validator = MyValidator(schema)
    await assert_fail(
        {'test_field': '0'},
        validator=validator,
        error=('test_field', ('test_field', 'type'), errors.BAD_TYPE, 'number'),
    )
    await assert_fail(
        {'test_field': 0},
        validator=validator,
        error=('test_field', (), errors.CUSTOM, None, ('Below the min',)),
    )
    assert validator.errors == {'test_field': ['Below the min']}


@mark.asyncio
async def test_custom_validator():
    class MyValidator(Validator):
        async def _validate_isodd(self, isodd, field, value):
            """{'type': 'boolean'}"""
            if isodd and not bool(value & 1):
                self._error(field, 'Not an odd number')

    schema = {'test_field': {'isodd': True}}
    validator = MyValidator(schema)
    await assert_success({'test_field': 7}, validator=validator)
    await assert_fail(
        {'test_field': 6},
        validator=validator,
        error=('test_field', (), errors.CUSTOM, None, ('Not an odd number',)),
    )
    assert validator.errors == {'test_field': ['Not an odd number']}


@mark.parametrize(
    'value, _type', (('', 'string'), ((), 'list'), ({}, 'dict'), ([], 'list'))
)
@mark.asyncio
async def test_empty_values(value, _type):
    field = 'test'
    schema = {field: {'type': _type}}
    document = {field: value}

    await assert_success(document, schema)

    schema[field]['empty'] = False
    await assert_fail(
        document,
        schema,
        error=(field, (field, 'empty'), errors.EMPTY_NOT_ALLOWED, False),
    )

    schema[field]['empty'] = True
    await assert_success(document, schema)


@mark.asyncio
async def test_empty_skips_regex(validator):
    schema = {'foo': {'empty': True, 'regex': r'\d?\d\.\d\d', 'type': 'string'}}
    assert await validator({'foo': ''}, schema)


@mark.asyncio
async def test_ignore_none_values():
    field = 'test'
    schema = {field: {'type': 'string', 'empty': False, 'required': False}}
    document = {field: None}

    # Test normal behaviour
    validator = Validator(schema, ignore_none_values=False)
    await assert_fail(document, validator=validator)
    validator.schema[field]['required'] = True
    await validator.schema.validate()
    _errors = await assert_fail(document, validator=validator)
    assert_not_has_error(
        _errors, field, (field, 'required'), errors.REQUIRED_FIELD, True
    )

    # Test ignore None behaviour
    validator = Validator(schema, ignore_none_values=True)
    validator.schema[field]['required'] = False
    await validator.schema.validate()
    await assert_success(document, validator=validator)
    validator.schema[field]['required'] = True
    _errors = await assert_fail(schema=schema, document=document, validator=validator)
    assert_has_error(_errors, field, (field, 'required'), errors.REQUIRED_FIELD, True)
    assert_not_has_error(_errors, field, (field, 'type'), errors.BAD_TYPE, 'string')


@mark.asyncio
async def test_unknown_keys():
    schema = {}

    # test that unknown fields are allowed when allow_unknown is True.
    v = Validator(allow_unknown=True, schema=schema)
    await assert_success({"unknown1": True, "unknown2": "yes"}, validator=v)

    # test that unknown fields are allowed only if they meet the
    # allow_unknown schema when provided.
    v.allow_unknown = {'type': 'string'}
    await assert_success(document={'name': 'mark'}, validator=v)
    await assert_fail({"name": 1}, validator=v)

    # test that unknown fields are not allowed if allow_unknown is False
    v.allow_unknown = False
    await assert_fail({'name': 'mark'}, validator=v)


@mark.asyncio
async def test_unknown_key_dict(validator):
    # https://github.com/pyeve/cerberus/issues/177
    validator.allow_unknown = True
    document = {'a_dict': {'foo': 'foo_value', 'bar': 25}}
    await assert_success(document, {}, validator=validator)


@mark.asyncio
async def test_unknown_key_list(validator):
    # https://github.com/pyeve/cerberus/issues/177
    validator.allow_unknown = True
    document = {'a_dict': ['foo', 'bar']}
    await assert_success(document, {}, validator=validator)


@mark.asyncio
async def test_unknown_keys_list_of_dicts(validator):
    # test that allow_unknown is honored even for subdicts in lists.
    # https://github.com/pyeve/cerberus/issues/67.
    validator.allow_unknown = True
    document = {'a_list_of_dicts': [{'sku': 'YZ069', 'price': 25, 'extra': True}]}
    await assert_success(document, validator=validator)


@mark.asyncio
async def test_unknown_keys_retain_custom_rules():
    # test that allow_unknown schema respect custom validation rules.
    # https://github.com/pyeve/cerberus/issues/#66.
    class CustomValidator(Validator):
        def _validate_type_foo(self, value):
            if value == "foo":
                return True

    validator = CustomValidator({})
    validator.allow_unknown = {"type": "foo"}
    await assert_success(document={"fred": "foo", "barney": "foo"}, validator=validator)


@mark.asyncio
async def test_nested_unknown_keys():
    schema = {
        'field1': {
            'type': 'dict',
            'allow_unknown': True,
            'schema': {'nested1': {'type': 'string'}},
        }
    }
    document = {'field1': {'nested1': 'foo', 'arb1': 'bar', 'arb2': 42}}
    await assert_success(document=document, schema=schema)

    schema['field1']['allow_unknown'] = {'type': 'string'}
    await assert_fail(document=document, schema=schema)


def test_novalidate_noerrors(validator):
    """
    In v0.1.0 and below `self.errors` raised an exception if no
    validation had been performed yet.
    """
    assert validator.errors == {}


@mark.asyncio
async def test_callable_validator():
    """
    Validator instance is callable, functions as a shorthand
    passthrough to validate()
    """
    schema = {'test_field': {'type': 'string'}}
    v = Validator(schema)
    assert await v.validate({'test_field': 'foo'})
    assert await v({'test_field': 'foo'})
    assert not await v.validate({'test_field': 1})
    assert not await v({'test_field': 1})


@mark.asyncio
async def test_dependencies_field():
    schema = {'test_field': {'dependencies': 'foo'}, 'foo': {'type': 'string'}}
    await assert_success({'test_field': 'foobar', 'foo': 'bar'}, schema)
    await assert_fail({'test_field': 'foobar'}, schema)


@mark.asyncio
async def test_dependencies_list():
    schema = {
        'test_field': {'dependencies': ['foo', 'bar']},
        'foo': {'type': 'string'},
        'bar': {'type': 'string'},
    }
    await assert_success({'test_field': 'foobar', 'foo': 'bar', 'bar': 'foo'}, schema)
    await assert_fail({'test_field': 'foobar', 'foo': 'bar'}, schema)


@mark.asyncio
async def test_dependencies_list_with_required_field():
    schema = {
        'test_field': {'required': True, 'dependencies': ['foo', 'bar']},
        'foo': {'type': 'string'},
        'bar': {'type': 'string'},
    }
    # False: all dependencies missing
    await assert_fail({'test_field': 'foobar'}, schema)
    # False: one of dependencies missing
    await assert_fail({'test_field': 'foobar', 'foo': 'bar'}, schema)
    # False: one of dependencies missing
    await assert_fail({'test_field': 'foobar', 'bar': 'foo'}, schema)
    # False: dependencies are validated and field is required
    await assert_fail({'foo': 'bar', 'bar': 'foo'}, schema)
    # False: All dependencies are optional but field is still required
    await assert_fail({}, schema)
    # True: dependency missing
    await assert_fail({'foo': 'bar'}, schema)
    # True: dependencies are validated but field is not required
    schema['test_field']['required'] = False
    await assert_success({'foo': 'bar', 'bar': 'foo'}, schema)


@mark.asyncio
async def test_dependencies_list_with_subodcuments_fields():
    schema = {
        'test_field': {'dependencies': ['a_dict.foo', 'a_dict.bar']},
        'a_dict': {
            'type': 'dict',
            'schema': {'foo': {'type': 'string'}, 'bar': {'type': 'string'}},
        },
    }
    await assert_success(
        {'test_field': 'foobar', 'a_dict': {'foo': 'foo', 'bar': 'bar'}}, schema
    )
    await assert_fail({'test_field': 'foobar', 'a_dict': {}}, schema)
    await assert_fail({'test_field': 'foobar', 'a_dict': {'foo': 'foo'}}, schema)


@mark.asyncio
async def test_dependencies_dict():
    schema = {
        'test_field': {'dependencies': {'foo': 'foo', 'bar': 'bar'}},
        'foo': {'type': 'string'},
        'bar': {'type': 'string'},
    }
    await assert_success({'test_field': 'foobar', 'foo': 'foo', 'bar': 'bar'}, schema)
    await assert_fail({'test_field': 'foobar', 'foo': 'foo'}, schema)
    await assert_fail({'test_field': 'foobar', 'foo': 'bar'}, schema)
    await assert_fail({'test_field': 'foobar', 'bar': 'bar'}, schema)
    await assert_fail({'test_field': 'foobar', 'bar': 'foo'}, schema)
    await assert_fail({'test_field': 'foobar'}, schema)


@mark.asyncio
async def test_dependencies_dict_with_required_field():
    schema = {
        'test_field': {'required': True, 'dependencies': {'foo': 'foo', 'bar': 'bar'}},
        'foo': {'type': 'string'},
        'bar': {'type': 'string'},
    }
    # False: all dependencies missing
    await assert_fail({'test_field': 'foobar'}, schema)
    # False: one of dependencies missing
    await assert_fail({'test_field': 'foobar', 'foo': 'foo'}, schema)
    await assert_fail({'test_field': 'foobar', 'bar': 'bar'}, schema)
    # False: dependencies are validated and field is required
    await assert_fail({'foo': 'foo', 'bar': 'bar'}, schema)
    # False: All dependencies are optional, but field is still required
    await assert_fail({}, schema)
    # False: dependency missing
    await assert_fail({'foo': 'bar'}, schema)

    await assert_success({'test_field': 'foobar', 'foo': 'foo', 'bar': 'bar'}, schema)

    # True: dependencies are validated but field is not required
    schema['test_field']['required'] = False
    await assert_success({'foo': 'bar', 'bar': 'foo'}, schema)


@mark.asyncio
async def test_dependencies_field_satisfy_nullable_field():
    # https://github.com/pyeve/cerberus/issues/305
    schema = {'foo': {'nullable': True}, 'bar': {'dependencies': 'foo'}}

    await assert_success({'foo': None, 'bar': 1}, schema)
    await assert_success({'foo': None}, schema)
    await assert_fail({'bar': 1}, schema)


@mark.asyncio
async def test_dependencies_field_with_mutually_dependent_nullable_fields():
    # https://github.com/pyeve/cerberus/pull/306
    schema = {
        'foo': {'dependencies': 'bar', 'nullable': True},
        'bar': {'dependencies': 'foo', 'nullable': True},
    }
    await assert_success({'foo': None, 'bar': None}, schema)
    await assert_success({'foo': 1, 'bar': 1}, schema)
    await assert_success({'foo': None, 'bar': 1}, schema)
    await assert_fail({'foo': None}, schema)
    await assert_fail({'foo': 1}, schema)


@mark.asyncio
async def test_dependencies_dict_with_subdocuments_fields():
    schema = {
        'test_field': {
            'dependencies': {'a_dict.foo': ['foo', 'bar'], 'a_dict.bar': 'bar'}
        },
        'a_dict': {
            'type': 'dict',
            'schema': {'foo': {'type': 'string'}, 'bar': {'type': 'string'}},
        },
    }
    await assert_success(
        {'test_field': 'foobar', 'a_dict': {'foo': 'foo', 'bar': 'bar'}}, schema
    )
    await assert_success(
        {'test_field': 'foobar', 'a_dict': {'foo': 'bar', 'bar': 'bar'}}, schema
    )
    await assert_fail({'test_field': 'foobar', 'a_dict': {}}, schema)
    await assert_fail(
        {'test_field': 'foobar', 'a_dict': {'foo': 'foo', 'bar': 'foo'}}, schema
    )
    await assert_fail({'test_field': 'foobar', 'a_dict': {'bar': 'foo'}}, schema)
    await assert_fail({'test_field': 'foobar', 'a_dict': {'bar': 'bar'}}, schema)


@mark.asyncio
async def test_root_relative_dependencies():
    # https://github.com/pyeve/cerberus/issues/288
    subschema = {'version': {'dependencies': '^repo'}}
    schema = {'package': {'allow_unknown': True, 'schema': subschema}, 'repo': {}}
    await assert_fail(
        {'package': {'repo': 'somewhere', 'version': 0}},
        schema,
        error=('package', ('package', 'schema'), errors.MAPPING_SCHEMA, subschema),
        child_errors=[
            (
                ('package', 'version'),
                ('package', 'schema', 'version', 'dependencies'),
                errors.DEPENDENCIES_FIELD,
                '^repo',
                ('^repo',),
            )
        ],
    )
    await assert_success({'repo': 'somewhere', 'package': {'version': 1}}, schema)


@mark.asyncio
async def test_dependencies_errors():
    v = Validator(
        {
            'field1': {'required': False},
            'field2': {'required': True, 'dependencies': {'field1': ['one', 'two']}},
        }
    )
    await assert_fail(
        {'field1': 'three', 'field2': 7},
        validator=v,
        error=(
            'field2',
            ('field2', 'dependencies'),
            errors.DEPENDENCIES_FIELD_VALUE,
            {'field1': ['one', 'two']},
            ({'field1': 'three'},),
        ),
    )


@mark.asyncio
async def test_options_passed_to_nested_validators(validator):
    validator.schema = {
        'sub_dict': {'type': 'dict', 'schema': {'foo': {'type': 'string'}}}
    }
    validator.allow_unknown = True
    await assert_success({'sub_dict': {'foo': 'bar', 'unknown': True}}, validator=validator)


@mark.asyncio
async def test_self_root_document():
    """
    Make sure self.root_document is always the root document. See:
    * https://github.com/pyeve/cerberus/pull/42
    * https://github.com/pyeve/eve/issues/295
    """

    class MyValidator(Validator):
        def _validate_root_doc(self, root_doc, field, value):
            """{'type': 'boolean'}"""
            if 'sub' not in self.root_document or len(self.root_document['sub']) != 2:
                self._error(field, 'self.context is not the root doc!')

    schema = {
        'sub': {
            'type': 'list',
            'root_doc': True,
            'schema': {
                'type': 'dict',
                'schema': {'foo': {'type': 'string', 'root_doc': True}},
            },
        }
    }
    await assert_success(
        {'sub': [{'foo': 'bar'}, {'foo': 'baz'}]}, validator=MyValidator(schema)
    )


@mark.asyncio
async def test_validator_rule(validator):
    def validate_name(field, value, error):
        if not value.islower():
            error(field, 'must be lowercase')

    validator.schema = {
        'name': {'validator': validate_name},
        'age': {'type': 'integer'},
    }

    await assert_fail(
        {'name': 'ItsMe', 'age': 2},
        validator=validator,
        error=('name', (), errors.CUSTOM, None, ('must be lowercase',)),
    )
    assert validator.errors == {'name': ['must be lowercase']}
    await assert_success({'name': 'itsme', 'age': 2}, validator=validator)


@mark.asyncio
async def test_validated(validator):
    validator.schema = {'property': {'type': 'string'}}
    document = {'property': 'string'}
    assert (await validator.validated(document)) == document
    document = {'property': 0}
    assert (await validator.validated(document)) is None


@mark.asyncio
async def test_anyof():
    # prop1 must be either a number between 0 and 10
    schema = {'prop1': {'min': 0, 'max': 10}}
    doc = {'prop1': 5}

    await assert_success(doc, schema)

    # prop1 must be either a number between 0 and 10 or 100 and 110
    schema = {'prop1': {'anyof': [{'min': 0, 'max': 10}, {'min': 100, 'max': 110}]}}
    doc = {'prop1': 105}

    await assert_success(doc, schema)

    # prop1 must be either a number between 0 and 10 or 100 and 110
    schema = {'prop1': {'anyof': [{'min': 0, 'max': 10}, {'min': 100, 'max': 110}]}}
    doc = {'prop1': 50}

    await assert_fail(doc, schema)

    # prop1 must be an integer that is either be
    # greater than or equal to 0, or greater than or equal to 10
    schema = {'prop1': {'type': 'integer', 'anyof': [{'min': 0}, {'min': 10}]}}
    await assert_success({'prop1': 10}, schema)
    # test that intermediate schemas do not sustain
    assert 'type' not in schema['prop1']['anyof'][0]
    assert 'type' not in schema['prop1']['anyof'][1]
    assert 'allow_unknown' not in schema['prop1']['anyof'][0]
    assert 'allow_unknown' not in schema['prop1']['anyof'][1]
    await assert_success({'prop1': 5}, schema)

    exp_child_errors = [
        (('prop1',), ('prop1', 'anyof', 0, 'min'), errors.MIN_VALUE, 0),
        (('prop1',), ('prop1', 'anyof', 1, 'min'), errors.MIN_VALUE, 10),
    ]
    await assert_fail(
        {'prop1': -1},
        schema,
        error=(('prop1',), ('prop1', 'anyof'), errors.ANYOF, [{'min': 0}, {'min': 10}]),
        child_errors=exp_child_errors,
    )
    doc = {'prop1': 5.5}
    await assert_fail(doc, schema)
    doc = {'prop1': '5.5'}
    await assert_fail(doc, schema)


@mark.asyncio
async def test_allof():
    # prop1 has to be a float between 0 and 10
    schema = {'prop1': {'allof': [{'type': 'float'}, {'min': 0}, {'max': 10}]}}
    doc = {'prop1': -1}
    await assert_fail(doc, schema)
    doc = {'prop1': 5}
    await assert_success(doc, schema)
    doc = {'prop1': 11}
    await assert_fail(doc, schema)

    # prop1 has to be a float and an integer
    schema = {'prop1': {'allof': [{'type': 'float'}, {'type': 'integer'}]}}
    doc = {'prop1': 11}
    await assert_success(doc, schema)
    doc = {'prop1': 11.5}
    await assert_fail(doc, schema)
    doc = {'prop1': '11'}
    await assert_fail(doc, schema)


@mark.asyncio
async def test_unicode_allowed():
    # issue 280
    doc = {'letters': u'♄εℓł☺'}

    schema = {'letters': {'type': 'string', 'allowed': ['a', 'b', 'c']}}
    await assert_fail(doc, schema)

    schema = {'letters': {'type': 'string', 'allowed': [u'♄εℓł☺']}}
    await assert_success(doc, schema)

    schema = {'letters': {'type': 'string', 'allowed': ['♄εℓł☺']}}
    doc = {'letters': '♄εℓł☺'}
    await assert_success(doc, schema)


@mark.skipif(sys.version_info[0] < 3, reason='requires python 3.x')
@mark.asyncio
async def test_unicode_allowed_py3():
    """
    All strings are unicode in Python 3.x. Input doc and schema have equal strings and
    validation yield success.
    """

    # issue 280
    doc = {'letters': u'♄εℓł☺'}
    schema = {'letters': {'type': 'string', 'allowed': ['♄εℓł☺']}}
    await assert_success(doc, schema)


@mark.skipif(sys.version_info[0] > 2, reason='requires python 2.x')
@mark.asyncio
async def test_unicode_allowed_py2():
    """
    Python 2.x encodes value of allowed using default encoding if the string includes
    characters outside ASCII range. Produced string does not match input which is an
    unicode string.
    """

    # issue 280
    doc = {'letters': u'♄εℓł☺'}
    schema = {'letters': {'type': 'string', 'allowed': ['♄εℓł☺']}}
    await assert_fail(doc, schema)


@mark.asyncio
async def test_oneof():
    # prop1 can only only be:
    # - greater than 10
    # - greater than 0
    # - equal to -5, 5, or 15

    schema = {
        'prop1': {
            'type': 'integer',
            'oneof': [{'min': 0}, {'min': 10}, {'allowed': [-5, 5, 15]}],
        }
    }

    # document is not valid
    # prop1 not greater than 0, 10 or equal to -5
    doc = {'prop1': -1}
    await assert_fail(doc, schema)

    # document is valid
    # prop1 is less then 0, but is -5
    doc = {'prop1': -5}
    await assert_success(doc, schema)

    # document is valid
    # prop1 greater than 0
    doc = {'prop1': 1}
    await assert_success(doc, schema)

    # document is not valid
    # prop1 is greater than 0
    # and equal to 5
    doc = {'prop1': 5}
    await assert_fail(doc, schema)

    # document is not valid
    # prop1 is greater than 0
    # and greater than 10
    doc = {'prop1': 11}
    await assert_fail(doc, schema)

    # document is not valid
    # prop1 is greater than 0
    # and greater than 10
    # and equal to 15
    doc = {'prop1': 15}
    await assert_fail(doc, schema)


@mark.asyncio
async def test_noneof():
    # prop1 can not be:
    # - greater than 10
    # - greater than 0
    # - equal to -5, 5, or 15

    schema = {
        'prop1': {
            'type': 'integer',
            'noneof': [{'min': 0}, {'min': 10}, {'allowed': [-5, 5, 15]}],
        }
    }

    # document is valid
    doc = {'prop1': -1}
    await assert_success(doc, schema)

    # document is not valid
    # prop1 is equal to -5
    doc = {'prop1': -5}
    await assert_fail(doc, schema)

    # document is not valid
    # prop1 greater than 0
    doc = {'prop1': 1}
    await assert_fail(doc, schema)

    # document is not valid
    doc = {'prop1': 5}
    await assert_fail(doc, schema)

    # document is not valid
    doc = {'prop1': 11}
    await assert_fail(doc, schema)

    # document is not valid
    # and equal to 15
    doc = {'prop1': 15}
    await assert_fail(doc, schema)


@mark.asyncio
async def test_anyof_allof():
    # prop1 can be any number outside of [0-10]
    schema = {
        'prop1': {
            'allof': [
                {'anyof': [{'type': 'float'}, {'type': 'integer'}]},
                {'anyof': [{'min': 10}, {'max': 0}]},
            ]
        }
    }

    doc = {'prop1': 11}
    await assert_success(doc, schema)
    doc = {'prop1': -1}
    await assert_success(doc, schema)
    doc = {'prop1': 5}
    await assert_fail(doc, schema)

    doc = {'prop1': 11.5}
    await assert_success(doc, schema)
    doc = {'prop1': -1.5}
    await assert_success(doc, schema)
    doc = {'prop1': 5.5}
    await assert_fail(doc, schema)

    doc = {'prop1': '5.5'}
    await assert_fail(doc, schema)


@mark.asyncio
async def test_anyof_schema(validator):
    # test that a list of schemas can be specified.

    valid_parts = [
        {'schema': {'model number': {'type': 'string'}, 'count': {'type': 'integer'}}},
        {'schema': {'serial number': {'type': 'string'}, 'count': {'type': 'integer'}}},
    ]
    valid_item = {'type': ['dict', 'string'], 'anyof': valid_parts}
    schema = {'parts': {'type': 'list', 'schema': valid_item}}
    document = {
        'parts': [
            {'model number': 'MX-009', 'count': 100},
            {'serial number': '898-001'},
            'misc',
        ]
    }

    # document is valid. each entry in 'parts' matches a type or schema
    await assert_success(document, schema, validator=validator)

    document['parts'].append({'product name': "Monitors", 'count': 18})
    # document is invalid. 'product name' does not match any valid schemas
    await assert_fail(document, schema, validator=validator)

    document['parts'].pop()
    # document is valid again
    await assert_success(document, schema, validator=validator)

    document['parts'].append({'product name': "Monitors", 'count': 18})
    document['parts'].append(10)
    # and invalid. numbers are not allowed.

    exp_child_errors = [
        (('parts', 3), ('parts', 'schema', 'anyof'), errors.ANYOF, valid_parts),
        (
            ('parts', 4),
            ('parts', 'schema', 'type'),
            errors.BAD_TYPE,
            ['dict', 'string'],
        ),
    ]

    _errors = await assert_fail(
        document,
        schema,
        validator=validator,
        error=('parts', ('parts', 'schema'), errors.SEQUENCE_SCHEMA, valid_item),
        child_errors=exp_child_errors,
    )
    assert_not_has_error(
        _errors, ('parts', 4), ('parts', 'schema', 'anyof'), errors.ANYOF, valid_parts
    )

    # tests errors.BasicErrorHandler's tree representation
    v_errors = validator.errors
    assert 'parts' in v_errors
    assert 3 in v_errors['parts'][-1]
    assert v_errors['parts'][-1][3][0] == "no definitions validate"
    scope = v_errors['parts'][-1][3][-1]
    assert 'anyof definition 0' in scope
    assert 'anyof definition 1' in scope
    assert scope['anyof definition 0'] == [{"product name": ["unknown field"]}]
    assert scope['anyof definition 1'] == [{"product name": ["unknown field"]}]
    assert v_errors['parts'][-1][4] == ["must be of ['dict', 'string'] type"]


@mark.asyncio
async def test_anyof_2():
    # these two schema should be the same
    schema1 = {
        'prop': {
            'anyof': [
                {'type': 'dict', 'schema': {'val': {'type': 'integer'}}},
                {'type': 'dict', 'schema': {'val': {'type': 'string'}}},
            ]
        }
    }
    schema2 = {
        'prop': {
            'type': 'dict',
            'anyof': [
                {'schema': {'val': {'type': 'integer'}}},
                {'schema': {'val': {'type': 'string'}}},
            ],
        }
    }

    doc = {'prop': {'val': 0}}
    await assert_success(doc, schema1)
    await assert_success(doc, schema2)

    doc = {'prop': {'val': '0'}}
    await assert_success(doc, schema1)
    await assert_success(doc, schema2)

    doc = {'prop': {'val': 1.1}}
    await assert_fail(doc, schema1)
    await assert_fail(doc, schema2)


@mark.asyncio
async def test_anyof_type():
    schema = {'anyof_type': {'anyof_type': ['string', 'integer']}}
    await assert_success({'anyof_type': 'bar'}, schema)
    await assert_success({'anyof_type': 23}, schema)


@mark.asyncio
async def test_oneof_schema():
    schema = {
        'oneof_schema': {
            'type': 'dict',
            'oneof_schema': [
                {'digits': {'type': 'integer', 'min': 0, 'max': 99}},
                {'text': {'type': 'string', 'regex': '^[0-9]{2}$'}},
            ],
        }
    }
    await assert_success({'oneof_schema': {'digits': 19}}, schema)
    await assert_success({'oneof_schema': {'text': '84'}}, schema)
    await assert_fail({'oneof_schema': {'digits': 19, 'text': '84'}}, schema)


@mark.asyncio
async def test_nested_oneof_type():
    schema = {
        'nested_oneof_type': {'valuesrules': {'oneof_type': ['string', 'integer']}}
    }
    await assert_success({'nested_oneof_type': {'foo': 'a'}}, schema)
    await assert_success({'nested_oneof_type': {'bar': 3}}, schema)


@mark.asyncio
async def test_nested_oneofs(validator):
    validator.schema = {
        'abc': {
            'type': 'dict',
            'oneof_schema': [
                {
                    'foo': {
                        'type': 'dict',
                        'schema': {'bar': {'oneof_type': ['integer', 'float']}},
                    }
                },
                {'baz': {'type': 'string'}},
            ],
        }
    }

    document = {'abc': {'foo': {'bar': 'bad'}}}

    expected_errors = {
        'abc': [
            'none or more than one rule validate',
            {
                'oneof definition 0': [
                    {
                        'foo': [
                            {
                                'bar': [
                                    'none or more than one rule validate',
                                    {
                                        'oneof definition 0': [
                                            'must be of integer type'
                                        ],
                                        'oneof definition 1': ['must be of float type'],
                                    },
                                ]
                            }
                        ]
                    }
                ],
                'oneof definition 1': [{'foo': ['unknown field']}],
            },
        ]
    }

    await assert_fail(document, validator=validator)
    assert validator.errors == expected_errors


@mark.asyncio
async def test_no_of_validation_if_type_fails(validator):
    valid_parts = [
        {'schema': {'model number': {'type': 'string'}, 'count': {'type': 'integer'}}},
        {'schema': {'serial number': {'type': 'string'}, 'count': {'type': 'integer'}}},
    ]
    validator.schema = {'part': {'type': ['dict', 'string'], 'anyof': valid_parts}}
    document = {'part': 10}
    _errors = await assert_fail(document, validator=validator)
    assert len(_errors) == 1


@mark.asyncio
async def test_issue_107(validator):
    schema = {
        'info': {
            'type': 'dict',
            'schema': {'name': {'type': 'string', 'required': True}},
        }
    }
    document = {'info': {'name': 'my name'}}
    await assert_success(document, schema, validator=validator)

    v = Validator(schema)
    await assert_success(document, schema, v)
    # it once was observed that this behaves other than the previous line
    assert await v.validate(document)


@mark.asyncio
async def test_dont_type_validate_nulled_values(validator):
    await assert_fail({'an_integer': None}, validator=validator)
    assert validator.errors == {'an_integer': ['null value not allowed']}


@mark.asyncio
async def test_dependencies_error(validator):
    schema = {
        'field1': {'required': False},
        'field2': {'required': True, 'dependencies': {'field1': ['one', 'two']}},
    }
    await validator.validate({'field2': 7}, schema)
    exp_msg = errors.BasicErrorHandler.messages[
        errors.DEPENDENCIES_FIELD_VALUE.code
    ].format(field='field2', constraint={'field1': ['one', 'two']})
    assert validator.errors == {'field2': [exp_msg]}


@mark.asyncio
async def test_dependencies_on_boolean_field_with_one_value():
    # https://github.com/pyeve/cerberus/issues/138
    schema = {
        'deleted': {'type': 'boolean'},
        'text': {'dependencies': {'deleted': False}},
    }
    try:
        await assert_success({'text': 'foo', 'deleted': False}, schema)
        await assert_fail({'text': 'foo', 'deleted': True}, schema)
        await assert_fail({'text': 'foo'}, schema)
    except TypeError as e:
        if str(e) == "argument of type 'bool' is not iterable":
            raise AssertionError(
                "Bug #138 still exists, couldn't use boolean in dependency "
                "without putting it in a list.\n"
                "'some_field': True vs 'some_field: [True]"
            )
        else:
            raise


@mark.asyncio
async def test_dependencies_on_boolean_field_with_value_in_list():
    # https://github.com/pyeve/cerberus/issues/138
    schema = {
        'deleted': {'type': 'boolean'},
        'text': {'dependencies': {'deleted': [False]}},
    }

    await assert_success({'text': 'foo', 'deleted': False}, schema)
    await assert_fail({'text': 'foo', 'deleted': True}, schema)
    await assert_fail({'text': 'foo'}, schema)


@mark.asyncio
async def test_document_path():
    class DocumentPathTester(Validator):
        async def _validate_trail(self, constraint, field, value):
            """{'type': 'boolean'}"""
            test_doc = self.root_document
            for crumb in self.document_path:
                test_doc = test_doc[crumb]
            assert test_doc == self.document

    v = DocumentPathTester()
    schema = {'foo': {'schema': {'bar': {'trail': True}}}}
    document = {'foo': {'bar': {}}}
    await assert_success(document, schema, validator=v)


@mark.asyncio
async def test_excludes():
    schema = {
        'this_field': {'type': 'dict', 'excludes': 'that_field'},
        'that_field': {'type': 'dict'},
    }
    await assert_success({'this_field': {}}, schema)
    await assert_success({'that_field': {}}, schema)
    await assert_success({}, schema)
    await assert_fail({'that_field': {}, 'this_field': {}}, schema)


@mark.asyncio
async def test_mutual_excludes():
    schema = {
        'this_field': {'type': 'dict', 'excludes': 'that_field'},
        'that_field': {'type': 'dict', 'excludes': 'this_field'},
    }
    await assert_success({'this_field': {}}, schema)
    await assert_success({'that_field': {}}, schema)
    await assert_success({}, schema)
    await assert_fail({'that_field': {}, 'this_field': {}}, schema)


@mark.asyncio
async def test_required_excludes():
    schema = {
        'this_field': {'type': 'dict', 'excludes': 'that_field', 'required': True},
        'that_field': {'type': 'dict', 'excludes': 'this_field', 'required': True},
    }
    await assert_success({'this_field': {}}, schema, update=False)
    await assert_success({'that_field': {}}, schema, update=False)
    await assert_fail({}, schema)
    await assert_fail({'that_field': {}, 'this_field': {}}, schema)


@mark.asyncio
async def test_multiples_exclusions():
    schema = {
        'this_field': {'type': 'dict', 'excludes': ['that_field', 'bazo_field']},
        'that_field': {'type': 'dict', 'excludes': 'this_field'},
        'bazo_field': {'type': 'dict'},
    }
    await assert_success({'this_field': {}}, schema)
    await assert_success({'that_field': {}}, schema)
    await assert_fail({'this_field': {}, 'that_field': {}}, schema)
    await assert_fail({'this_field': {}, 'bazo_field': {}}, schema)
    await assert_fail({'that_field': {}, 'this_field': {}, 'bazo_field': {}}, schema)
    await assert_success({'that_field': {}, 'bazo_field': {}}, schema)


@mark.asyncio
async def test_bad_excludes_fields(validator):
    validator.schema = {
        'this_field': {
            'type': 'dict',
            'excludes': ['that_field', 'bazo_field'],
            'required': True,
        },
        'that_field': {'type': 'dict', 'excludes': 'this_field', 'required': True},
    }
    await assert_fail({'that_field': {}, 'this_field': {}}, validator=validator)
    handler = errors.BasicErrorHandler
    assert validator.errors == {
        'that_field': [
            handler.messages[errors.EXCLUDES_FIELD.code].format(
                "'this_field'", field="that_field"
            )
        ],
        'this_field': [
            handler.messages[errors.EXCLUDES_FIELD.code].format(
                "'that_field', 'bazo_field'", field="this_field"
            )
        ],
    }


@mark.asyncio
async def test_boolean_is_not_a_number():
    # https://github.com/pyeve/cerberus/issues/144
    await assert_fail({'value': True}, {'value': {'type': 'number'}})


@mark.asyncio
async def test_min_max_date():
    schema = {'date': {'min': date(1900, 1, 1), 'max': date(1999, 12, 31)}}
    await assert_success({'date': date(1945, 5, 8)}, schema)
    await assert_fail({'date': date(1871, 5, 10)}, schema)


@mark.asyncio
async def test_dict_length():
    schema = {'dict': {'minlength': 1}}
    await assert_fail({'dict': {}}, schema)
    await assert_success({'dict': {'foo': 'bar'}}, schema)


@mark.asyncio
async def test_forbidden():
    schema = {'user': {'forbidden': ['root', 'admin']}}
    await assert_fail({'user': 'admin'}, schema)
    await assert_success({'user': 'alice'}, schema)


@mark.asyncio
async def test_forbidden_number():
    schema = {'amount': {'forbidden': (0, 0.0)}}
    await assert_fail({'amount': 0}, schema)
    await assert_fail({'amount': 0.0}, schema)


@mark.asyncio
async def test_mapping_with_sequence_schema():
    schema = {'list': {'schema': {'allowed': ['a', 'b', 'c']}}}
    document = {'list': {'is_a': 'mapping'}}
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
async def test_sequence_with_mapping_schema():
    schema = {'list': {'schema': {'foo': {'allowed': ['a', 'b', 'c']}}, 'type': 'dict'}}
    document = {'list': ['a', 'b', 'c']}
    await assert_fail(document, schema)


@mark.asyncio
async def test_type_error_aborts_validation():
    schema = {'foo': {'type': 'string', 'allowed': ['a']}}
    document = {'foo': 0}
    await assert_fail(
        document, schema, error=('foo', ('foo', 'type'), errors.BAD_TYPE, 'string')
    )


@mark.asyncio
async def test_dependencies_in_oneof():
    # https://github.com/pyeve/cerberus/issues/241
    schema = {
        'a': {
            'type': 'integer',
            'oneof': [
                {'allowed': [1], 'dependencies': 'b'},
                {'allowed': [2], 'dependencies': 'c'},
            ],
        },
        'b': {},
        'c': {},
    }
    await assert_success({'a': 1, 'b': 'foo'}, schema)
    await assert_success({'a': 2, 'c': 'bar'}, schema)
    await assert_fail({'a': 1, 'c': 'foo'}, schema)
    await assert_fail({'a': 2, 'b': 'bar'}, schema)


@mark.asyncio
async def test_allow_unknown_with_oneof_rules(validator):
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
    await validator(document, schema)
    _errors = validator._errors
    assert len(_errors) == 1
    assert_has_error(
        _errors, 'test', ('test', 'oneof'), errors.ONEOF, schema['test']['oneof']
    )
    assert len(_errors[0].child_errors) == 0
    # check that allow_unknown is actually applied
    document = {'test': {'known': 's', 'unknown': 'asd'}}
    await assert_success(document, validator=validator)


@mark.parametrize('constraint', (('Graham Chapman', 'Eric Idle'), 'Terry Gilliam'))
@mark.asyncio
async def test_contains(constraint):
    validator = Validator({'actors': {'contains': constraint}})

    document = {'actors': ('Graham Chapman', 'Eric Idle', 'Terry Gilliam')}
    assert await validator(document)

    document = {'actors': ('Eric idle', 'Terry Jones', 'John Cleese', 'Michael Palin')}
    assert not await validator(document)
    assert errors.MISSING_MEMBERS in validator.document_error_tree['actors']
    missing_actors = validator.document_error_tree['actors'][
        errors.MISSING_MEMBERS
    ].info[0]
    assert any(x in missing_actors for x in ('Eric Idle', 'Terry Gilliam'))


@mark.asyncio
async def test_require_all_simple():
    schema = {'foo': {'type': 'string'}}
    validator = Validator(require_all=True)
    await assert_fail(
        {},
        schema,
        validator,
        error=('foo', '__require_all__', errors.REQUIRED_FIELD, True),
    )
    await assert_success({'foo': 'bar'}, schema, validator)
    validator.require_all = False
    await assert_success({}, schema, validator)
    await assert_success({'foo': 'bar'}, schema, validator)


@mark.asyncio
async def test_require_all_override_by_required():
    schema = {'foo': {'type': 'string', 'required': False}}
    validator = Validator(require_all=True)
    await assert_success({}, schema, validator)
    await assert_success({'foo': 'bar'}, schema, validator)
    validator.require_all = False
    await assert_success({}, schema, validator)
    await assert_success({'foo': 'bar'}, schema, validator)

    schema = {'foo': {'type': 'string', 'required': True}}
    validator.require_all = True
    await assert_fail(
        {},
        schema,
        validator,
        error=('foo', ('foo', 'required'), errors.REQUIRED_FIELD, True),
    )
    await assert_success({'foo': 'bar'}, schema, validator)
    validator.require_all = False
    await assert_fail(
        {},
        schema,
        validator,
        error=('foo', ('foo', 'required'), errors.REQUIRED_FIELD, True),
    )
    await assert_success({'foo': 'bar'}, schema, validator)


@mark.parametrize(
    "validator_require_all, sub_doc_require_all",
    list(itertools.product([True, False], repeat=2)),
)
@mark.asyncio
async def test_require_all_override_by_subdoc_require_all(
    validator_require_all, sub_doc_require_all
):
    sub_schema = {"bar": {"type": "string"}}
    schema = {
        "foo": {
            "type": "dict",
            "require_all": sub_doc_require_all,
            "schema": sub_schema,
        }
    }
    validator = Validator(require_all=validator_require_all)

    await assert_success({"foo": {"bar": "baz"}}, schema, validator)
    if validator_require_all:
        await assert_fail({}, schema, validator)
    else:
        await assert_success({}, schema, validator)
    if sub_doc_require_all:
        await assert_fail({"foo": {}}, schema, validator)
    else:
        await assert_success({"foo": {}}, schema, validator)


@mark.asyncio
async def test_require_all_and_exclude():
    schema = {
        'foo': {'type': 'string', 'excludes': 'bar'},
        'bar': {'type': 'string', 'excludes': 'foo'},
    }
    validator = Validator(require_all=True)
    await assert_fail(
        {},
        schema,
        validator,
        errors=[
            ('foo', '__require_all__', errors.REQUIRED_FIELD, True),
            ('bar', '__require_all__', errors.REQUIRED_FIELD, True),
        ],
    )
    await assert_success({'foo': 'value'}, schema, validator)
    await assert_success({'bar': 'value'}, schema, validator)
    await assert_fail({'foo': 'value', 'bar': 'value'}, schema, validator)
    validator.require_all = False
    await assert_success({}, schema, validator)
    await assert_success({'foo': 'value'}, schema, validator)
    await assert_success({'bar': 'value'}, schema, validator)
    await assert_fail({'foo': 'value', 'bar': 'value'}, schema, validator)


@mark.asyncio
async def test_allowed_when_passing_list_of_dicts():
    # https://github.com/pyeve/cerberus/issues/524
    doc = {'letters': [{'some': 'dict'}]}
    schema = {'letters': {'type': 'list', 'allowed': ['a', 'b', 'c']}}

    await assert_fail(
        doc,
        schema,
        error=(
            'letters',
            ('letters', 'allowed'),
            errors.UNALLOWED_VALUES,
            ['a', 'b', 'c'],
            (({'some': 'dict'},),),
        ),
    )
