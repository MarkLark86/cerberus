# -*- coding: utf-8 -*-

from pytest import mark
from cerberus import schema_registry, rules_set_registry, Validator
from cerberus.tests import (
    assert_fail,
    assert_normalized,
    assert_schema_error,
    assert_success,
)


@mark.asyncio
async def test_schema_registry_simple():
    schema_registry.add('foo', {'bar': {'type': 'string'}})
    schema = {'a': {'schema': 'foo'}, 'b': {'schema': 'foo'}}
    document = {'a': {'bar': 'a'}, 'b': {'bar': 'b'}}
    await assert_success(document, schema)


@mark.asyncio
async def test_top_level_reference():
    schema_registry.add('peng', {'foo': {'type': 'integer'}})
    document = {'foo': 42}
    await assert_success(document, 'peng')


@mark.asyncio
async def test_rules_set_simple():
    rules_set_registry.add('foo', {'type': 'integer'})
    await assert_success({'bar': 1}, {'bar': 'foo'})
    await assert_fail({'bar': 'one'}, {'bar': 'foo'})


@mark.asyncio
async def test_allow_unknown_as_reference():
    rules_set_registry.add('foo', {'type': 'number'})
    v = Validator(allow_unknown='foo')
    await assert_success({0: 1}, {}, v)
    await assert_fail({0: 'one'}, {}, v)


@mark.asyncio
async def test_recursion():
    rules_set_registry.add('self', {'type': 'dict', 'allow_unknown': 'self'})
    v = Validator(allow_unknown='self')
    await assert_success({0: {1: {2: {}}}}, {}, v)


def test_references_remain_unresolved(validator):
    rules_set_registry.extend(
        (('boolean', {'type': 'boolean'}), ('booleans', {'valuesrules': 'boolean'}))
    )
    validator.schema = {'foo': 'booleans'}
    assert 'booleans' == validator.schema['foo']
    assert 'boolean' == rules_set_registry._storage['booleans']['valuesrules']


@mark.asyncio
async def test_rules_registry_with_anyof_type():
    rules_set_registry.add('string_or_integer', {'anyof_type': ['string', 'integer']})
    schema = {'soi': 'string_or_integer'}
    await assert_success({'soi': 'hello'}, schema)


@mark.asyncio
async def test_schema_registry_with_anyof_type():
    schema_registry.add('soi_id', {'id': {'anyof_type': ['string', 'integer']}})
    schema = {'soi': {'schema': 'soi_id'}}
    await assert_success({'soi': {'id': 'hello'}}, schema)


@mark.asyncio
async def test_normalization_with_rules_set():
    # https://github.com/pyeve/cerberus/issues/283
    rules_set_registry.add('foo', {'default': 42})
    await assert_normalized({}, {'bar': 42}, {'bar': 'foo'})
    rules_set_registry.add('foo', {'default_setter': lambda _: 42})
    await assert_normalized({}, {'bar': 42}, {'bar': 'foo'})
    rules_set_registry.add('foo', {'type': 'integer', 'nullable': True})
    await assert_success({'bar': None}, {'bar': 'foo'})


@mark.asyncio
async def test_rules_set_with_dict_field():
    document = {'a_dict': {'foo': 1}}
    schema = {'a_dict': {'type': 'dict', 'schema': {'foo': 'rule'}}}

    # the schema's not yet added to the valid ones, so test the faulty first
    rules_set_registry.add('rule', {'tüpe': 'integer'})
    await assert_schema_error(document, schema)

    rules_set_registry.add('rule', {'type': 'integer'})
    await assert_success(document, schema)
