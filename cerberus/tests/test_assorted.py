# -*- coding: utf-8 -*-

from decimal import Decimal

from pytest import mark

from cerberus import TypeDefinition, Validator
from cerberus.tests import assert_fail, assert_success
from cerberus.utils import validator_factory
from cerberus.validator import BareValidator


@mark.asyncio
async def test_clear_cache(validator):
    # TODO: Figure out why this wasn't called on construction of the Validator instance
    await validator.schema.validate()
    assert len(validator._valid_schemas) > 0
    validator.clear_caches()
    assert len(validator._valid_schemas) == 0


def test_docstring(validator):
    assert validator.__doc__


# Test that testing with the sample schema works as expected
# as there might be rules with side-effects in it


@mark.parametrize(
    "test,document",
    ((assert_fail, {"an_integer": 60}), (assert_success, {"an_integer": 110})),
)
@mark.asyncio
async def test_that_test_fails(test, document):
    try:
        await test(document)
    except AssertionError:
        pass
    else:
        raise AssertionError("test didn't fail")


@mark.asyncio
async def test_dynamic_types():
    decimal_type = TypeDefinition("decimal", (Decimal,), ())
    document = {"measurement": Decimal(0)}
    schema = {"measurement": {"type": "decimal"}}

    validator = Validator()
    validator.types_mapping["decimal"] = decimal_type
    await assert_success(document, schema, validator)

    class MyValidator(Validator):
        types_mapping = Validator.types_mapping.copy()
        types_mapping["decimal"] = decimal_type

    validator = MyValidator()
    await assert_success(document, schema, validator)


def test_mro():
    assert Validator.__mro__ == (Validator, BareValidator, object), Validator.__mro__


def test_mixin_init():
    class Mixin(object):
        def __init__(self, *args, **kwargs):
            kwargs["test"] = True
            super(Mixin, self).__init__(*args, **kwargs)

    MyValidator = validator_factory("MyValidator", Mixin)
    validator = MyValidator()
    assert validator._config["test"]


def test_sub_init():
    class MyValidator(Validator):
        def __init__(self, *args, **kwargs):
            kwargs["test"] = True
            super(MyValidator, self).__init__(*args, **kwargs)

    validator = MyValidator()
    assert validator._config["test"]
