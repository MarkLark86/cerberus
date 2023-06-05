# -*- coding: utf-8 -*-

import re

import pytest

from cerberus import errors, Validator, SchemaError, DocumentError
from cerberus.tests.conftest import sample_schema


async def assert_exception(exception, document={}, schema=None, validator=None, msg=None):
    """
    Tests whether a specific exception is raised. Optionally also tests whether the
    exception message is as expected.
    """
    if validator is None:
        validator = Validator()
    if msg is None:
        with pytest.raises(exception):
            await validator(document, schema)
    else:
        with pytest.raises(exception, match=re.escape(msg)):
            await validator(document, schema)


async def assert_schema_error(*args):
    """Tests whether a validation raises an exception due to a malformed schema."""
    await assert_exception(SchemaError, *args)


async def assert_document_error(*args):
    """Tests whether a validation raises an exception due to a malformed document."""
    await assert_exception(DocumentError, *args)


async def assert_fail(
    document,
    schema=None,
    validator=None,
    update=False,
    error=None,
    errors=None,
    child_errors=None,
):
    """Tests whether a validation fails."""
    if validator is None:
        validator = Validator(sample_schema)
    result = await validator(document, schema, update)
    assert isinstance(result, bool)
    assert not result

    actual_errors = validator._errors

    assert not (error is not None and errors is not None)
    assert not (errors is not None and child_errors is not None), (
        'child_errors can only be tested in ' 'conjunction with the error parameter'
    )
    assert not (child_errors is not None and error is None)
    if error is not None:
        assert len(actual_errors) == 1
        assert_has_error(actual_errors, *error)

        if child_errors is not None:
            assert len(actual_errors[0].child_errors) == len(child_errors)
            assert_has_errors(actual_errors[0].child_errors, child_errors)

    elif errors is not None:
        assert len(actual_errors) == len(errors)
        assert_has_errors(actual_errors, errors)

    return actual_errors


async def assert_success(document, schema=None, validator=None, update=False):
    """Tests whether a validation succeeds."""
    if validator is None:
        validator = Validator(sample_schema)
    result = await validator(document, schema, update)
    assert isinstance(result, bool)
    if not result:
        raise AssertionError(validator.errors)


def assert_has_error(_errors, d_path, s_path, error_def, constraint, info=()):
    if not isinstance(d_path, tuple):
        d_path = (d_path,)
    if not isinstance(info, tuple):
        info = (info,)

    assert isinstance(_errors, errors.ErrorList)

    for i, error in enumerate(_errors):
        assert isinstance(error, errors.ValidationError)
        try:
            assert error.document_path == d_path
            assert error.schema_path == s_path
            assert error.code == error_def.code
            assert error.rule == error_def.rule
            assert error.constraint == constraint
            if not error.is_group_error:
                assert error.info == info
        except AssertionError:
            pass
        except Exception:
            raise
        else:
            break
    else:
        raise AssertionError(
            """
        Error with properties:
          document_path={doc_path}
          schema_path={schema_path}
          code={code}
          constraint={constraint}
          info={info}
        not found in errors:
        {errors}
        """.format(
                doc_path=d_path,
                schema_path=s_path,
                code=hex(error.code),
                info=info,
                constraint=constraint,
                errors=_errors,
            )
        )
    return i


def assert_has_errors(_errors, _exp_errors):
    assert isinstance(_exp_errors, list)
    for error in _exp_errors:
        assert isinstance(error, tuple)
        assert_has_error(_errors, *error)


def assert_not_has_error(_errors, *args, **kwargs):
    try:
        assert_has_error(_errors, *args, **kwargs)
    except AssertionError:
        pass
    except Exception as e:
        raise e
    else:
        raise AssertionError('An unexpected error occurred.')


async def assert_bad_type(field, data_type, value):
    await assert_fail(
        {field: value}, error=(field, (field, 'type'), errors.BAD_TYPE, data_type)
    )


async def assert_normalized(document, expected, schema=None, validator=None):
    if validator is None:
        validator = Validator(sample_schema)
    await assert_success(document, schema, validator)
    assert validator.document == expected
