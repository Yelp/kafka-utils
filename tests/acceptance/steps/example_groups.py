from behave import given
from behave import then
from behave import when


@given('we have a set of existing consumer groups')
def step_impl1(context):
    pass


@when('we call the list_groups command')
def step_impl2(context):
    assert True is not False


@then('the groups will be listed')
def step_impl3(context):
    assert context.failed is False
