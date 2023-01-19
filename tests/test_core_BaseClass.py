# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
from sitt.core import BaseClass, Context


# we create a new concrete class to instantiate
class TestBaseClass(BaseClass):
    pass


class TestObject:
    def __init__(self):
        self.skip: bool = False


def test_is_skipped():
    ctx = Context()
    o = TestObject()
    t = TestBaseClass()

    # test basic skipping
    assert not t.is_skipped(o, ctx)
    o.skip = True
    assert t.is_skipped(o, ctx)

    # TODO: test module conditions
