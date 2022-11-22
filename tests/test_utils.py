# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
from sitt.utils import is_truthy


def test_is_truthy():
    assert is_truthy(1)
    assert is_truthy(True)
    assert is_truthy("ok")
    assert is_truthy("j")
    assert is_truthy("true")
    assert is_truthy("t")

    assert not is_truthy(0)
    assert not is_truthy(None)
    assert not is_truthy(False)
    assert not is_truthy("n")
    assert not is_truthy("no")
    assert not is_truthy("false")
    assert not is_truthy("f")
