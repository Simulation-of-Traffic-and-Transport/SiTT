# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Simulation Modules"""

from .dummy_fixed_speed import DummyFixedSpeed
from .simple import Simple

__all__ = [
    "DummyFixedSpeed",
    "Simple",
]
