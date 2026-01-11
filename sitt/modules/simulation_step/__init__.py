# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Simulation Modules"""

from .cached_dav import CachedDAV
from .cached_dav_river import CachedDAVRiver
from .cached_lake import CachedLake
from .dummy_fixed_speed import DummyFixedSpeed
from .dummy_for_tests import DummyForTests
from .simple import Simple
from .simple_dav import SimpleDAV
from .simple_dav_river import SimpleDAVRiver
from .simple_lake import SimpleLake
from .simple_river import SimpleRiver
from .simple_with_environment import SimpleWithEnvironment

__all__ = [
    "CachedDAV",
    "CachedDAVRiver",
    "CachedLake",
    "DummyFixedSpeed",
    "DummyForTests",
    "Simple",
    "SimpleDAV",
    "SimpleDAVRiver",
    "SimpleLake",
    "SimpleRiver",
    "SimpleWithEnvironment",
]
