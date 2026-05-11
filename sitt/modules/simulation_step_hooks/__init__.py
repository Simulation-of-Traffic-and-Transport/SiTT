# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Simulation Hooks - used in steps to alter the state or get specific information."""

from .pause_on_rain import PauseOnRain
from .pause_on_snow import PauseOnSnow
from.pause_on_wind import PauseOnWind
from .pause_on_x_degrees import PauseOnXDegrees
from .resting import Resting

__all__ = [
    "PauseOnRain",
    "PauseOnSnow",
    "PauseOnWind",
    "PauseOnXDegrees",
    "Resting"
]
