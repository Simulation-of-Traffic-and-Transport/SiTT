# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT

from __future__ import annotations

import logging
import sys

from sitt.__about__ import (
    __author__,
    __copyright__,
    __version__,
)
from sitt.base import (
    SkipStep,
    Configuration,
    SpaceTimeData,
    Context,
    State,
    Agent,
    SetOfResults,
    PreparationInterface,
    SimulationPrepareDayInterface,
    SimulationDefineStateInterface,
    SimulationStepInterface,
    OutputInterface
)
from sitt.core import Core, BaseClass, Preparation, Simulation, Output
from sitt.utils import is_truthy

import sitt.modules.preparation as preparation
import sitt.modules.simulation_step as simulation_step
import sitt.modules.output as output

__all__ = [
    "__version__",
    "__author__",
    "__copyright__",
    "SkipStep",
    "Configuration",
    "SpaceTimeData",
    "Context",
    "State",
    "Agent",
    "SetOfResults",
    "PreparationInterface",
    "SimulationPrepareDayInterface",
    "SimulationDefineStateInterface",
    "SimulationStepInterface",
    "OutputInterface",
    "Core",
    "BaseClass",
    "Preparation",
    "Simulation",
    "Output",
    "is_truthy",
    "preparation",
    "simulation_step",
    "output"
]

logger: logging.Logger = logging.getLogger()

# Minimum version check
python_version: tuple[int] = sys.version_info[:2]
if python_version[0] < 3 or (python_version[0] == 3 and python_version[1] < 10):
    logger.critical("Your Python version is too old. Si.T.T. requires at least Python 3.10.")
    sys.exit(-1)
