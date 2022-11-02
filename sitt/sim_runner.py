# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Simulation runner, can be called as process or thread in the future"""

from __future__ import annotations

import logging
from pickle import dumps

from sitt import State, Configuration, Context, Status

__all__ = ['run_simulation']

logger = logging.getLogger()


def run_simulation(state: State, config: Configuration, context: Context, pickle=False):
    """Simulation runner - external to use it in multiprocessing and multithreading."""

    # this will count until 100 and break the state if there are no advances within 100 days
    circuit_breaker_count = 0

    if logger.level <= logging.INFO:
        logger.info(f"{state.uid} is starting ({state.id}, {state.get_total_length_m(context) / 1000:.3f} km)")

    # run until finished
    while state.status == Status.RUNNING:
        # reset certain stuff
        state.initialize_next_step()

        # first is the before step
        for module in config.simulation:
            state = module.run_before(config, context, state)

        # main simulation step - this is broken into sub steps to calculate the next possible hub for this step
        circuit_breaker_sub_count = 0  # for the inner loop

        while state.status == Status.RUNNING and state.step_data.time_available > state.step_data.time_used and circuit_breaker_sub_count < config.break_simulation_after:
            for module in config.simulation:
                state = module.run(config, context, state)

            circuit_breaker_sub_count += 1

            if circuit_breaker_sub_count >= config.break_simulation_after:
                state.status = Status.CANCELLED
                logger.warning(
                    state.uid + " cancelled due to too many non-advances in inner loop (" + str(
                        config.break_simulation_after) + ")")

        # check advance and increment circuit_breaker_count, if not
        if state.has_advanced_today():
            circuit_breaker_count = 0
        else:
            circuit_breaker_count += 1

        # cancel if number of count is too hight
        if circuit_breaker_count >= config.break_simulation_after:
            state.status = Status.CANCELLED
            logger.warning(
                state.uid + " cancelled due to too many non-advances in outer loop (" + str(
                    config.break_simulation_after) + ")")

        # do the actual advance
        state.advance()

        # after simulation step
        for module in config.simulation:
            state = module.run_after(config, context, state)

        # TODO: persist step for history

        # finished?
        if state.status == Status.FINISHED and logger.level <= logging.INFO:
            logger.info(
                state.uid + " finished on step " + str(state.step))

    # pickle the state result - makes this work for multiprocessing.
    if pickle:
        return dumps(state)
    return state
