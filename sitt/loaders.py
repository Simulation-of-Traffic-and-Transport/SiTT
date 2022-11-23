# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
""""Loaders to use internally"""

from __future__ import annotations

import datetime as dt
import importlib
import os
import re
from typing import Any

import yaml

from sitt import Configuration, SkipStep, PreparationInterface, OutputInterface, \
    SimulationPrepareDayInterface, SimulationDefineStateInterface, SimulationStepInterface


def load_configuration_from_yaml(yaml_stream: Any) -> Configuration:
    # loader for yaml
    loader = yaml.SafeLoader
    loader.add_constructor("!Env", _env_constructor)

    yaml_config = yaml.load(yaml_stream, loader)
    return config_class_loader(yaml_config)


def _env_constructor(loader: yaml.loader.SafeLoader, node: Any) -> str:
    """Load !Env tag"""
    value = str(loader.construct_scalar(node))  # get the string value next to !Env
    match = re.compile(".*?\\${(\\w+)}.*?").findall(value)
    if match:
        for key in match:
            if key in os.environ:
                value = value.replace('${' + match[0] + '}', os.environ[key])
    return value


def config_class_loader(data: dict, config: Configuration | None = None) -> Configuration:
    """
    A loader for config class - can be called repeatedly, e.g. for config file and command line arguments.

    :param data: data object
    :param config: optional config object
    :return: Configuration
    """
    if config is None:
        config = Configuration()

    # verbose or quiet (quiet will override verbose, if both are set)?
    if 'quiet' in data and data['quiet']:
        config.quiet = True
        config.verbose = False  # overwrite this if called repeatedly
    elif 'verbose' in data and data['verbose']:
        config.verbose = True
        config.quiet = False  # overwrite this if called repeatedly

    # skip step - mutually exclusive configuration
    if 'skip_step' in data and data['skip_step']:
        config.skip_step = SkipStep(data['skip_step'].lower())
    elif 'skip_simulation' in data and data['skip_simulation']:
        config.skip_step = SkipStep.SIMULATION
    elif 'skip_output' in data and data['skip_output']:
        config.skip_step = SkipStep.OUTPUT

    # start and end hubs
    if 'simulation_start' in data and data['simulation_start']:
        config.simulation_start = data['simulation_start']
    if 'simulation_end' in data and data['simulation_end']:
        config.simulation_end = data['simulation_end']

    # maximum number of steps without advancing before breaking
    if 'break_simulation_after' in data and data['break_simulation_after']:
        config.break_simulation_after = int(data['break_simulation_after'])

    # start_date
    if 'start_date' in data and data['start_date']:
        if type(data['start_date']) == dt.date:
            config.start_date = data['start_date']
        else:
            config.start_date = dt.datetime.fromisoformat(data['start_date'])

    # step configuration
    _step_data = _parse_step_data('preparation', data, config)
    if _step_data:
        config.preparation = _step_data
    _step_data = _parse_step_data('simulation_prepare_day', data, config)
    if _step_data:
        config.simulation_prepare_day = _step_data
    _step_data = _parse_step_data('simulation_define_state', data, config)
    if _step_data:
        config.simulation_define_state = _step_data
    _step_data = _parse_step_data('simulation_step', data, config)
    if _step_data:
        config.simulation_step = _step_data
    _step_data = _parse_step_data('output', data, config)
    if _step_data:
        config.output = _step_data

    return config


def _parse_step_data(key: str, data: dict, config: Configuration) -> list[
                                                                         PreparationInterface | SimulationPrepareDayInterface | SimulationDefineStateInterface | SimulationStepInterface | OutputInterface] | None:
    """
    Helper to parse input data from steps. Throws exception if "class" key is not defined or the classes cannot be
    found.

    :param key: key to search data for
    :param data: input data
    :return: list of possible steps or empty list
    """

    if key in data and data[key]:
        try:
            return _load_step_classes(key, data, config)
        except Exception:
            raise Exception("error in " + key + " step:")

    return None


def _load_step_classes(key: str, data: dict, config: Configuration) -> list[
    PreparationInterface | SimulationPrepareDayInterface | SimulationDefineStateInterface | SimulationStepInterface | OutputInterface]:
    """
    Helper to actually load classes for steps

    :param raw_class_list: list of raw entries from config file
    :return: list of classes
    """
    raw_class_list: list = data[key]
    return _load_steps_from_raw_list(key, data, raw_class_list, config)


def _load_steps_from_raw_list(key: str, data: dict, raw_class_list: list, config: Configuration) -> list[
    PreparationInterface | SimulationPrepareDayInterface | SimulationDefineStateInterface | SimulationStepInterface | OutputInterface]:
    class_list = []

    for entry in raw_class_list:
        if 'class' not in entry:
            raise Exception("no class defined in", entry)
        if 'module' not in entry:
            raise Exception("no module defined in", entry)

        # import module, create class and instance
        module = importlib.import_module(entry['module'])
        my_class = getattr(module, entry['class'])
        my_instance = my_class()

        # parameters, if defined - this will not write arguments not defined in class
        if 'args' in entry:
            my_instance_attrs = my_instance.__dict__

            for arg in entry['args']:
                if arg in my_instance_attrs:
                    setattr(my_instance, arg, entry['args'][arg])

                    # special cases
                    if arg == 'connection':
                        # connection setting -> take data from settings.connections
                        if 'settings' not in data or 'connections' not in data['settings'] or entry['args'][arg] not in \
                                data['settings']['connections']:
                            raise Exception('no YAML entry settings.connections.' + entry['args'][arg])
                        for k in data['settings']['connections'][entry['args'][arg]]:
                            setattr(my_instance, k, data['settings']['connections'][entry['args'][arg]][k])

        if 'conditions' in entry:
            setattr(my_instance, 'conditions', entry['conditions'])

        # special conditional modules - recurse here
        if entry['class'] == 'ConditionalModule' and 'submodules' in entry:
            setattr(my_instance, 'submodules', _load_steps_from_raw_list(key, data, entry['submodules'], config))
            # setattr(my_instance, 'config', config) # this is set on run

        class_list.append(my_instance)

    return class_list
