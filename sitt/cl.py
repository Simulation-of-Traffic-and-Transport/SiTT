# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Command line runner"""

from __future__ import annotations

import logging
from argparse import ArgumentParser, BooleanOptionalAction, Namespace

from sitt import Configuration, Core, __version__
from sitt.loaders import load_configuration_from_yaml, config_class_loader


def parse_params() -> Namespace:
    """
    Parse parameters from command line
    :return:
    """
    # see: https://docs.python.org/3/library/argparse.html
    parser = ArgumentParser(description='Si.T.T. (Simulation of Traffic and Transport)')

    parser.add_argument("-c", "--config", help="Configuration filename", type=str, default="config.yaml")

    # verbosity is mutually exclusive
    quiet_or_verbose = parser.add_mutually_exclusive_group()
    quiet_or_verbose.add_argument("-v", "--verbose", action=BooleanOptionalAction, help="Verbose output", type=bool)
    quiet_or_verbose.add_argument("-q", "--quiet", action=BooleanOptionalAction,
                                  help="Suppress output (overrides verbose)", type=bool)

    # skipping steps is also mutually exclusive
    skip_steps = parser.add_mutually_exclusive_group()
    skip_steps.add_argument("--skip-step", choices=["simulation", "output"], help="Skip step", type=str)
    skip_steps.add_argument("--skip-simulation", action=BooleanOptionalAction, help="Skip simulation (and output)",
                            type=bool)
    skip_steps.add_argument("--skip-output", action=BooleanOptionalAction, help="Skip output", type=bool)
    parser.add_argument('-V', '--version', action='version', version="%(prog)s (" + __version__ + ")")

    return parser.parse_args()


def create_config_from_command_line() -> Configuration:
    """
    Create config from command line parameters.
    :return: a new configuration
    """
    args = parse_params()

    # read the configuration file first
    with open(args.config, 'rb') as f:
        config = load_configuration_from_yaml(f)

        # now read command line parameters into config - override stuff, if set
        config = config_class_loader(args.__dict__, config)

        return config


def run():
    """
    Run command line
    """
    # create configuration from command line arguments
    config = create_config_from_command_line()

    logging.getLogger().info("Si.T.T. command line - running core (" + __version__ + ").")

    # Create and run a Si.T.T. core
    core = Core(config)
    core.run()


# Just run command line
if __name__ == "__main__":
    run()
