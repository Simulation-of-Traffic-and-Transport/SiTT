# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Core classes needed to run the application.

.. warning::
    This module is treated as private API.
    Users should not need to use this module directly.
"""

import abc
import logging
import os.path
import pickle
from concurrent import futures

import networkx as nx

from sitt import Configuration, Parallelism, Context, SkipStep, State, SetOfResults, sim_runner

__all__ = ['Core', 'Preparation', 'Simulation', 'Output']

logger = logging.getLogger()


########################################################################################################################
# Core itself
########################################################################################################################


class Core:
    """
    Core of Simulation
    """

    def __init__(self, config: Configuration):
        """
        Constructor.

        :param config: configuration object
        """
        self.config: Configuration = config

    def run(self):
        """
        Run simulation.
        """
        # preparation step - this step must be run always
        preparation = Preparation(self.config)
        context = preparation.run()

        # simulation step
        if self.config.skip_step != SkipStep.SIMULATION:
            sim = Simulation(self.config, context)
            set_of_results = sim.run()

            # final step: output
            if self.config.skip_step != SkipStep.OUTPUT:
                output = Output(self.config, context, set_of_results)
                output.run()


########################################################################################################################
# Abstract base class for Preparation, Simulation, and Output.
########################################################################################################################


class BaseClass(abc.ABC):
    def __init__(self, config: Configuration | None = None):
        """
        Constructor.

        :param config: configuration object
        """
        self.config = config

    def is_skipped(self, module: object, context: Context) -> bool:
        """check for skip"""
        if hasattr(module, 'skip') and module.skip:
            logger.info("Skipping %s due to setting" % module)
            return True

        if hasattr(module, 'conditions') and module.conditions and len(module.conditions) > 0:
            for condition in module.conditions:
                condition_key = condition
                prerequisite = False
                if condition.startswith('not_'):
                    condition_key = condition[4:]
                    prerequisite = True

                mydata = module.conditions[condition]

                if self.condition_ok(condition_key, condition, mydata, module, context=context) == prerequisite:
                    logger.info("Skipping %s due to unmet condition: %s = %s" % (module, condition, mydata))
                    return True

        return False

    def condition_ok(self, key: str, condition: str, data: any, module: object, context: Context = None) -> bool:
        """Handle single condition"""
        if key == 'file_must_exist':
            return os.path.exists(data)
        elif key == 'data_must_exist':
            if 'class' in data:
                c = self.class_instance_for_name(data['class'], module, context)
                if c is not None:
                    if 'key' in data and hasattr(c, data['key']):
                        return getattr(c, data['key']) is not None
            logger.warning("%s not in %s not valid: %s = %s" % (condition, module, condition, data))
        else:
            # Show warning if unknown condition
            logger.warning("Unknown condition in %s: %s = %s" % (module, condition, data))

        return True

    def class_instance_for_name(self, name: str, module: object, context: Context) -> object | None:
        if name == 'context':
            return context
        if name == 'config':
            return self.config
        if name == 'module':
            return module
        return None


########################################################################################################################
# Preparation class
########################################################################################################################


class Preparation(BaseClass):
    """
    Preparation class - will aggregate all information for the simulation
    """

    def __init__(self, config: Configuration):
        super().__init__(config)

    def run(self) -> Context:
        """
        Run the preparation

        :return: created context object
        """
        logger.info("******** Preparation: started ********")

        context = Context()

        # run modules
        for module in self.config.preparation:
            if not self.is_skipped(module, context):
                context = module.run(self.config, context)

        logger.info("******** Preparation: finished ********")

        return context


########################################################################################################################
# Simulation class
########################################################################################################################


class Simulation(BaseClass):
    """
    Main simulation class - this will run the actual simulation.
    """

    def __init__(self, config: Configuration, context: Context):
        """
        Constructor.

        :param config: configuration object
        :param context: context object
        """
        super().__init__(config)
        self.context = context

    def run(self) -> SetOfResults:
        """
        Run the simulation

        :return: created set of results object
        """
        logger.info("******** Simulation: started ********")

        # Checking start and stop hubs
        if not self.config.simulation_start:
            logger.error("simulation_start is empty - simulation failed!")
        if not self.config.simulation_end:
            logger.error("simulation_end is empty - simulation failed!")

        if logger.level <= logging.INFO:
            logger.info("start:       " + self.config.simulation_start)
            logger.info("end:         " + self.config.simulation_end)
            logger.info("parallelism: " + str(self.config.simulation_parallelism))

        set_of_results = SetOfResults()

        # This is the first take of how we handle the simulation:
        # We will create all simple paths in the graph and let one agent run through each one
        # we might use concurrent multiprocessing for this
        results = []

        # multiprocessing
        if self.config.simulation_parallelism == Parallelism.PROCESSES:
            with futures.ProcessPoolExecutor() as e:
                for p in nx.all_simple_edge_paths(self.context.graph, self.config.simulation_start,
                                                  self.config.simulation_end):
                    results.append(e.submit(sim_runner, State(p), self.config, self.context, pickle=True))

                # wait for return values
                for result in futures.as_completed(results):
                    # unpickle result
                    decoded = pickle.loads(result.result())
                    print(decoded)
                    # TODO: add to set of results

        # multithreading
        elif self.config.simulation_parallelism == Parallelism.THREADS:
            with futures.ThreadPoolExecutor() as e:
                for p in nx.all_simple_edge_paths(self.context.graph, self.config.simulation_start,
                                                  self.config.simulation_end):
                    results.append(e.submit(sim_runner, State(p), self.config, self.context))

                # wait for return values
                for result in futures.as_completed(results):
                    print(result.result())
                    # TODO: add to set of results

        # single thread
        else:
            for p in nx.all_simple_edge_paths(self.context.graph, self.config.simulation_start,
                                              self.config.simulation_end):
                print(sim_runner(State(p), self.config, self.context))
                # TODO: add to set of results

        logger.info("******** Simulation: finished ********")

        return set_of_results


########################################################################################################################
# Output class
########################################################################################################################


class Output(BaseClass):
    """
    Main simulation class - this will run the actual simulation.
    """

    def __init__(self, config: Configuration, context: Context, set_of_results: SetOfResults):
        """
        Constructor.

        :param config: configuration object
        :param context: context object
        :param set_of_results: SetOfResults object
        """
        super().__init__(config)
        self.context = context
        self.set_of_results = set_of_results

    def run(self):
        """
        Run the output

        :return: created set of results object
        """
        logger.info("******** Output: started ********")

        # run modules
        for module in self.config.output:
            module.run(self.config, self.context, self.set_of_results)

        logger.info("******** Output: finished ********")
