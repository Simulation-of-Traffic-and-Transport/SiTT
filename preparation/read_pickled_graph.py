# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Read pickled graph."""

import pickle

import igraph as ig

if __name__ == "__main__":
    """Read pickled graph."""

    with open('graph_dump.pickle', 'rb') as f:
        g: ig.Graph = pickle.load(f)
        print(g)
