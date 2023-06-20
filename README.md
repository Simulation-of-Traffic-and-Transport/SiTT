# Si.T.T. (Simulation of Traffic and Transport)

Si.T.T. is a program suite to simulate the traffic and transport of pre-industrial societies. It uses an agent-based
approach to model the simulation. An agent can be thought of as a batch of cargo transported through the network
(rather than an individual person travelling it).

[![PyPI - Version](https://img.shields.io/pypi/v/sitt.svg)](https://pypi.org/project/sitt)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/sitt.svg)](https://pypi.org/project/sitt)

*Note:* This project is WIP at the moment and in pre-alpha status. Please come back later to test the full capabilities.

Main repository: https://codeberg.org/SiTT/SiTT  
Public mirror: https://github.com/Simulation-of-Traffic-and-Transport/SiTT

-----

**Table of Contents**

- [Installation](#installation)
- [Documentation](#documentation)
- [Examples](#examples)
- [License](#license)

## Installation

You need Python 3.10 or later to run Si.T.T.

```console
pip install sitt
```

After installation, you can run Si.T.T. using it as a module:

```console
python -m sitt
```

This will activate the command line interface and print a help message. For other ways to run Si.T.T., check out the
examples below.

## Dependencies

Installing Si.T.T. as package should install all dependent packages, too. You might want to install them by hand and/or
install the extra dependencies for certain use cases:

```console
pip install -r requirements.txt
pip install -r requirements_extras.txt
```

The extras file contains optional python modules such as pytest or the binary Postgres package.

## Testing

You can run unit tests using pytest:

```console
pip install pytest
cd tests
pytests -v .
```

## Documentation

You can read some documentation on the following pages:

* [Si.T.T.'S General Concept](readmes/concept.md) (with figures)
* [Postgres Schemata](readmes/postgres_schemata.md)
* [Water Data](readmes/water_data.md) - information on water bodies, rivers, etc.

## Examples

Can be found in the [examples directory](examples/README.md).

## License

`sitt` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
