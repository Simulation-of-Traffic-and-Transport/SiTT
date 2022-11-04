# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Utility functions"""

from __future__ import annotations

import nanoid

__all__ = ['is_truthy', 'generate_uid']


def is_truthy(val) -> bool:
    """Utility function to check the truthiness of a value"""
    return val and val != 'n' and val != 'no' and val != 'f' and val != 'false'
    # if not val or val == 'n' or val == 'no' or val == 'f' or val == 'false':
    #     return False
    #
    # return True


def generate_uid() -> str:
    return nanoid.generate('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', 12)
