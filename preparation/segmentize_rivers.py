# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Segment rivers and water bodies - this is stuff that will take a very long time to complete, so it should be
done in advance."""

import argparse

if __name__ == "__main__":
    """Segment rivers and water bodies - this is stuff that will take a very long time."""
    parser = argparse.ArgumentParser(description="Segment rivers and water bodies - this is stuff that will take a very long time to complete, so it should be done in advance.", exit_on_error=False)
    parser.add_argument('action', default='help', choices=['help','segment'], help='action to perform')
    try:
        args = parser.parse_args()

        if args.action =='help':
            parser.print_help()
        elif args.action =='segment':
            print("Segmentation")
    except:
        parser.print_help()
