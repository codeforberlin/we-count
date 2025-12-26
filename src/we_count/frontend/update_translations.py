#!/usr/bin/env python3
# Copyright (c) 2024-2025 Berlin zählt Mobilität
# SPDX-License-Identifier: MIT

# @file    update_translations.py
# @author  Egbert Klaassen
# @date    2025-07-19

import glob
import os
import subprocess

dirname = os.path.dirname(__file__)
files = [os.path.basename(x) for x in glob.glob(os.path.join(dirname, '*.py'))]
subprocess.check_call(['pybabel', 'extract', '-o', 'locales/messages.pot'] + files, cwd=dirname)
subprocess.check_call(['pybabel', 'update', '-D', 'bzm', '-i', 'locales/messages.pot', '-d', 'locales'], cwd=dirname)
subprocess.check_call(['pybabel', 'compile', '-f', '-D', 'bzm', '-d', 'locales'], cwd=dirname)
