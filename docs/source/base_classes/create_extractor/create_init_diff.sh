#!/bin/bash

original_file = "src/spooq2/extractor/__init__.py"
adapted_file = ""

diff -U 3 -t --label="original " --label="adapted $filepath" ~/projects/spooq2/src/spooq2/extractor/__init__.py init.py > init.diff
