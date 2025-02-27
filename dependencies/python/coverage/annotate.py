# Licensed under the Apache License: http://www.apache.org/licenses/LICENSE-2.0
# For details: https://github.com/nedbat/coveragepy/blob/master/NOTICE.txt

"""Source file annotation for coverage.py."""

from __future__ import annotations

import os
import re

from typing import TYPE_CHECKING
from collections.abc import Iterable

from coverage.files import flat_rootname
from coverage.misc import ensure_dir, isolate_module
from coverage.plugin import FileReporter
from coverage.report_core import get_analysis_to_report
from coverage.results import Analysis
from coverage.types import TMorf

if TYPE_CHECKING:
    from coverage import Coverage

os = isolate_module(os)


class AnnotateReporter:
    """Generate annotated source files showing line coverage.

    This reporter creates annotated copies of the measured source files. Each
    .py file is copied as a .py,cover file, with a left-hand margin annotating
    each line::

        > def h(x):
        -     if 0:   #pragma: no cover
        -         pass
        >     if x == 1:
        !         a = 1
        >     else:
        >         a = 2

        > h(2)

    Executed lines use ">", lines not executed use "!", lines excluded from
    consideration use "-".

    """

    def __init__(self, coverage: Coverage) -> None:
        self.coverage = coverage
        self.config = self.coverage.config
        self.directory: str | None = None

    blank_re = re.compile(r"\s*(#|$)")
    else_re = re.compile(r"\s*else\s*:\s*(#|$)")

    def report(self, morfs: Iterable[TMorf] | None, directory: str | None = None) -> None:
        """Run the report.

        See `coverage.report()` for arguments.

        """
        self.directory = directory
        self.coverage.get_data()
        for fr, analysis in get_analysis_to_report(self.coverage, morfs):
            self.annotate_file(fr, analysis)

    def annotate_file(self, fr: FileReporter, analysis: Analysis) -> None:
        """Annotate a single file.

        `fr` is the FileReporter for the file to annotate.

        """
        statements = sorted(analysis.statements)
        missing = sorted(analysis.missing)
        excluded = sorted(analysis.excluded)

        if self.directory:
            ensure_dir(self.directory)
            dest_file = os.path.join(self.directory, flat_rootname(fr.relative_filename()))
            assert dest_file.endswith("_py")
            dest_file = dest_file[:-3] + ".py"
        else:
            dest_file = fr.filename
        dest_file += ",cover"

        with open(dest_file, "w", encoding="utf-8") as dest:
            i = j = 0
            covered = True
            source = fr.source()
            for lineno, line in enumerate(source.splitlines(True), start=1):
                while i < len(statements) and statements[i] < lineno:
                    i += 1
                while j < len(missing) and missing[j] < lineno:
                    j += 1
                if i < len(statements) and statements[i] == lineno:
                    covered = j >= len(missing) or missing[j] > lineno
                if self.blank_re.match(line):
                    dest.write("  ")
                elif self.else_re.match(line):
                    # Special logic for lines containing only "else:".
                    if j >= len(missing):
                        dest.write("> ")
                    elif statements[i] == missing[j]:
                        dest.write("! ")
                    else:
                        dest.write("> ")
                elif lineno in excluded:
                    dest.write("- ")
                elif covered:
                    dest.write("> ")
                else:
                    dest.write("! ")

                dest.write(line)
