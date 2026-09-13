#!/usr/bin/env python3
"""Fail if src/ assembles SQL out of string pieces.

Statements are built as parser nodes (include/sql_build.hpp, the Relation API), never as text: a
name stays a name and a value is a bound parameter or a constant node, so neither can close a
literal and carry on as syntax. This guard is what stops the old style creeping back in.

Keyword alone is not the signal -- error-message prose says "UPDATE and DELETE are driven by a scan"
and is not a query. What marks an assembled statement is a keyword-bearing literal next to a `+`.
That is deliberately narrow: it does not see user SQL passed straight through (vcat_primary_key_query
and vcat_primary_key_check hand the caller's own string to SendQuery, which is the documented
contract), and a fragment split so that no single line holds both a keyword and a `+` slips by. It
catches how the code was actually written.
"""

import pathlib
import re
import sys

# A double-quoted C++ literal, respecting backslash escapes.
LITERAL = re.compile(r'"((?:[^"\\\n]|\\.)*)"')
# A statement opener at the head of the literal, or a clause keyword surrounded by spaces.
KEYWORD = re.compile(
    r"(?i)(^\s*(select|insert|update|delete|with|create|drop|alter)\s)|(\s(from|where|values|set|into)\s)"
)
# The literal being glued to something.
CONCATENATION = re.compile(r'("(?:[^"\\\n]|\\.)*"\s*\+)|(\+\s*"(?:[^"\\\n]|\\.)*")')


def findings(root):
    for path in sorted(root.rglob("*")):
        if path.suffix not in (".cpp", ".hpp"):
            continue
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if line.lstrip().startswith(("//", "*", "/*")):
                continue
            if not CONCATENATION.search(line):
                continue
            for match in LITERAL.finditer(line):
                if KEYWORD.search(match.group(1)):
                    yield path, number, match.group(1)
                    break


def main():
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "src")
    found = list(findings(root))
    for path, number, literal in found:
        print(f"{path}:{number}: SQL assembled from strings: \"{literal[:80]}\"", file=sys.stderr)
    if found:
        print(
            f"\n{len(found)} site(s). Build the statement as parser nodes instead -- see "
            "src/virtual_catalog/include/sql_build.hpp.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
