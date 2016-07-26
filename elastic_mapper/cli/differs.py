from deepdiff import DeepDiff
import pprint

import re


class MappingDiffer(object):

    def __init__(self, typename, source, dest):
        self.typename = typename
        self.source = source  # mapping generated by a mapper
        self.dest = dest  # mapping present in ES

        self.diff()

    def diff(self):
        diff = DeepDiff(self.source,
                        self.dest,
                        ignore_order=True)
        print "%s:\n\n" % self.typename
        pprint.pprint(diff)
        for chain in diff['dic_item_added']:
            self._parse_chain(chain)

        for chain in diff['dic_item_removed']:
            self._parse_chain(chain)

        for chain, change in diff['values_changed'].items():
            self._parse_chain(chain)
            print change

    def _parse_chain(self, chain):
        pattern = re.compile(r"(?<=\[)(.*?)(?=\])")
        matches = pattern.findall(chain)
        matches = [m.replace("'", "") for m in matches if m not in ("'properties'")]
        print "chain %s -> %s" % (chain, matches)
