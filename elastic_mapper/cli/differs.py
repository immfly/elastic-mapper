from deepdiff import DeepDiff
import pprint
from enum import Enum

import re


class State(Enum):
    ok = 1
    extra_field = 2 # dynamic field
    extra_param = 3
    missing_field = 4
    missing_param = 5
    type_conflict = 6
    param_conflict = 7


class MappingState(object):

    def __init__(self, issue_type, diff, change=None):
        self.diff = diff
        self.chain = self._parse_chain(diff)
        self.change = change
        self.type = self._parse_type(issue_type)

    def __repr__(self):
        return "%s: %s" % (self.type.name, self.chain)

    def _parse_chain(self, diff):
        pattern = re.compile(r"(?<=\[)(.*?)(?=\])")
        matches = pattern.findall(diff)
        matches = [m.replace("'", "") for m in matches]
        print "channel %s -> %s" % (diff, matches)
        return matches

    def _parse_type(self, issue_type):
        if issue_type == 'added':
            if len(self.chain) % 2 == 0: # param
                return State.extra_param
            else: # field
                return State.extra_field
        elif issue_type == 'removed':
            if len(self.chain) % 2 == 0: # param
                return State.missing_param
            else: # field
                return State.missing_field
        else: # 'changed'
            if self.chain[-1] == 'type':
                return State.type_conflict
            elif len(self.chain) % 2 == 0: # param:
                return State.param_conflict
            else:
                raise Exception("Unknown mapping state")


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
        states = []
        for chain in diff['dic_item_added']:
            # fields -> dynamic fields (fields present in ES but not in the mapper)
            # params -> params present in ES but not in the mapper
            states.append(MappingState('added', chain))

        for chain in diff['dic_item_removed']:
            # fields -> new fields added to the mapper, no data with the new field sent to ES yet
            # params -> params newly added or declared in mapper but not sent to ES yet
            states.append(MappingState('removed', chain))

        for chain, change in diff['values_changed'].items():
            # type -> type conflicts (one type declared in mapper while a different one exists in ES)
            # params -> params modified in mapper but not sent to ES yet
            states.append(MappingState('changed', chain, change))

        print("****")
        for state in states:
            print(state)

    def _parse_chain(self, chain):
        pattern = re.compile(r"(?<=\[)(.*?)(?=\])")
        matches = pattern.findall(chain)
        matches = [m.replace("'", "") for m in matches if m not in ("'properties'")]
        print "chain %s -> %s" % (chain, matches)