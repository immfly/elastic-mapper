import collections
from enum import Enum

import re

from deepdiff import DeepDiff

import six


class State(Enum):
    ok = 1
    extra_field = 2  # dynamic field
    extra_param = 3
    missing_field = 4
    missing_param = 5
    type_conflict = 6
    param_conflict = 7
    template_missing_type = 8
    template_extra_type = 9
    inconsistent_field = 10


class MappingState(object):
    texts = {
        State.ok: ('OK', ''),
        State.inconsistent_field: ('Inconsistent Field', 'Field `{fieldname}` is defined with '
                                   'different types in different mappers'),
    }

    def __init__(self, fieldname, state=State.ok):
        self.fieldname = fieldname
        self.state = state

    def __repr__(self):
        return "%s (%s): %s" % (self.fieldname, self.state.name, self.description)

    @property
    def name(self):
        return self.texts[self.state][0]

    @property
    def description(self):
        text = self.texts[self.state][1]
        return text.format(**self.__dict__)


class TemplateState(MappingState):
    texts = {
        State.template_missing_type: ('Missing Type', 'Type `{typename}` indexed in ES but not '
                                      'declared in the template'),
        State.template_extra_type: ('Extra Type', 'Type `{typename}` defined in the template '
                                    'but not exported to ES yet'),
    }

    def __init__(self, typename, state):
        self.typename = typename
        self.state = state

    def __repr__(self):
        return "%s (%s): %s" % (self.typename, self.state.name, self.description)


class MappingIssue(MappingState):
    texts = {
        State.extra_field: ('Dynamic Field', 'Field indexed in ES but not declared in the mapper'),
        State.extra_param: ('Extra Param', ('Parameter `{param_name}` defined in ES but not '
                                            'declared in the mapper')),
        State.missing_field: ('Missing Field', ('Field declared in the mapper but not exported '
                                                'to ES yet')),
        State.missing_param: ('Missing Param', ('Parameter {{{param_name}={param_value}}} is '
                                                'declared in the mapper but not exported to '
                                                'ES yet')),
        State.type_conflict: ('Type Conflict', ('Field indexed as `{source_type}` but declared '
                                                'as `{dest_type}`')),
        State.param_conflict: ('Param Conflict', ('Parameter `{param_name}` exported in ES as '
                                                  '`{source_param}` but defined as '
                                                  '`{dest_param}` in the mapper')),
    }

    def __init__(self, issue_type, diff, source, dest, change=None):
        self.diff = diff
        self.chain = self._parse_chain(diff)
        self.source = source
        self.dest = dest
        self.change = change
        self.fieldname = self._parse_fieldname(self.chain)
        self._set_state(issue_type)

    def _parse_chain(self, diff):
        pattern = re.compile(r"(?<=\[)(.*?)(?=\])")
        matches = pattern.findall(diff)
        matches = [m.replace("'", "") for m in matches]
        return matches

    def _parse_fieldname(self, chain):
        return '.'.join(chain[::2])

    def _set_state(self, issue_type):
        if issue_type == 'added':
            if len(self.chain) % 2 == 0:  # param
                self.state = State.extra_param
                self.param_name = self.chain[-1]
            else:  # field
                self.state = State.extra_field
        elif issue_type == 'removed':
            if len(self.chain) % 2 == 0:  # param
                self.state = State.missing_param
                self.param_name = self.chain[-1]
                self.param_value = self._get_value_from_chain(self.source)
            else:  # field
                self.state = State.missing_field
        else:  # 'changed'
            if self.chain[-1] == 'type':
                self.state = State.type_conflict
                self.source_type = self.change['new_value']
                self.dest_type = self.change['old_value']
            elif len(self.chain) % 2 == 0:  # param:
                self.state = State.param_conflict
                self.param_name = self.chain[-1]
                self.source_param = self.change['new_value']
                self.dest_param = self.change['old_value']
            else:
                raise Exception("Unknown mapping state")

    def _get_value_from_chain(self, mapping):
        node = mapping
        for key in self.chain:
            node = node[key]
        return node


def normalize_mapping(mapping):
    "Converts all string-like values of a mapping into the same text type"
    normalized = {}
    for k, v in mapping.iteritems():
        if isinstance(v, dict):
            normalized[k] = normalize_mapping(v)
        elif isinstance(v, six.string_types):
            normalized[k] = six.text_type(v)
        else:
            normalized[k] = v
    return normalized


class MappingDiffer(object):

    def __init__(self, typename, source, dest):
        self.typename = typename
        self.source = normalize_mapping(source)  # mapping generated by a mapper
        self.dest = normalize_mapping(dest)  # mapping present in ES

        self.diff()

    def diff(self):
        diff = DeepDiff(self.source,
                        self.dest,
                        ignore_order=True)

        states = {}
        fieldnames = [f for f in self._gen_mapping_fieldnames(self.source)]
        for fieldname in sorted(fieldnames):
            states[fieldname] = []

        for chain in diff.get('dictionary_item_added', []):
            # fields -> dynamic fields (fields present in ES but not in the mapper)
            # params -> params present in ES but not in the mapper
            issue = MappingIssue('added', chain, self.source, self.dest)
            states[issue.fieldname] = [issue]

        for chain in diff.get('dictionary_item_removed', []):
            # fields -> new fields added to the mapper, no data with the new field sent to ES yet
            # params -> params newly added or declared in mapper but not sent to ES yet
            issue = MappingIssue('removed', chain, self.source, self.dest)
            if issue.fieldname in fieldnames:
                states[issue.fieldname].append(issue)

        for chain, change in diff.get('values_changed', diff.get('type_changes', {})).items():
            # type -> type conflicts (a type declared in mapper while a different one exists in ES)
            # params -> params modified in mapper but not sent to ES yet
            issue = MappingIssue('changed', chain, self.source, self.dest, change)
            if issue.fieldname in fieldnames:
                states[issue.fieldname].append(issue)

        return self._normalize_states(states)

    def _normalize_states(self, states):
        """
        Perform some useful transformations to the state list:
            - Remove prefix with typename for all state fieldnames
            - Remove superfluous conflicts when there is a type conflict
            - Add `ok` state for fields with no conflicts
        """
        normalized = collections.OrderedDict()
        for fieldname, state_list in states.items():
            # norm_fieldname = fieldname.split('.', 1)[1]
            norm_fieldname = fieldname
            # in case some state is `type_conflict`, we remove the other issues for that field
            # since they will be irrelevant when having a type conflict
            conflicts = [s for s in state_list if s.state == State.type_conflict]
            if conflicts:
                state_list = [conflicts[0]]
            if not state_list:
                normalized[norm_fieldname] = [MappingState(norm_fieldname), ]
            else:
                for state in state_list:
                    state.fieldname = norm_fieldname
                normalized[norm_fieldname] = state_list
        # for fieldname, state in states.items():
        #     state.fieldname = state.fieldname.split('.', 1)[1]
        #     normalized[state.fieldname] = state
        return normalized

    def _gen_mapping_fieldnames(self, mapping, prefix=''):
        fieldnames = []
        new_prefix = (prefix + '.') if prefix else ''
        for fieldname, attrs in mapping.iteritems():
            if 'properties' in attrs.keys():
                # nested object
                nested_fieldnames = self._gen_mapping_fieldnames(attrs['properties'],
                                                                 new_prefix + fieldname)
                for nf in nested_fieldnames:
                    fieldnames.append(nf)
            else:
                fieldnames.append(new_prefix + fieldname)
        return fieldnames


TemplateDiffResult = collections.namedtuple('TemplateDiffResult',
                                            ['type_states', 'template_states'],
                                            verbose=True)


class TemplateDiffer(object):

    def __init__(self, template_name, source, dest):
        self.template_name = template_name
        self.source = normalize_mapping(source)  # mapping generated by a mapper
        self.dest = normalize_mapping(dest)  # mapping present in ES

        self.diff()

    def diff(self):
        # test missing and extra types
        local_types = set(self.source.keys())
        es_types = set(self.dest.keys())
        common_types = local_types.intersection(es_types)
        extra_types = local_types.difference(es_types)
        missing_types = es_types.difference(local_types)

        result = TemplateDiffResult(type_states=dict(), template_states=[])
        # check field conflicts for types present in both templates
        for common_type in common_types:
            local_mapping = self.source[common_type]['properties']
            es_mapping = self.dest[common_type]['properties']
            differ = MappingDiffer(common_type,
                                   local_mapping,
                                   es_mapping)
            result.type_states[common_type] = differ.diff()

        # TODO: refactor this mess into something more functional
        items = self.source.iteritems()
        for i, (typename1, mapping1) in enumerate(items):
            for j, (typename2, mapping2) in enumerate(items):
                if typename1 != typename2:
                    duplicated_fields = set(mapping1['properties']) & set(mapping2['properties'])
                    for duplicated_field in duplicated_fields:
                        differ = MappingDiffer(duplicated_field,
                                               mapping1['properties'],
                                               mapping2['properties'])
                        diff = differ.diff()
                        # mark a conflict if the duplicated fields have different types
                        if all([duplicated_field in diff,
                               diff[duplicated_field][0].state != State.ok]):
                            state = MappingState(fieldname=duplicated_field,
                                                 state=State.inconsistent_field)
                            # add inconsistence conflict for typename 1
                            if typename1 not in result.type_states:
                                result.type_states[typename1] = collections.OrderedDict()
                            result.type_states[typename1][duplicated_field] = [state, ]

                            # add inconsistence conflict for typename 2
                            if typename2 not in result.type_states:
                                result.type_states[typename2] = collections.OrderedDict()
                            result.type_states[typename2][duplicated_field] = [state, ]

        for extra_type in extra_types:
            state = TemplateState(typename=extra_type, state=State.template_extra_type)
            result.template_states.append(state)

        for missing_type in missing_types:
            state = TemplateState(typename=missing_type, state=State.template_missing_type)
            result.template_states.append(state)

        return result
