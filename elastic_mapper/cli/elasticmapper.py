import collections
import json
import re
import os
from pygments import highlight, lexers, formatters
from tabulate import tabulate
from termcolor import colored
from elasticsearch import Elasticsearch

import click

import loaders
import differs
from differs import State


MAPPING_COLOR = "cyan"
TEMPLATE_COLOR = "magenta"
SEP_COUNT = 10

ES_HOST = {
    "host": 'localhost',
    "port": 9200,
}
es = Elasticsearch(hosts=[ES_HOST])


@click.group()
def cli():
    pass


@cli.command()
@click.option('--path',
              default=os.getcwd(),
              help='Path to the project with Mappers.')
@click.option('--mappings', 'show_mappings', is_flag=True, default=False,
              help='Show mapping data from project')
@click.option('--templates', 'show_templates', is_flag=True, default=False,
              help='Show template data from project')
def show(path, show_mappings, show_templates):
    path = os.path.abspath(path)
    loader = loaders.ProjectMappingLoader(path)

    if show_mappings:
        mappings = loader.get_mappings()
        click.secho("*" * SEP_COUNT + " MAPPINGS " + "*" * SEP_COUNT, fg=MAPPING_COLOR)
        for name, mapping in mappings.items():
            click.secho("%s:" % name, fg=MAPPING_COLOR)
            data = json.dumps(mapping, indent=4)
            print(highlight(unicode(data, 'UTF-8'),
                            lexers.JsonLexer(),
                            formatters.TerminalFormatter()))

    if show_templates:
        templates = loader.get_templates()
        click.secho("*" * SEP_COUNT + " TEMPLATES " + "*" * SEP_COUNT, fg=TEMPLATE_COLOR)
        for name, template in templates.items():
            click.secho("%s:" % name, fg=TEMPLATE_COLOR)
            data = json.dumps(template, indent=4)
            print(highlight(unicode(data, 'UTF-8'),
                            lexers.JsonLexer(),
                            formatters.TerminalFormatter()))


def get_matching_indexes(key, mappings):
    pattern = re.compile(r"(?<=\{)(.*?)(?=\})")
    matches = pattern.findall(key)
    for match in matches:
        key = key.replace('{' + match + '}', "\\w")

    repl = re.compile(key)
    matches = []
    for key in mappings.keys():
        if repl.match(key):
            matches.append(key)

    return matches


def print_mapping_state(typename, states):
    table = []
    symbols = {
        State.ok: 'green',
        State.extra_field: 'blue',
        State.extra_param: 'blue',
        State.missing_field: 'yellow',
        State.missing_param: 'yellow',
        State.type_conflict: 'red',
        State.param_conflict: 'red',

    }
    color_weights = {
        'green': 0,
        'blue': 1,
        'yellow': 2,
        'red': 3,
    }
    weight = 0
    # TODO: reorganize this after accepting lists of issues
    # http://stackoverflow.com/questions/30419488/python-tabulate-format-want-to-display-the-table-with-one-blank-element
    # TODO: remove superfluous issues (e.g. when a type conflict exists with other minor conflicts)
    for fieldname, state_list in states.iteritems():
        for state in state_list:
            color = symbols[state.state]
            weight = max(weight, color_weights[color])
            table.append([colored(fieldname, color, attrs=['bold', ]),
                          colored(state.name, color),
                          state.description])

    title_color = {v: k for k, v in color_weights.iteritems()}[weight]
    print(colored("%s:" % typename, title_color, attrs=['bold', ]))
    headers = ['Field', 'Issue', 'Description']
    print tabulate(table, headers, tablefmt='fancy_grid')


@cli.command()
@click.option('--path',
              default=os.getcwd(),
              help='Path to the project with Mappers.')
@click.option('--mappings', 'show_mappings', is_flag=True, default=False,
              help='Show mapping data from project')
@click.option('--templates', 'show_templates', is_flag=True, default=False,
              help='Show template data from project')
def diff(path, show_mappings, show_templates):
    # import pprint
    path = os.path.abspath(path)
    loader = loaders.ProjectMappingLoader(path)

    if show_mappings:
        es_mappings = es.indices.get_mapping(index='*')
        data = json.dumps(es_mappings, indent=4)
        print(highlight(unicode(data, 'UTF-8'),
              lexers.JsonLexer(),
              formatters.TerminalFormatter()))
        # group types by index
        templates = collections.defaultdict(list)
        for name, mapper in loader.mappings.items():
            templates[mapper.template].append(mapper)

        for template, mappers in templates.iteritems():
            print("Template " +
                  colored(template.name, attrs=['bold', ]) +
                  " (%s):\n" % template.parse_index_template())
            for mapper in mappers:
                matches = get_matching_indexes(mapper.template.index, es_mappings)
                # TODO: control conflicts in different timed indices instead of looping here
                for match in matches:
                    # print "***********"
                    # print "mapper mapping:"
                    # pprint.pprint(mapping.generate_mapping())
                    # print "ES mapping:"
                    # pprint.pprint(es_mappings[match]['mappings'])
                    local_mapping = mapper.generate_mapping()[mapper.typename]['properties']
                    es_mapping = es_mappings[match]['mappings'][mapper.typename]['properties']
                    differ = differs.MappingDiffer(name,
                                                   local_mapping,
                                                   es_mapping)
                    states = differ.diff()
                    print_mapping_state(mapper.typename, states)
                    print('\n')

    if show_templates:
        templates = loader.get_templates()
        if not templates:
            print(colored("No templates found in ", 'red', attrs=['bold', ]) +
                  colored(path, 'red'))
            return

        for name, template in templates.items():
            exists = es.indices.exists_template(name)
            if not exists:
                print(colored("Template ", 'red') +
                      colored(name, 'red', attrs=['bold', ]) +
                      colored(" does not exist in ES ", 'red'))
            else:
                es_template = es.indices.get_template(name)
                data = json.dumps(es_template, indent=4)
                print(highlight(unicode(data, 'UTF-8'),
                                lexers.JsonLexer(),
                                formatters.TerminalFormatter()))
                local_types = set(template['mappings'].keys())
                es_types = set(es_template[name]['mappings'].keys())
                common_types = local_types.intersection(es_types)
                print("Template " +
                      colored(name, attrs=['bold', ]) +
                      " (%s):\n" % loader.templates[name].parse_index_template())
                for common_type in common_types:
                    local_mapping = template['mappings'][common_type]['properties']
                    es_mapping = es_template[name]['mappings'][common_type]['properties']
                    differ = differs.MappingDiffer(common_type,
                                                   local_mapping,
                                                   es_mapping)
                    states = differ.diff()
                    print_mapping_state(common_type, states)
                    print('\n')
                # TODO: test missing and extra types
                # missing_types = local_types.difference(es_types)
                # extra_types = es_types.difference(local_types)
                # TODO: test conflicts with fields with same name in different types
