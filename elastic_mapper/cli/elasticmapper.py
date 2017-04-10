import collections
import json
import os
from pygments import highlight, lexers, formatters
from tabulate import tabulate
from termcolor import colored
from elasticsearch import Elasticsearch

import click
import six

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
            print(highlight(six.text_type(data),
                            lexers.JsonLexer(),
                            formatters.TerminalFormatter()))

    if show_templates:
        templates = loader.get_templates()
        click.secho("*" * SEP_COUNT + " TEMPLATES " + "*" * SEP_COUNT, fg=TEMPLATE_COLOR)
        for name, template in templates.items():
            click.secho("%s:" % name, fg=TEMPLATE_COLOR)
            data = json.dumps(template, indent=4)
            print(highlight(six.text_type(data),
                            lexers.JsonLexer(),
                            formatters.TerminalFormatter()))


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
        State.inconsistent_field: 'red',
        State.index_type_conflict: 'red',
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
    for fieldname, state_list in six.iteritems(states):
        for state in state_list:
            color = symbols[state.state]
            weight = max(weight, color_weights[color])
            table.append([colored(fieldname, color, attrs=['bold', ]),
                          colored(state.name, color),
                          state.description])

    title_color = {v: k for k, v in six.iteritems(color_weights)}[weight]
    print(colored("%s:" % typename, title_color, attrs=['bold', ]))
    headers = ['Field', 'Issue', 'Description']
    print(tabulate(table, headers, tablefmt='fancy_grid'))


def print_template_state(template, states):
    print("Template " +
          colored(template.name, attrs=['bold', ]) +
          " (%s):\n" % template.parse_index_template())

    for issue in states.template_states:
        if issue.state == State.template_extra_type:
            print(colored("Type ", 'blue') +
                  colored("%s" % issue.typename, 'blue', attrs=['bold', ]) +
                  colored(" not exported to ES yet", 'blue'))
        if issue.state == State.template_missing_type:
            print(colored("Type ", 'yellow') +
                  colored("%s" % issue.typename, 'yellow', attrs=['bold', ]) +
                  colored(" added to Elasticsearch but not defined in the temmplate", 'yellow'))

    for typename, issues in six.iteritems(states.type_states):
        print("\n")
        print_mapping_state(typename, issues)


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
        print(highlight(six.text_type(data), lexers.JsonLexer(), formatters.TerminalFormatter()))
        # group types by index
        templates = collections.defaultdict(list)
        for name, mapper in loader.mappings.items():
            templates[mapper.template].append(mapper)

        for template, mappers in six.iteritems(templates):
            print("Template " +
                  colored(template.name, attrs=['bold', ]) +
                  " (%s):\n" % template.parse_index_template())
            for mapper in mappers:
                es_mappings = es.indices.get_mapping(index="*")
                differ = differs.TimelyIndexDiffer(template.name,
                                                   mapper.generate_mapping(),
                                                   template.index,
                                                   es_mappings)
                states = differ.diff()
                print_mapping_state(mapper.typename, states)
                # matches = parsers.get_matching_indexes(mapper.template.index, es_mappings)
                # # TODO: control conflicts in different timed indices instead of looping here
                # for match in matches:
                #     # print "***********"
                #     # print "mapper mapping:"
                #     # pprint.pprint(mapping.generate_mapping())
                #     # print "ES mapping:"
                #     # pprint.pprint(es_mappings[match]['mappings'])
                #     local_mapping = mapper.generate_mapping()[mapper.typename]['properties']
                #     es_mapping = es_mappings[match]['mappings'][mapper.typename]['properties']
                #     differ = differs.MappingDiffer(name,
                #                                    local_mapping,
                #                                    es_mapping)
                #     states = differ.diff()
                #     print_mapping_state(mapper.typename, states)
                #     print('\n')

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
                print(highlight(six.text_type(data),
                                lexers.JsonLexer(),
                                formatters.TerminalFormatter()))
                differ = differs.TemplateDiffer(name,
                                                template['mappings'],
                                                es_template[name]['mappings'])
                states = differ.diff()
                print_template_state(loader.templates[name], states)


@cli.command()
@click.option('--path',
              default=os.getcwd(),
              help='Path to the project with Mappers.')
@click.option('--mappings', 'sync_mappings', is_flag=True, default=False,
              help='Put mapping data from project')
@click.option('--templates', 'sync_templates', is_flag=True, default=False,
              help='Put template data from project')
@click.option('--yes', 'skip_confirmation', is_flag=True, default=False,
              help="Don't prompt for confirmation when pushing to Elasticsearch")
@click.option('--dry', 'dry_run', is_flag=True, default=False,
              help="Show the steps to be performed without actually running them")
def sync(path, sync_mappings, sync_templates, skip_confirmation, dry_run):
    path = os.path.abspath(path)
    loader = loaders.ProjectMappingLoader(path)

    if sync_mappings:
        raise NotImplementedError(colored("Mapping sync is not supported yet", 'red'))

    if sync_templates:
        if not loader.get_templates():
            print(colored("No templates found in ", 'red', attrs=['bold', ]) +
                  colored(path, 'red'))
            return

        templates = collections.defaultdict(list)
        for name, mapper in loader.mappings.items():
            templates[mapper.template].append(mapper)

        click.echo("Detected templates:")
        for template, mappers in six.iteritems(templates):
            click.echo("   - %s (%s)" % (template.name, template.parse_index_template()))
            for mapper in mappers:
                click.echo("      - %s" % (mapper.typename))

        if not skip_confirmation:
            click.confirm("Are you sure you want to put the project templates into Elasticsearch?",
                          abort=True)
        for name, template in loader.get_templates().items():
            click.echo("Putting template %s..." % name)
            if not dry_run:
                es.indices.put_template(name, template)
            exists = es.indices.exists_template(name)
            if exists or dry_run:
                click.secho("Successfully added template %s to Elasticsearch" % name,
                            fg='green')
            else:
                click.secho("Error adding template %s to Elasticsearch" % name,
                            fg='red')
