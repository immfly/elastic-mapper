import json
import os
from pygments import highlight, lexers, formatters
from elasticsearch import Elasticsearch

import click

import loaders, differs


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
    import re
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


@cli.command()
@click.option('--path',
              default=os.getcwd(),
              help='Path to the project with Mappers.')
@click.option('--mappings', 'show_mappings', is_flag=True, default=False,
              help='Show mapping data from project')
@click.option('--templates', 'show_templates', is_flag=True, default=False,
              help='Show template data from project')
def diff(path, show_mappings, show_templates):
    from deepdiff import DeepDiff
    import pprint
    path = os.path.abspath(path)
    loader = loaders.ProjectMappingLoader(path)
    es_mappings = es.indices.get_mapping(index='*')
    data = json.dumps(es_mappings, indent=4)
    print(highlight(unicode(data, 'UTF-8'),
                    lexers.JsonLexer(),
                    formatters.TerminalFormatter()))

    if show_mappings:
        mappings = loader.mappings
        # print mappings
        for name, mapping in mappings.items():
            matches = get_matching_indexes(mapping.template.index, es_mappings)
            for match in matches:
                print "***********"
                pprint.pprint(mapping.generate_mapping())
                pprint.pprint(es_mappings[match]['mappings'])
                differ = differs.MappingDiffer(name,
                                               mapping.generate_mapping(),
                                               es_mappings[match]['mappings'])
                # diff = DeepDiff(mapping.generate_mapping(), es_mappings[match]['mappings'], ignore_order=True)
                # pprint.pprint(diff)
                # import ipdb; ipdb.set_trace()
