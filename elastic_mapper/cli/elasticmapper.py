import json
import os

import click

import loaders


@click.command()
@click.argument('project_path', default=os.getcwd())
def elasticmapper(project_path):
    project_path = os.path.abspath(project_path)
    print project_path
    click.secho("project path: %s" % project_path, fg="blue")
    loader = loaders.ProjectMappingLoader(project_path)
    for name, mapping in loader.get_mappings().items():
        click.secho("%s:" % name, fg="green")
        print json.dumps(mapping, indent=4)

    for name, template in loader.get_templates().items():
        click.secho("%s:" % name, fg="blue")
        print json.dumps(template, indent=4)
