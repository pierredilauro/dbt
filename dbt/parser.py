import copy
import os
import yaml

import dbt.flags
import dbt.model
import dbt.utils

import jinja2.runtime
import dbt.clients.jinja

import dbt.contracts.graph.parsed
import dbt.contracts.graph.unparsed
import dbt.contracts.project

from dbt.utils import NodeType
from dbt.logger import GLOBAL_LOGGER as logger

QUERY_VALIDATE_NOT_NULL = """
with validation as (
  select {field} as f
  from {ref}
)
select count(*) from validation where f is null
"""


QUERY_VALIDATE_UNIQUE = """
with validation as (
  select {field} as f
  from {ref}
  where {field} is not null
),
validation_errors as (
    select f from validation group by f having count(*) > 1
)
select count(*) from validation_errors
"""


QUERY_VALIDATE_ACCEPTED_VALUES = """
with all_values as (
  select distinct {field} as f
  from {ref}
),
validation_errors as (
    select f from all_values where f not in ({values_csv})
)
select count(*) from validation_errors
"""


QUERY_VALIDATE_REFERENTIAL_INTEGRITY = """
with parent as (
  select {parent_field} as id
  from {parent_ref}
), child as (
  select {child_field} as id
  from {child_ref}
)
select count(*) from child
where id not in (select id from parent) and id is not null
"""


def get_path(resource_type, package_name, resource_name):
    return "{}.{}.{}".format(resource_type, package_name, resource_name)


def get_model_path(package_name, resource_name):
    return get_path(NodeType.Model, package_name, resource_name)


def get_test_path(package_name, resource_name):
    return get_path(NodeType.Test, package_name, resource_name)


def get_macro_path(package_name, resource_name):
    return get_path('macros', package_name, resource_name)


def __config(model, cfg):

    def config(*args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0:
            opts = args[0]
        elif len(args) == 0 and len(kwargs) > 0:
            opts = kwargs
        else:
            dbt.utils.compiler_error(
                model.get('name'),
                "Invalid model config given inline in {}".format(model))

        cfg.update_in_model_config(opts)

    return config


def get_fqn(path, package_project_config, extra=[]):
    parts = dbt.utils.split_path(path)
    name, _ = os.path.splitext(parts[-1])
    fqn = ([package_project_config.get('name')] +
           parts[:-1] +
           extra +
           [name])

    return fqn


def parse_macro_file(macro_file_path,
                     macro_file_contents,
                     root_path,
                     package_name,
                     tags=None,
                     context=None):

    logger.debug("Parsing {}".format(macro_file_path))

    to_return = {}

    if tags is None:
        tags = set()

    if context is None:
        context = {
            'ref': lambda *args: '',
            'var': lambda *args: '',
            'target': property(lambda x: '', lambda x: x),
            'this': ''
        }

    base_node = {
        'resource_type': NodeType.Macro,
        'package_name': package_name,
        'depends_on': {
            'macros': [],
        }
    }

    template = dbt.clients.jinja.get_template(
        macro_file_contents, context, node=base_node)

    for key, item in template.module.__dict__.items():
        if type(item) == jinja2.runtime.Macro:
            unique_id = get_path(NodeType.Macro,
                                 package_name,
                                 key)

            new_node = base_node.copy()
            new_node.update({
                'name': key,
                'unique_id': unique_id,
                'tags': tags,
                'root_path': root_path,
                'path': macro_file_path,
                'raw_sql': macro_file_contents,
                'parsed_macro': item
            })
            to_return[unique_id] = new_node

    return to_return


def parse_node(node, node_path, root_project_config, package_project_config,
               all_projects, tags=None, fqn_extra=None):
    logger.debug("Parsing {}".format(node_path))
    node = copy.deepcopy(node)

    if tags is None:
        tags = set()

    if fqn_extra is None:
        fqn_extra = []

    node.update({
        'depends_on': {
            'nodes': [],
            'macros': [],
        }
    })

    fqn = get_fqn(node.get('path'), package_project_config, fqn_extra)

    config = dbt.model.SourceConfig(
        root_project_config, package_project_config, fqn)

    context = {}

    context['ref'] = lambda *args: ''
    context['config'] = __config(node, config)
    context['var'] = lambda *args: ''
    context['target'] = property(lambda x: '', lambda x: x)
    context['this'] = ''

    dbt.clients.jinja.get_rendered(
        node.get('raw_sql'), context, node,
        capture_macros=True)

    config_dict = node.get('config', {})
    config_dict.update(config.config)

    node['unique_id'] = node_path
    node['config'] = config_dict
    node['empty'] = (len(node.get('raw_sql').strip()) == 0)
    node['fqn'] = fqn
    node['tags'] = tags

    return node


def parse_sql_nodes(nodes, root_project, projects, tags=None):
    if tags is None:
        tags = set()

    to_return = {}

    dbt.contracts.graph.unparsed.validate_nodes(nodes)

    for node in nodes:
        package_name = node.get('package_name')

        node_path = get_path(node.get('resource_type'),
                             package_name,
                             node.get('name'))

        # TODO if this is set, raise a compiler error
        to_return[node_path] = parse_node(node,
                                          node_path,
                                          root_project,
                                          projects.get(package_name),
                                          projects,
                                          tags=tags)

    dbt.contracts.graph.parsed.validate_nodes(to_return)

    return to_return


def load_and_parse_sql(package_name, root_project, all_projects, root_dir,
                       relative_dirs, resource_type, tags=None):
    extension = "[!.#~]*.sql"

    if tags is None:
        tags = set()

    if dbt.flags.STRICT_MODE:
        dbt.contracts.project.validate_list(all_projects)

    file_matches = dbt.clients.system.find_matching(
        root_dir,
        relative_dirs,
        extension)

    result = []

    for file_match in file_matches:
        file_contents = dbt.clients.system.load_file_contents(
            file_match.get('absolute_path'))

        parts = dbt.utils.split_path(file_match.get('relative_path', ''))
        name, _ = os.path.splitext(parts[-1])

        if resource_type == NodeType.Test:
            path = dbt.utils.get_pseudo_test_path(
                name, file_match.get('relative_path'), 'data_test')
        else:
            path = file_match.get('relative_path')

        result.append({
            'name': name,
            'root_path': root_dir,
            'resource_type': resource_type,
            'path': path,
            'package_name': package_name,
            'raw_sql': file_contents
        })

    return parse_sql_nodes(result, root_project, all_projects, tags)


def load_and_parse_macros(package_name, root_project, all_projects, root_dir,
                          relative_dirs, resource_type, tags=None):
    extension = "[!.#~]*.sql"

    if tags is None:
        tags = set()

    if dbt.flags.STRICT_MODE:
        dbt.contracts.project.validate_list(all_projects)

    file_matches = dbt.clients.system.find_matching(
        root_dir,
        relative_dirs,
        extension)

    result = {}

    for file_match in file_matches:
        file_contents = dbt.clients.system.load_file_contents(
            file_match.get('absolute_path'))

        result.update(
            parse_macro_file(
                file_match.get('relative_path'),
                file_contents,
                root_dir,
                package_name))

    dbt.contracts.graph.parsed.validate_macros(result)

    return result


def parse_schema_tests(tests, root_project, projects):
    to_return = {}

    for test in tests:
        test_yml = yaml.safe_load(test.get('raw_yml'))

        if test_yml is None:
            continue

        for model_name, test_spec in test_yml.items():
            if test_spec is None or test_spec.get('constraints') is None:
                continue

            for test_type, configs in test_spec.get('constraints', {}).items():
                if configs is None:
                    continue

                for config in configs:
                    to_add = parse_schema_test(
                        test, model_name, config, test_type,
                        root_project,
                        projects.get(test.get('package_name')),
                        all_projects=projects)

                    if to_add is not None:
                        to_return[to_add.get('unique_id')] = to_add

    return to_return


def parse_schema_test(test_base, model_name, test_config, test_type,
                      root_project_config, package_project_config,
                      all_projects):
    if test_type == 'not_null':
        raw_sql = QUERY_VALIDATE_NOT_NULL.format(
            ref="{{ref('"+model_name+"')}}", field=test_config)
        name_key = test_config

    elif test_type == 'unique':
        raw_sql = QUERY_VALIDATE_UNIQUE.format(
            ref="{{ref('"+model_name+"')}}", field=test_config)
        name_key = test_config

    elif test_type == 'relationships':
        if not isinstance(test_config, dict):
            return None

        child_field = test_config.get('from')
        parent_field = test_config.get('field')
        parent_model = test_config.get('to')

        raw_sql = QUERY_VALIDATE_REFERENTIAL_INTEGRITY.format(
            child_field=child_field,
            child_ref="{{ref('"+model_name+"')}}",
            parent_field=parent_field,
            parent_ref=("{{ref('"+parent_model+"')}}"))

        name_key = '{}_to_{}_{}'.format(child_field, parent_model,
                                        parent_field)

    elif test_type == 'accepted_values':
        if not isinstance(test_config, dict):
            return None

        raw_sql = QUERY_VALIDATE_ACCEPTED_VALUES.format(
            ref="{{ref('"+model_name+"')}}",
            field=test_config.get('field', ''),
            values_csv="'{}'".format(
                "','".join([str(v) for v in test_config.get('values', [])])))

        name_key = test_config.get('field')

    else:
        raise dbt.exceptions.ValidationException(
            'Unknown schema test type {}'.format(test_type))

    name = '{}_{}_{}'.format(test_type, model_name, name_key)

    pseudo_path = dbt.utils.get_pseudo_test_path(name, test_base.get('path'),
                                                 'schema_test')

    to_return = {
        'name': name,
        'resource_type': test_base.get('resource_type'),
        'package_name': test_base.get('package_name'),
        'root_path': test_base.get('root_path'),
        'path': pseudo_path,
        'raw_sql': raw_sql
    }

    return parse_node(to_return,
                      get_test_path(test_base.get('package_name'),
                                    name),
                      root_project_config,
                      package_project_config,
                      all_projects,
                      tags={'schema'},
                      fqn_extra=None)


def load_and_parse_yml(package_name, root_project, all_projects, root_dir,
                       relative_dirs):
    extension = "[!.#~]*.yml"

    if dbt.flags.STRICT_MODE:
        dbt.contracts.project.validate_list(all_projects)

    file_matches = dbt.clients.system.find_matching(
        root_dir,
        relative_dirs,
        extension)

    result = []

    for file_match in file_matches:
        file_contents = dbt.clients.system.load_file_contents(
            file_match.get('absolute_path'))

        parts = dbt.utils.split_path(file_match.get('relative_path', ''))
        name, _ = os.path.splitext(parts[-1])

        result.append({
            'name': name,
            'root_path': root_dir,
            'resource_type': NodeType.Test,
            'path': file_match.get('relative_path'),
            'package_name': package_name,
            'raw_yml': file_contents
        })

    return parse_schema_tests(result, root_project, all_projects)


def parse_archives_from_projects(root_project, all_projects):
    archives = []
    to_return = {}

    for name, project in all_projects.items():
        archives = archives + parse_archives_from_project(project)

    for archive in archives:
        node_path = get_path(archive.get('resource_type'),
                             archive.get('package_name'),
                             archive.get('name'))

        to_return[node_path] = parse_node(
            archive,
            node_path,
            root_project,
            all_projects.get(archive.get('package_name')),
            all_projects)

    return to_return


def parse_archives_from_project(project):
    archives = []
    archive_configs = project.get('archive', [])

    for archive_config in archive_configs:
        tables = archive_config.get('tables')

        if tables is None:
            continue

        for table in tables:
            config = table.copy()
            config['source_schema'] = archive_config.get('source_schema')
            config['target_schema'] = archive_config.get('target_schema')

            archives.append({
                'name': table.get('target_table'),
                'root_path': project.get('project-root'),
                'resource_type': NodeType.Archive,
                'path': project.get('project-root'),
                'package_name': project.get('name'),
                'config': config,
                'raw_sql': '-- noop'
            })

    return archives
