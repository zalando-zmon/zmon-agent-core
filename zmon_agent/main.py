from opentracing_utils import trace_requests
trace_requests()  # noqa

import os
import sys
import time
import argparse
import logging
import json
import traceback

import requests
import tokens
import opentracing

from zmon_cli.client import Zmon, compare_entities

from opentracing_utils import init_opentracing_tracer, trace, extract_span_from_kwargs

# TODO: Load dynamically
from zmon_agent.discovery.kubernetes import get_discovery_agent_class


BUILTIN_DISCOVERY = ('kubernetes',)

AGENT_TYPE = 'zmon-agent'

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
logger.setLevel(logging.INFO)


def get_clients(zmon_url, verify=True) -> Zmon:
    """Return Pykube and Zmon client instances as a tuple."""

    # Get token if set as ENV variable. This is useful in development.
    zmon_token = os.getenv('ZMON_AGENT_TOKEN')

    if not zmon_token:
        zmon_token = tokens.get('uid')

    return Zmon(zmon_url, token=zmon_token, verify=verify)


def get_existing_ids(existing_entities):
    return [entity['id'] for entity in existing_entities]


@trace(pass_span=True)
def remove_missing_entities(existing_ids, current_ids, zmon_client, dry_run=False, **kwargs):
    current_span = extract_span_from_kwargs(**kwargs)

    to_be_removed_ids = list(set(existing_ids) - set(current_ids))

    error_count = 0

    if not dry_run:
        logger.info('Removing {} entities from ZMON'.format(len(to_be_removed_ids)))
        for entity_id in to_be_removed_ids:
            logger.info('Removing entity with id: {}'.format(entity_id))
            try:
                deleted = zmon_client.delete_entity(entity_id)
                if not deleted:
                    current_span.set_tag('error', True)
                    logger.info('Failed to delete entity!')
                    error_count += 1
            except Exception:
                current_span.set_tag('error', True)
                current_span.log_kv({'exception': traceback.format_exc()})

    return to_be_removed_ids, error_count


def new_or_updated_entity(entity, existing_entities_dict):
    # check if new entity
    if entity['id'] not in existing_entities_dict:
        return True

    existing_entities_dict[entity['id']].pop('last_modified', None)

    return not compare_entities(entity, existing_entities_dict[entity['id']])


@trace(pass_span=True)
def add_new_entities(all_current_entities, existing_entities, zmon_client, dry_run=False, **kwargs):
    current_span = extract_span_from_kwargs(**kwargs)

    existing_entities_dict = {e['id']: e for e in existing_entities}
    new_entities = [e for e in all_current_entities if new_or_updated_entity(e, existing_entities_dict)]

    error_count = 0

    if not dry_run:
        logger.info('Found {} new or updated entities to be added in ZMON'.format(len(new_entities)))
        for entity in new_entities:
            logger.info('Adding new or updated {} entity with ID: {}'.format(entity['type'], entity['id']))
            try:
                resp = zmon_client.add_entity(entity)
                resp.raise_for_status()
            except Exception:
                current_span.set_tag('error', True)
                logger.exception('Failed to add entity!')
                current_span.log_kv({'exception': traceback.format_exc(), "entity": entity})
                error_count += 1

    return new_entities, error_count


def sync(infrastructure_account, region, entity_service, verify, dry_run, interval):
    # TODO: load agent dynamically!
    Discovery = get_discovery_agent_class()
    discovery = Discovery(region, infrastructure_account)

    while True:
        try:
            sync_span = opentracing.tracer.start_span(operation_name='zmon-agent-sync')

            sync_span.set_tag('account', infrastructure_account)
            sync_span.set_tag('region', region)
            sync_span.log_kv({'sync_interval': interval, 'discovery_plugin_count': 1})

            with sync_span:
                zmon_client = get_clients(entity_service, verify=verify)

                discovery_tags = discovery.get_discovery_tags()
                for k, v in discovery_tags.items():
                    sync_span.set_tag(k, v)

                account_entity = discovery.get_account_entity()

                all_current_entities = discovery.get_entities() + [account_entity]

                # ZMON entities
                query = discovery.get_filter_query()
                existing_entities = zmon_client.get_entities(query=query)

                # Remove non-existing entities
                existing_ids = get_existing_ids(existing_entities)

                current_ids = [entity['id'] for entity in all_current_entities]

                to_be_removed_ids, delete_err = remove_missing_entities(
                    existing_ids, current_ids, zmon_client, dry_run=dry_run)

                # Add new entities
                new_entities, add_err = add_new_entities(
                    all_current_entities, existing_entities, zmon_client, dry_run=dry_run)

                logger.info('Found {} new entities from {} entities ({} failed)'.format(
                    len(new_entities), len(all_current_entities), add_err))

                sync_span.log_kv({
                    'new_entities': len(new_entities),
                    'removed_entities': len(to_be_removed_ids),
                    'total_entities': len(all_current_entities),
                })

                # Add account entity - always!
                if not dry_run:
                    try:
                        account_entity['errors'] = {'delete_count': delete_err, 'add_count': add_err}
                        sync_span.log_kv({'delete_error_count': delete_err, 'add_error_count': add_err})
                        zmon_client.add_entity(account_entity)
                    except Exception:
                        sync_span.set_tag('error', True)
                        sync_span.set_tag('local-entity-error', True)
                        sync_span.log_kv({'exception': traceback.format_exc()})
                        logger.exception('Failed to add account entity!')

                logger.info(
                    'ZMON agent completed sync with {} addition errors and {} deletion errors'.format(
                        add_err, delete_err))

                if dry_run:
                    output = {
                        'to_be_removed_ids': to_be_removed_ids,
                        'new_entities': new_entities
                    }

                    print(json.dumps(output, indent=4))

                if not interval:
                    logger.info('ZMON agent running once. Exiting!')
                    break

            logger.info('ZMON agent sleeping for {} seconds ...'.format(interval))
            time.sleep(interval)
        except (KeyboardInterrupt, SystemExit):
            break
        except Exception:
            fail_sleep = interval if interval else 60
            logger.exception('ZMON agent failed. Retrying after {} seconds ...'.format(fail_sleep))
            time.sleep(fail_sleep)


def main():
    argp = argparse.ArgumentParser(description='ZMON Kubernetes Agent')

    argp.add_argument('-i', '--infrastructure-account', dest='infrastructure_account', default=None,
                      help='Infrastructure account which identifies this agent. Can be set via  '
                           'ZMON_AGENT_INFRASTRUCTURE_ACCOUNT env variable.')
    argp.add_argument('-r', '--region', dest='region',
                      help='Cluster region. Can be set via ZMON_AGENT_REGION env variable.')

    argp.add_argument('-d', '--discover', dest='discover',
                      help=('Comma separated list of builtin discovery agents to be used. Current supported discovery '
                            'agents are {}. Can be set via ZMON_AGENT_BUILTIN_DISCOVERY env variable.').format(
                                BUILTIN_DISCOVERY))

    argp.add_argument('-e', '--entity-service', dest='entity_service',
                      help='ZMON backend URL. Can be set via ZMON_AGENT_ENTITY_SERVICE_URL env variable.')

    argp.add_argument('--interval', dest='interval',
                      help='Interval for agent sync. If not set then agent will run once. Can be set via '
                      'ZMON_AGENT_INTERVAL env variable.')

    # OPENTRACING SUPPORT
    argp.add_argument('--opentracing', dest='opentracing', default=os.environ.get('ZMON_AGENT_OPENTRACING_TRACER'),
                      help='OpenTracing tracer name as supported by opentracing-utils. Please Ignore for NOOP tracer.')

    argp.add_argument('-j', '--json', dest='json', action='store_true', help='Print JSON output only.', default=False)
    argp.add_argument('--skip-ssl', dest='skip_ssl', action='store_true', default=False)
    argp.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False, help='Verbose output.')

    args = argp.parse_args()

    logger.info('Initializing opentracing tracer: {}'.format(args.opentracing if args.opentracing else 'noop'))
    init_opentracing_tracer(args.opentracing)

    # Give some time for tracer initialization
    if args.opentracing:
        time.sleep(2)

    init_span = opentracing.tracer.start_span(operation_name='zmon-agent-init')

    with init_span:
        # Hard requirements
        infrastructure_account = (args.infrastructure_account if args.infrastructure_account else
                                  os.environ.get('ZMON_AGENT_INFRASTRUCTURE_ACCOUNT'))
        if not infrastructure_account:
            init_span.set_tag('error', True)
            raise RuntimeError('Cannot determine infrastructure account. Please use --infrastructure-account option or '
                               'set env variable ZMON_AGENT_INFRASTRUCTURE_ACCOUNT.')

        region = os.environ.get('ZMON_AGENT_REGION', args.region)
        entity_service = os.environ.get('ZMON_AGENT_ENTITY_SERVICE_URL', args.entity_service)
        interval = os.environ.get('ZMON_AGENT_INTERVAL', args.interval)

        init_span.set_tag('account', infrastructure_account)
        init_span.set_tag('region', region)

        if interval:
            interval = int(interval)

        # OAUTH2 tokens
        tokens.configure()
        tokens.manage('uid', ['uid'])

        verbose = args.verbose if args.verbose else os.environ.get('ZMON_AGENT_DEBUG', False)
        if verbose:
            logger.setLevel(logging.DEBUG)

        verify = True
        if args.skip_ssl:
            logger.warning('ZMON agent will skip SSL verification!')
            verify = False

        if not region:
            # Assuming running on AWS
            logger.info('Trying to figure out region ...')
            try:
                response = requests.get(
                    'http://169.254.169.254/latest/meta-data/placement/availability-zone', timeout=2)

                response.raise_for_status()

                region = response.text[:-1]
            except Exception:
                init_span.set_tag('error', True)
                logger.error('AWS region was not specified and can not be fetched from instance meta-data!')
                raise

    logger.info('Starting sync operations!')

    sync(infrastructure_account, region, entity_service, verify, args.json, interval)

    if args.opentracing:
        time.sleep(5)


if __name__ == '__main__':
    main()
