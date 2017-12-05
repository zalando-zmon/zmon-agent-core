import os
import sys
import time
import argparse
import logging
import json

import requests
import tokens

from zmon_cli.client import Zmon, compare_entities

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


def remove_missing_entities(existing_ids, current_ids, zmon_client, dry_run=False):
    to_be_removed_ids = list(set(existing_ids) - set(current_ids))

    error_count = 0

    if not dry_run:
        logger.info('Removing {} entities from ZMon'.format(len(to_be_removed_ids)))
        for entity_id in to_be_removed_ids:
            logger.info('Removing entity with id: {}'.format(entity_id))
            deleted = zmon_client.delete_entity(entity_id)
            if not deleted:
                logger.info('Failed to delete entity!')
                error_count += 1

    return to_be_removed_ids, error_count


def new_or_updated_entity(entity, existing_entities_dict):
    # check if new entity
    if entity['id'] not in existing_entities_dict:
        return True

    existing_entities_dict[entity['id']].pop('last_modified', None)

    return not compare_entities(entity, existing_entities_dict[entity['id']])


def add_new_entities(all_current_entities, existing_entities, zmon_client, dry_run=False):
    existing_entities_dict = {e['id']: e for e in existing_entities}
    new_entities = [e for e in all_current_entities if new_or_updated_entity(e, existing_entities_dict)]

    error_count = 0

    if not dry_run:
        try:
            logger.info('Found {} new entities to be added in ZMon'.format(len(new_entities)))
            for entity in new_entities:
                logger.info(
                    'Adding new {} entity with ID: {}'.format(entity['type'], entity['id']))

                resp = zmon_client.add_entity(entity)

                resp.raise_for_status()
        except Exception:
            logger.exception('Failed to add entity!')
            error_count += 1

    return new_entities, error_count


def sync(infrastructure_account, region, entity_service, verify, dry_run, interval):
    # TODO: load agent dynamically!
    Discovery = get_discovery_agent_class()
    discovery = Discovery(region, infrastructure_account)

    while True:
        try:
            zmon_client = get_clients(entity_service, verify=verify)

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

            # Add account entity - always!
            if not dry_run:
                try:
                    account_entity['errors'] = {'delete_count': delete_err, 'add_count': add_err}
                    zmon_client.add_entity(account_entity)
                except Exception:
                    logger.exception('Failed to add account entity!')

            logger.info(
                'ZMON agent completed sync with {} addition errors and {} deletion errors'.format(add_err, delete_err))

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
        except KeyboardInterrupt:
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

    argp.add_argument('-j', '--json', dest='json', action='store_true', help='Print JSON output only.', default=False)
    argp.add_argument('--skip-ssl', dest='skip_ssl', action='store_true', default=False)
    argp.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False, help='Verbose output.')

    args = argp.parse_args()

    # Hard requirements
    infrastructure_account = (args.infrastructure_account if args.infrastructure_account else
                              os.environ.get('ZMON_AGENT_INFRASTRUCTURE_ACCOUNT'))
    if not infrastructure_account:
        raise RuntimeError('Cannot determine infrastructure account. Please use --infrastructure-account option or '
                           'set env variable ZMON_AGENT_INFRASTRUCTURE_ACCOUNT.')

    region = os.environ.get('ZMON_AGENT_REGION', args.region)
    entity_service = os.environ.get('ZMON_AGENT_ENTITY_SERVICE_URL', args.entity_service)
    interval = os.environ.get('ZMON_AGENT_INTERVAL', args.interval)

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
            response = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone', timeout=2)

            response.raise_for_status()

            region = response.text[:-1]
        except Exception:
            logger.error('AWS region was not specified and can not be fetched from instance meta-data!')
            raise

    sync(infrastructure_account, region, entity_service, verify, args.json, interval)


if __name__ == '__main__':
    main()
