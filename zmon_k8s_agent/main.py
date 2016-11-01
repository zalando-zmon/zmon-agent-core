import argparse
import os
import logging
import json

import requests
import tokens
import pykube

from zmon_cli.client import Zmon, compare_entities


AGENT_TYPE = 'zmon-k8s-agent'

logger = logging.getLogger(__name__)

POD_TYPE = 'k8s_pod'
SERVICE_TYPE = 'k8s_service'
NODE_TYPE = 'k8s_node'


INFRASTRUCTURE_ACCOUNT = 'k8s:zalando-zmon'
REGION = 'europe-west1-c'

DEFAULT_SERVICE_ACC = '/var/run/secrets/kubernetes.io/serviceaccount'


def get_clients(service_acc_path, zmon_url, verify=True):
    """Return Pykube and ZMon client instances as a tuple."""

    # Get token if set as ENV variable. This is useful in development.
    zmon_token = os.getenv('ZMON_AGENT_TOKEN')

    if not zmon_token:
        zmon_token = tokens.get('uid')

    if service_acc_path:
        config = pykube.KubeConfig.from_file(service_acc_path)
    else:
        # Assuming running on K8S cluster with default serviceaccount available!
        config = pykube.KubeConfig.from_service_account()

    return (pykube.HTTPClient(config),
            Zmon(zmon_url, token=zmon_token, verify=verify))


def get_cluster_pods(kube_client, region, infrastructure_account=INFRASTRUCTURE_ACCOUNT, namespace='default'):
    pods = pykube.Pod.objects(kube_client).filter(namespace=namespace)

    entities = []

    for pod in pods:
        if not pod.ready:
            continue

        obj = pod.obj

        entity = {
            'id': 'k8s-pod-{}-{}[{}:{}]'.format(pod.name, obj['metadata']['uid'], infrastructure_account, region),
            'type': POD_TYPE,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,
            'ip': obj['status']['podIP'],
            'host': obj['status']['podIP'],
            'pod_name': obj['metadata']['name'],
            'pod_uid': obj['metadata']['uid'],
            'pod_namespace': obj['metadata']['namespace'],
            'pod_host_ip': obj['status']['hostIP'],
        }

        for label, val in pod.obj['metadata']['labels'].items():
            entity['labels.{}'.format(label)] = val

        entities.append(entity)

    return entities


def get_cluster_services(kube_client, region, infrastructure_account=INFRASTRUCTURE_ACCOUNT, namespace='default'):
    services = pykube.Service.objects(kube_client).filter(namespace=namespace)

    entities = []

    for service in services:
        obj = service.obj

        entity = {
            'id': 'k8s-service-{}-{}[{}:{}]'.format(
                service.name, obj['metadata']['uid'], infrastructure_account, region),
            'type': SERVICE_TYPE,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,
            'ip': obj['spec']['clusterIP'],
            'host': obj['spec']['clusterIP'],
            'port': obj['spec']['ports'][0],  # Assume first port is the used one.
            'service_name': obj['metadata']['name'],
            'service_namespace': obj['metadata']['namespace'],
            'service_type': obj['spec']['type'],
            'service_ports': obj['spec']['ports'],  # Could be useful when multiple ports are exposed.
        }

        entities.append(entity)

    return entities


def get_cluster_nodes(kube_client):
    return []


def get_existing_ids(existing_entities):
    return [entity['id'] for entity in existing_entities]


def remove_missing_entities(existing_ids, current_ids, zmon_client, json=False):
    to_be_removed_ids = list(set(existing_ids) - set(current_ids))

    error_count = 0

    if not json:
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


def add_new_entities(all_current_entities, existing_entities, zmon_client, json=False):
    existing_entities_dict = {e['id']: e for e in existing_entities}
    new_entities = [e for e in all_current_entities if new_or_updated_entity(e, existing_entities_dict)]

    error_count = 0

    if not json:
        try:
            logger.info('Found {} new entities to be added in ZMon'.format(len(new_entities)))
            for entity in new_entities:
                logger.info(
                    'Adding new {} entity with ID: {}'.format(entity['type'], entity['id']))
                # resp = zmon_client.add_entity(entity)
        except:
            logger.exception('Failed to add entity!')
            error_count += 1

    return new_entities, error_count


def main():
    argp = argparse.ArgumentParser(description='ZMON K8S Agent')
    argp.add_argument('-s', '--service-account-path', dest='service_acc_path', default=None,
                      help='K8S service account path. Can be set via ZMON_SERVICE_ACCOUNT_PATH env variable.')
    argp.add_argument('-n', '--namespace', dest='namespace', default='default', help='K8S cluster namespace.')

    argp.add_argument('-e', '--entity-service', dest='entityservice',
                      help='ZMON REST endpoint. Can be set via ZMON_ENTITY_SERVICE_URL env variable.')

    argp.add_argument('-r', '--region', dest='region', default=None)

    argp.add_argument('-j', '--json', dest='json', action='store_true', help='Print JSON output only.', default=False)
    argp.add_argument('--skip-ssl', dest='skip_ssl', action='store_true', default=False)
    argp.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False, help='Verbose output.')

    args = argp.parse_args()

    service_acc_path = args.service_acc_path if args.service_acc_path else os.environ.get('ZMON_SERVICE_ACCOUNT_PATH')
    entityservice = args.entityservice if args.entityservice else os.environ.get(
        'ZMON_ENTITY_SERVICE_URL', 'https://zmon.zalando.net')

    tokens.configure()
    tokens.manage('uid', ['uid'])
    tokens.start()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    verify = True
    if args.skip_ssl:
        logger.warning('ZMON K8S agent will skip SSL verification!')
        verify = False

    if not args.region:
        logger.info('Trying to figure out region..')
        try:
            response = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone', timeout=2)
        except:
            logger.error('Region was not specified as a parameter and can not be fetched from instance meta-data!')
            raise
        region = response.text[:-1]
    else:
        region = args.region

    kube_client, zmon_client = get_clients(service_acc_path, entityservice, verify=verify)

    # 1) Get cluster pods
    pod_entities = get_cluster_pods(kube_client, region, namespace=args.namespace)

    # 2) Get cluster services
    service_entities = get_cluster_services(kube_client, region, namespace=args.namespace)

    # 3) Get cluster nodes
    node_entities = get_cluster_nodes(kube_client)

    # 4) Get ZMON entities
    query = {'created_by': AGENT_TYPE}
    existing_entities = zmon_client.get_entities(query=query)

    # 5) Remove non-existing entities
    existing_ids = get_existing_ids(existing_entities)

    all_current_entities = pod_entities + service_entities + node_entities

    current_ids = [entity['id'] for entity in all_current_entities]

    to_be_removed_ids, delete_err = remove_missing_entities(existing_ids, current_ids, zmon_client, json=args.json)

    # 6) Add new entities
    new_entities, add_err = add_new_entities(all_current_entities, existing_entities, zmon_client, json=args.json)

    if args.json:
        output = {
            'to_be_removed_ids': to_be_removed_ids,
            'new_entities': new_entities
        }

        print(json.dumps(output, indent=4))


if __name__ == '__main__':
    main()
