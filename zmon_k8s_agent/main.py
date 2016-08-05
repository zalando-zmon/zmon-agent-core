import argparse
import os
import logging
import json

import tokens
import pykube

from zmon_k8s_agent.zmon import ZMon

AGENT_TYPE = 'zmon-k8s-agent'

logger = logging.getLogger(__name__)

POD_TYPE = 'k8s_pod'
SERVICE_TYPE = 'k8s_service'
NODE_TYPE = 'k8s_node'


INFRASTRUCTURE_ACCOUNT = 'k8s:zalando-zmon'
REGION = 'europe-west1-c'


def get_clients(service_acc_path, zmon_url, disable_oauth2=False, verify=True):
    """Return Pykube and ZMon client instances as a tuple."""

    # Get token if set as ENV variable. This is useful in development.
    zmon_token = os.getenv('ZMON_AGENT_TOKEN')

    if not disable_oauth2 and not zmon_token:
        zmon_token = tokens.get('uid')

    zmon_user = os.getenv('ZMON_USER')
    zmon_password = os.getenv('ZMON_PASSWORD')

    # config = pykube.KubeConfig.from_service_account(path=service_acc_path)
    config = pykube.KubeConfig.from_file(service_acc_path)

    return (pykube.HTTPClient(config),
            ZMon(zmon_url, token=zmon_token, username=zmon_user, password=zmon_password, verify=verify))


def get_cluster_pods(kube_client, namespace, infrastructure_account=INFRASTRUCTURE_ACCOUNT, region=REGION):
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


def get_cluster_services(kube_client, namespace, infrastructure_account=INFRASTRUCTURE_ACCOUNT, region=REGION):
    services = pykube.Service.objects(kube_client).filter(namespace=namespace)

    entities = []

    for service in services:
        obj = service.obj

        entity = {
            'id': 'k8s-pod-{}-{}[{}:{}]'.format(service.name, obj['metadata']['uid'], infrastructure_account, region),
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

    if not json:
        logger.info('Removing {} entities from ZMon'.format(len(to_be_removed_ids)))
        for entity_id in to_be_removed_ids:
            logger.info('Removing entity with id: {}'.format(entity_id))
            deleted = zmon_client.delete_entity(entity_id)
            if deleted:
                logger.info('ZMon entity deleted successfully')
            else:
                logger.info('Failed to delete entity!')

    return to_be_removed_ids


def add_new_entities(all_current_entities, current_ids, existing_ids, zmon_client, json=False):
    new_entities = [e for e in all_current_entities if e['id'] in list(set(current_ids) - set(existing_ids))]

    if not json:
        try:
            logger.info('Found {} new entities to be added in ZMon'.format(len(new_entities)))
            for entity in new_entities:
                logger.info(
                    'Adding new {} entity with ID: {}'.format(entity['type'], entity['id']))
                # resp = zmon_client.add_entity(entity)
                # logger.info('ZMon response ... {}'.format(resp.status_code))
        except:
            logger.exception('Failed to add entity!')

    return new_entities


def main():
    argp = argparse.ArgumentParser(description='ZMon K8S Agent')
    argp.add_argument('-s', '--service-account-path', dest='service_acc_path', help='K8S service account path.')
    argp.add_argument('-n', '--namespace', dest='namespace', help='K8S cluster namespace.')
    argp.add_argument('-e', '--entity-service', dest='entityservice', help='ZMon REST endpoint.')
    argp.add_argument('-j', '--json', dest='json', action='store_true', help='Print JSON output only.', default=False)
    argp.add_argument('--no-oauth2', dest='disable_oauth2', action='store_true', default=False)
    argp.add_argument('--skip-ssl', dest='skip_ssl', action='store_true', default=False)
    argp.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False, help='Verbose output.')

    args = argp.parse_args()

    if not args.disable_oauth2:
        tokens.configure()
        tokens.manage('uid', ['uid'])
        tokens.start()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    verify = True
    if args.skip_ssl:
        logger.warning('ZMON K8S agent will skip SSL verification!')
        verify = False

    kube_client, zmon_client = get_clients(
        args.service_acc_path, args.entityservice, disable_oauth2=args.skip_ssl, verify=verify)

    # 1) Get cluster pods
    pod_entities = get_cluster_pods(kube_client, args.namespace)

    # 2) Get cluster services
    service_entities = get_cluster_services(kube_client, args.namespace)

    # 3) Get cluster nodes
    node_entities = get_cluster_nodes(kube_client)

    # 4) Get ZMON entities
    query = {'created_by': AGENT_TYPE}
    existing_entities = zmon_client.get_entities(query=query)

    # 5) Remove non-existing entities
    existing_ids = get_existing_ids(existing_entities)

    all_current_entities = pod_entities + service_entities + node_entities

    current_ids = [entity['id'] for entity in all_current_entities]

    to_be_removed_ids = remove_missing_entities(existing_ids, current_ids, zmon_client, json=args.json)

    # 6) Add new entities
    new_entities = add_new_entities(all_current_entities, current_ids, existing_ids, zmon_client, json=args.json)

    if args.json:
        output = {
            'to_be_removed_ids': to_be_removed_ids,
            'new_entities': new_entities
        }

        print(json.dumps(output, indent=4))


if __name__ == '__main__':
    main()
