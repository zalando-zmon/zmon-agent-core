"""Discovery class used by agent core"""

# TODO: this is pilot implementation!

import os
import sys
import logging
import psycopg2

from . import kube


AGENT_TYPE = 'zmon-kubernetes-agent'

POD_TYPE = 'kube_pod'
SERVICE_TYPE = 'kube_service'
NODE_TYPE = 'kube_node'
REPLICASET_TYPE = 'kube_replicaset'
STATEFULSET_TYPE = 'kube_statefulset'
DAEMONSET_TYPE = 'kube_daemonset'
INGRESS_TYPE = 'kube_ingress'

POSTGRESQL_CLUSTER_TYPE = 'postgresql_cluster'
POSTGRESQL_CLUSTER_MEMBER_TYPE = 'postgresql_cluster_member'
POSTGRESQL_DATABASE_TYPE = 'postgresql_database'
POSTGRESQL_DEFAULT_PORT = 5432
POSTGRESQL_CONNECT_TIMEOUT = os.environ.get('ZMON_AGENT_POSTGRESQL_CONNECT_TIMEOUT', 2)

INSTANCE_TYPE_LABEL = 'beta.kubernetes.io/instance-type'

PROTECTED_FIELDS = ('id', 'type', 'infrastructure_account', 'created_by', 'region')

SERVICE_ACCOUNT_PATH = '/var/run/secrets/kubernetes.io/serviceaccount'

SKIPPED_ANNOTATIONS = (
    'kubernetes.io/created-by',
)

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
logger.setLevel(logging.INFO)


class Discovery:

    def __init__(self, region, infrastructure_account):
        # TODO: get config path from ENV variable
        self.namespace = os.environ.get('ZMON_AGENT_KUBERNETES_NAMESPACE')
        self.cluster_id = os.environ.get('ZMON_AGENT_KUBERNETES_CLUSTER_ID')
        self.alias = os.environ.get('ZMON_AGENT_KUBERNETES_CLUSTER_ALIAS', '')
        self.environment = os.environ.get('ZMON_AGENT_KUBERNETES_CLUSTER_ENVIRONMENT', '')

        self.postgres_user = os.environ.get('ZMON_AGENT_POSTGRES_USER')
        self.postgres_pass = os.environ.get('ZMON_AGENT_POSTGRES_PASS')
        if not (self.postgres_user and self.postgres_pass):
            logger.warning('No credentials provided for PostgreSQL database discovery!')

        if not self.cluster_id:
            raise RuntimeError('Cannot determine cluster ID. Please set env variable ZMON_AGENT_KUBERNETES_CLUSTER_ID')

        config_path = os.environ.get('ZMON_AGENT_KUBERNETES_CONFIG_PATH')
        self.kube_client = kube.Client(config_file_path=config_path)

        self.region = region
        self.infrastructure_account = infrastructure_account

    def get_filter_query(self) -> dict:
        return {'created_by': AGENT_TYPE, 'kube_cluster': self.cluster_id}

    def get_account_entity(self):
        entity = {
            'type': 'local',
            'infrastructure_account': self.infrastructure_account,
            'region': self.region,
            'kube_cluster': self.cluster_id,
            'alias': self.alias,
            'environment': self.environment,
            'id': 'kube-cluster[{}:{}]'.format(self.infrastructure_account, self.region),
            'created_by': AGENT_TYPE,
        }

        return entity

    def get_entities(self) -> list:

        pod_entities = get_cluster_pods(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            namespace=self.namespace)

        # Pass pod_entities in order to get node_pod_count!
        node_entities = get_cluster_nodes(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            pod_entities, namespace=self.namespace)

        service_entities = get_cluster_services(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            namespace=self.namespace)
        replicaset_entities = get_cluster_replicasets(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            namespace=self.namespace)
        daemonset_entities = get_cluster_daemonsets(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            namespace=self.namespace)
        statefulset_entities = get_cluster_statefulsets(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            namespace=self.namespace)

        ingress_entities = get_cluster_ingresses(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            namespace=self.namespace)

        postgresql_cluster_entities = get_postgresql_clusters(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            namespace=self.namespace)
        postgresql_cluster_member_entities = get_postgresql_cluster_members(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            namespace=self.namespace)
        postgresql_database_entities = get_postgresql_databases(
            postgresql_cluster_entities, self.cluster_id, self.alias, self.environment, self.region,
            self.infrastructure_account, self.postgres_user, self.postgres_pass)

        all_current_entities = (
            pod_entities + node_entities + service_entities + replicaset_entities + daemonset_entities +
            ingress_entities + statefulset_entities + postgresql_cluster_entities + postgresql_cluster_member_entities +
            postgresql_database_entities
        )

        return all_current_entities


def get_all(kube_client, kube_func, namespace=None) -> list:
    items = []

    namespaces = [namespace] if namespace else [ns.name for ns in kube_client.get_namespaces()]

    for ns in namespaces:
        items += list(kube_func(namespace=ns))

    return items


def add_labels_to_entity(entity: dict, labels: dict) -> dict:
    for label, val in labels.items():
        if label in (PROTECTED_FIELDS + SKIPPED_ANNOTATIONS):
            if label in PROTECTED_FIELDS:
                logger.warning('Skipping label [{}:{}] as it is in Protected entity fields {}'.format(
                    label, val, PROTECTED_FIELDS))
            continue

        entity[label] = val

    return entity


def get_cluster_pods(kube_client, cluster_id, alias, environment, region, infrastructure_account, namespace=None):
    """
    Return all Pods as ZMON entities.
    """
    entities = []

    pods = get_all(kube_client, kube_client.get_pods, namespace)

    for pod in pods:
        if not pod.ready:
            continue

        obj = pod.obj

        containers = obj['spec'].get('containers', [])
        container_statuses = {c['name']: c for c in obj['status']['containerStatuses']}
        conditions = {c['type']: c['status'] for c in obj['status']['conditions']}

        entity = {
            'id': 'pod-{}-{}[{}]'.format(pod.name, pod.namespace, cluster_id),
            'type': POD_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'ip': obj['status'].get('podIP', ''),
            'host': obj['status'].get('podIP', ''),

            'pod_name': pod.name,
            'pod_namespace': obj['metadata']['namespace'],
            'pod_host_ip': obj['status'].get('hostIP', ''),
            'pod_node_name': obj['spec']['nodeName'],

            'containers': {
                c['name']: {
                    'image': c['image'],
                    'ready': container_statuses.get(c['name'], {}).get('ready', True),
                    'restarts': container_statuses.get(c['name'], {}).get('restartCount', 0),
                    'ports': [p['containerPort'] for p in c.get('ports', []) if 'containerPort' in p],
                } for c in containers
            },

            'pod_phase': obj['status'].get('phase'),
            'pod_initialized': conditions.get('Initialized', False),
            'pod_ready': conditions.get('Ready', True),
            'pod_scheduled': conditions.get('PodScheduled', False),
        }

        entity = add_labels_to_entity(entity, obj['metadata'].get('labels', {}))
        entity = add_labels_to_entity(entity, obj['metadata'].get('annotations', {}))

        entities.append(entity)

    return entities


def get_cluster_services(kube_client, cluster_id, alias, environment, region, infrastructure_account, namespace=None):
    entities = []

    endpoints = get_all(kube_client, kube_client.get_endpoints, namespace)
    # number of endpoints per service
    endpoints_map = {e.name: len(e.obj['subsets']) for e in endpoints if e.obj.get('subsets')}

    services = get_all(kube_client, kube_client.get_services, namespace)

    for service in services:
        obj = service.obj

        host = obj['spec']['clusterIP']
        service_type = obj['spec']['type']
        if service_type == 'LoadBalancer':
            ingress = obj['status'].get('loadBalancer', {}).get('ingress', [])
            hostname = ingress[0].get('hostname') if ingress else None
            if hostname:
                host = hostname

        entity = {
            'id': 'service-{}-{}[{}]'.format(service.name, service.namespace, cluster_id),
            'type': SERVICE_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'ip': obj['spec']['clusterIP'],
            'host': host,
            'port': obj['spec']['ports'][0],  # Assume first port is the used one.

            'service_name': service.name,
            'service_namespace': obj['metadata']['namespace'],
            'service_type': service_type,
            'service_ports': obj['spec']['ports'],  # Could be useful when multiple ports are exposed.

            'endpoints_count': endpoints_map.get(service.name, 0),
        }

        entities.append(entity)

    return entities


def get_cluster_nodes(
        kube_client, cluster_id, alias, environment, region, infrastructure_account, pod_entities=None, namespace=None):
    entities = []

    nodes = kube_client.get_nodes()

    if not pod_entities:
        logger.warning('No pods supplied, Nodes will not show pod count!')

    node_pod_count = {}
    for pod in pod_entities:
        name = pod.get('pod_node_name')
        if name:
            node_pod_count[name] = node_pod_count.get(name, 0) + 1

    for node in nodes:
        obj = node.obj

        addresses = {address['type']: address['address'] for address in obj['status']['addresses']}
        ip = addresses.get('ExternalIP', addresses.get('InternalIP', ''))
        host = node.obj['metadata']['labels'].get('kubernetes.io/hostname', ip)
        instance_type = node.obj['metadata']['labels'].get(INSTANCE_TYPE_LABEL, '')
        statuses = {condition['type']: condition['status'] for condition in obj['status']['conditions']}

        entity = {
            'id': 'node-{}[{}]'.format(node.name, cluster_id),
            'type': NODE_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'ip': ip,
            'host': host,
            'external_ip': addresses.get('ExternalIP', ''),
            'internal_ip': addresses.get('InternalIP', ''),

            'node_name': node.name,
            'node_type': instance_type,
            'instance_type': instance_type,
            'pod_count': node_pod_count.get(node.name, 0),
            'pod_capacity': obj['status'].get('capacity', {}).get('pods', 0),
            'memory_capacity': obj['status'].get('capacity', {}).get('memory', 0),
            'pod_allocatable': obj['status'].get('allocatable', {}).get('pods', 0),
            'memory_allocatable': obj['status'].get('allocatable', {}).get('memory', 0),
            'image_count': len(obj['status'].get('images', [])),

            'container_runtime_version': obj['status']['nodeInfo']['containerRuntimeVersion'],
            'os_image': obj['status']['nodeInfo']['osImage'],
            'kernel_version': obj['status']['nodeInfo']['kernelVersion'],
            'kube_proxy_version': obj['status']['nodeInfo']['kubeProxyVersion'],
            'kubelet_version': obj['status']['nodeInfo']['kubeletVersion'],

            'node_ready': statuses.get('Ready', False),
            'node_out_of_disk': statuses.get('OutOfDisk', False),
            'node_memory_pressure': statuses.get('MemoryPressure', False),
            'node_disk_pressure': statuses.get('DiskPressure', False),
        }

        entity = add_labels_to_entity(entity, obj['metadata'].get('labels', {}))
        entity = add_labels_to_entity(entity, obj['metadata'].get('annotations', {}))

        entities.append(entity)

    return entities


def get_cluster_replicasets(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                            namespace=None):
    entities = []

    replicasets = get_all(kube_client, kube_client.get_replicasets, namespace)

    for replicaset in replicasets:
        obj = replicaset.obj

        containers = obj['spec']['template']['spec']['containers']

        entity = {
            'id': 'replicaset-{}-{}[{}]'.format(replicaset.name, replicaset.namespace, cluster_id),
            'type': REPLICASET_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'replicaset_name': replicaset.name,
            'replicaset_namespace': obj['metadata']['namespace'],

            'containers': {c['name']: c.get('image', '') for c in containers if 'name' in c},

            'replicas': obj['spec'].get('replicas', 0),
            'ready_replicas': obj['status'].get('readyReplicas', 0),
        }

        entity = add_labels_to_entity(entity, obj['metadata'].get('labels', {}))
        entity = add_labels_to_entity(entity, obj['metadata'].get('annotations', {}))

        entities.append(entity)

    return entities


def get_cluster_statefulsets(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                             namespace='default'):
    entities = []

    statefulsets = get_all(kube_client, kube_client.get_statefulsets, namespace)

    for statefulset in statefulsets:
        obj = statefulset.obj

        # Stale replic set?!
        if obj['spec']['replicas'] == 0:
            continue

        containers = obj['spec'].get('template', {}).get('spec', {}).get('containers', [])

        entity = {
            'id': 'statefulset-{}-{}[{}]'.format(statefulset.name, statefulset.namespace, cluster_id),
            'type': STATEFULSET_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'statefulset_name': statefulset.name,
            'statefulset_namespace': obj['metadata']['namespace'],
            'statefulset_service_name': obj['spec']['serviceName'],

            'volume_claims': {
                v['metadata']['name']: v['status'].get('phase', 'UNKNOWN')
                for v in obj['spec'].get('volumeClaimTemplates', [])
            },
            'containers': {c['name']: c.get('image', '') for c in containers if 'name' in c},

            'replicas': obj['spec'].get('replicas'),
            'replicas_status': obj['status'].get('replicas'),
        }

        entity = add_labels_to_entity(entity, obj['metadata'].get('labels', {}))
        entity = add_labels_to_entity(entity, obj['metadata'].get('annotations', {}))

        entities.append(entity)

    return entities


def get_cluster_daemonsets(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                           namespace='default'):
    entities = []

    daemonsets = get_all(kube_client, kube_client.get_daemonsets, namespace)

    for daemonset in daemonsets:
        obj = daemonset.obj

        containers = obj['spec']['template']['spec']['containers']

        entity = {
            'id': 'daemonset-{}-{}[{}]'.format(daemonset.name, daemonset.namespace, cluster_id),
            'type': DAEMONSET_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'daemonset_name': daemonset.name,
            'daemonset_namespace': obj['metadata']['namespace'],

            'containers': {c['name']: c.get('image', '') for c in containers if 'name' in c},

            'desired_count': obj['status'].get('desiredNumberScheduled', 0),
            'current_count': obj['status'].get('currentNumberScheduled', 0),
        }

        entity = add_labels_to_entity(entity, obj['metadata'].get('labels', {}))
        entity = add_labels_to_entity(entity, obj['metadata'].get('annotations', {}))

        entities.append(entity)

    return entities


def get_cluster_ingresses(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                          namespace='default'):
    entities = []

    ingresses = get_all(kube_client, kube_client.get_ingresses, namespace)

    for ingress in ingresses:
        obj = ingress.obj

        entity = {
            'id': 'ingress-{}-{}[{}]'.format(ingress.name, ingress.namespace, cluster_id),
            'type': INGRESS_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'ingress_name': ingress.name,
            'ingress_namespace': ingress.namespace,

            'ingress_rules': obj['spec'].get('rules', [])
        }

        entity = add_labels_to_entity(entity, obj['metadata'].get('labels', {}))

        entities.append(entity)

    return entities


########################################################################################################################
# POSTGRESQL   | TODO: move to separate discovery                                                                      #
########################################################################################################################
def list_postgres_databases(*args, **kwargs):
    logger.info("Trying to list DBs on host: {}".format(kwargs['host']))

    try:
        kwargs.update({'connect_timeout': POSTGRESQL_CONNECT_TIMEOUT})

        conn = psycopg2.connect(*args, **kwargs)

        cur = conn.cursor()
        cur.execute("""
            SELECT datname
              FROM pg_database
             WHERE datname NOT IN('postgres', 'template0', 'template1')
        """)
        return [row[0] for row in cur.fetchall()]
    except:
        logger.exception("Failed to list DBs!")
        return []


def get_postgresql_clusters(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                            namespace=None):
    entities = []

    services = get_all(kube_client, kube_client.get_services, namespace)

    for service in services:
        obj = service.obj

        # TODO: filter in the API call
        labels = obj['metadata'].get('labels', {})
        if labels.get('application') != 'spilo':
            continue

        service_namespace = obj['metadata']['namespace']
        service_dns_name = '{}.{}.svc.cluster.local'.format(service.name, service_namespace)

        entity = {
            'id': 'pg-{}[{}]'.format(service.name, cluster_id),
            'type': POSTGRESQL_CLUSTER_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'dnsname': service_dns_name,
            'shards': {
                'postgres': '{}:{}/postgres'.format(service_dns_name, POSTGRESQL_DEFAULT_PORT)
            }
        }

        entities.append(entity)

    return entities


def get_postgresql_cluster_members(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                                   namespace=None):
    entities = []

    pods = get_all(kube_client, kube_client.get_pods, namespace)
    pvcs = get_all(kube_client, kube_client.get_persistentvolumeclaims, namespace)
    pvs = get_all(kube_client, kube_client.get_persistentvolumes)

    for pod in pods:
        obj = pod.obj

        # TODO: filter in the API call
        labels = obj['metadata'].get('labels', {})
        if labels.get('application') != 'spilo' or labels.get('version') is None:
            continue

        pod_number = pod.name.split('-')[-1]
        pod_namespace = obj['metadata']['namespace']
        service_dns_name = '{}.{}.svc.cluster.local'.format(labels['version'], pod_namespace)

        # unfortunately, there appears to be no way of filtering these on the server side :(
        try:
            pvc_name = obj['spec']['volumes'][0]['persistentVolumeClaim']['claimName']  # assume only one PVC
            for pvc in pvcs:
                if pvc.name == pvc_name:
                    for pv in pvs:
                        if pv.name == pvc.obj['spec']['volumeName']:
                            ebs_volume_id = pv.obj['spec']['awsElasticBlockStore']['volumeID'].split('/')[-1]
                            break  # only one matching item is expected, so when found, we can leave the loop
                    break
        except:
            ebs_volume_id = ''

        entity = {
            'id': 'pg-{}-{}[{}]'.format(service_dns_name, pod_number, cluster_id),
            'type': POSTGRESQL_CLUSTER_MEMBER_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'dnsname': service_dns_name,
            'pod': pod.name,
            'volume': ebs_volume_id
        }

        entities.append(entity)

    return entities


def get_postgresql_databases(postgresql_clusters, cluster_id, alias, environment, region, infrastructure_account,
                             postgres_user, postgres_pass):
    if not (postgres_user and postgres_pass):
        return []

    entities = []

    for pgcluster in postgresql_clusters:
        dbnames = list_postgres_databases(host=pgcluster['dnsname'],
                                          port=POSTGRESQL_DEFAULT_PORT,
                                          user=postgres_user,
                                          password=postgres_pass,
                                          dbname='postgres',
                                          sslmode='require')
        for db in dbnames:
            entity = {
                'id': '{}-{}'.format(db, pgcluster['id']),
                'type': POSTGRESQL_DATABASE_TYPE,
                'kube_cluster': cluster_id,
                'alias': alias,
                'environment': environment,
                'created_by': AGENT_TYPE,
                'infrastructure_account': infrastructure_account,
                'region': region,

                'postgresql_cluster': pgcluster['id'],
                'database_name': db,
                'shards': {
                    db: '{}:{}/{}'.format(pgcluster['dnsname'], POSTGRESQL_DEFAULT_PORT, db)
                }
            }

            entities.append(entity)

    return entities
