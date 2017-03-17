"""Discovery class used by agent core"""

# TODO: this is pilot implementation!

import os
import logging

from . import kube


AGENT_TYPE = 'zmon-kubernetes-agent'

POD_TYPE = 'kube_pod'
SERVICE_TYPE = 'kube_service'
NODE_TYPE = 'kube_node'
REPLICASET_TYPE = 'kube_replicaset'
STATEFULSET_TYPE = 'kube_statefulset'
DAEMONSET_TYPE = 'kube_daemonset'

INSTANCE_TYPE_LABEL = 'beta.kubernetes.io/instance-type'

PROTECTED_FIELDS = ('id', 'type', 'infrastructure_account', 'created_by', 'region')

SERVICE_ACCOUNT_PATH = '/var/run/secrets/kubernetes.io/serviceaccount'

SKIPPED_ANNOTATIONS = (
    'kubernetes.io/created-by',
)

logger = logging.getLogger(__name__)


class Discovery:

    def __init__(self, region, infrastructure_account):
        # TODO: get config path from ENV variable
        self.namespace = os.environ.get('ZMON_AGENT_KUBERNETES_NAMESPACE')
        self.cluster_id = os.environ.get('ZMON_AGENT_KUBERNETES_CLUSTER_ID')
        self.alias = os.environ.get('ZMON_AGENT_KUBERNETES_CLUSTER_ALIAS', '')

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
            'id': 'kube-cluster[{}:{}]'.format(self.infrastructure_account, self.region),
            'created_by': AGENT_TYPE,
        }

        return entity

    def get_entities(self) -> list:

        pod_entities = get_cluster_pods(
            self.kube_client, self.cluster_id, self.alias, self.region, self.infrastructure_account, namespace=self.namespace)

        # Pass pod_entities in order to get node_pod_count!
        node_entities = get_cluster_nodes(
            self.kube_client, self.cluster_id, self.alias, self.region, self.infrastructure_account, pod_entities,
            namespace=self.namespace)

        service_entities = get_cluster_services(
            self.kube_client, self.cluster_id, self.alias, self.region, self.infrastructure_account, namespace=self.namespace)
        replicaset_entities = get_cluster_replicasets(
            self.kube_client, self.cluster_id, self.alias, self.region, self.infrastructure_account, namespace=self.namespace)
        daemonset_entities = get_cluster_daemonsets(
            self.kube_client, self.cluster_id, self.alias, self.region, self.infrastructure_account, namespace=self.namespace)
        statefulset_entities = get_cluster_statefulsets(
            self.kube_client, self.cluster_id, self.alias, self.region, self.infrastructure_account, namespace=self.namespace)

        all_current_entities = (
            pod_entities + node_entities + service_entities + replicaset_entities + daemonset_entities +
            statefulset_entities
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


def get_cluster_pods(kube_client, cluster_id, alias, region, infrastructure_account, namespace=None):
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


def get_cluster_services(kube_client, cluster_id, alias, region, infrastructure_account, namespace=None):
    entities = []

    endpoints = get_all(kube_client, kube_client.get_endpoints, namespace)
    # number of endpoints per service
    endpoints_map = {e.name: len(e.obj.get('subsets', 0)) for e in endpoints}

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


def get_cluster_nodes(kube_client, cluster_id, alias, region, infrastructure_account, pod_entities=None, namespace=None):
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
            'pod_capacity': obj['status']['capacity']['pods'],
            'memory_capacity': obj['status']['capacity']['memory'],
            'pod_allocatable': obj['status']['allocatable']['pods'],
            'memory_allocatable': obj['status']['allocatable']['memory'],
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


def get_cluster_replicasets(kube_client, cluster_id, alias, region, infrastructure_account, namespace=None):
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
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'replicaset_name': replicaset.name,
            'replicaset_namespace': obj['metadata']['namespace'],

            'containers': {c['name']: c['image'] for c in containers},

            'replicas': obj['spec'].get('replicas', 0),
            'ready_replicas': obj['status'].get('readyReplicas', 0),
        }

        entity = add_labels_to_entity(entity, obj['metadata'].get('labels', {}))
        entity = add_labels_to_entity(entity, obj['metadata'].get('annotations', {}))

        entities.append(entity)

    return entities


def get_cluster_statefulsets(kube_client, cluster_id, alias, region, infrastructure_account, namespace='default'):
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
            'containers': {c['name']: c['image'] for c in containers},

            'replicas': obj['spec'].get('replicas'),
            'replicas_status': obj['status'].get('replicas'),
        }

        entity = add_labels_to_entity(entity, obj['metadata'].get('labels', {}))
        entity = add_labels_to_entity(entity, obj['metadata'].get('annotations', {}))

        entities.append(entity)

    return entities


def get_cluster_daemonsets(kube_client, cluster_id, alias, region, infrastructure_account, namespace='default'):
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
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'daemonset_name': daemonset.name,
            'daemonset_namespace': obj['metadata']['namespace'],

            'containers': {c['name']: c['image'] for c in containers},

            'desired_count': obj['status'].get('desiredNumberScheduled', 0),
            'current_count': obj['status'].get('currentNumberScheduled', 0),
        }

        entity = add_labels_to_entity(entity, obj['metadata'].get('labels', {}))
        entity = add_labels_to_entity(entity, obj['metadata'].get('annotations', {}))

        entities.append(entity)

    return entities
