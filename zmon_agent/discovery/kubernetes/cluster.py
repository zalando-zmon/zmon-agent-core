"""Discovery class used by agent core"""

# TODO: this is pilot implementation!

import itertools
import json
import os
import sys
import logging
import traceback
import pykube

import psycopg2

from opentracing.ext import tags as ot_tags

from opentracing_utils import trace, extract_span_from_kwargs, remove_span_from_kwargs

from . import kube
from . import volumes
from . import kube_resources

AGENT_TYPE = 'zmon-kubernetes-agent'

POD_TYPE = 'kube_pod'
CONTAINER_TYPE = 'kube_pod_container'
NAMESPACE_TYPE = 'kube_namespace'
SERVICE_TYPE = 'kube_service'
NODE_TYPE = 'kube_node'
DEPLOYMENT_TYPE = 'kube_deployment'
REPLICASET_TYPE = 'kube_replicaset'
STATEFULSET_TYPE = 'kube_statefulset'
DAEMONSET_TYPE = 'kube_daemonset'
INGRESS_TYPE = 'kube_ingress'
JOB_TYPE = 'kube_job'
CRONJOB_TYPE = 'kube_cronjob'
PVC_TYPE = 'kube_persistentvolumeclaim'

POSTGRESQL_CRD_TYPE = 'postgresql'
POSTGRESQL_CLUSTER_TYPE = 'postgresql_cluster'
POSTGRESQL_CLUSTER_MEMBER_TYPE = 'postgresql_cluster_member'
POSTGRESQL_DATABASE_TYPE = 'postgresql_database'
POSTGRESQL_DATABASE_REPLICA_TYPE = 'postgresql_database_replica'
POSTGRESQL_DEFAULT_PORT = 5432
POSTGRESQL_CONNECT_TIMEOUT = os.environ.get('ZMON_AGENT_POSTGRESQL_CONNECT_TIMEOUT', 2)

HPA_TYPE = 'kube_hpa'

# Custom Resources
CREDENTIALSET_TYPE = 'kube_credentialset'

INSTANCE_TYPE_LABEL = 'beta.kubernetes.io/instance-type'

PROTECTED_FIELDS = {'id', 'type', 'infrastructure_account', 'created_by', 'region', 'team'}

SERVICE_ACCOUNT_PATH = '/var/run/secrets/kubernetes.io/serviceaccount'

SKIPPED_ANNOTATIONS = {'kubernetes.io/created-by', 'autoscaling.alpha.kubernetes.io/current-metrics',
                       'autoscaling.alpha.kubernetes.io/metrics', 'autoscaling.alpha.kubernetes.io/conditions'}

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
logger.setLevel(logging.INFO)


class PostgreSQL(pykube.objects.NamespacedAPIObject):
    version = "acid.zalan.do/v1"
    endpoint = "postgresqls"
    kind = "postgresql"


class CustomResourceDefinition(pykube.objects.APIObject):
    version = "apiextensions.k8s.io/v1beta1"
    endpoint = "customresourcedefinitions"
    kind = "CustomResourceDefinition"


class PostgreSQLClient(kube.Client):
    def __init__(self, config_file_path=None, service_acc_path=kube.DEFAULT_SERVICE_ACC):
        super().__init__(config_file_path, service_acc_path)
        self.is_operator_present = False

        for crd in CustomResourceDefinition.objects(self.client):
            if str(crd) == 'postgresqls.acid.zalan.do':
                self.is_operator_present = True

    def get_postgresqls(self, namespace=kube.DEFAULT_NAMESPACE) -> pykube.query.Query:
        if self.is_operator_present:
            return PostgreSQL.objects(self.client).filter(namespace=namespace)


class Discovery:

    def __init__(self, region, infrastructure_account):
        # TODO: get config path from ENV variable
        self.namespace = os.environ.get('ZMON_AGENT_KUBERNETES_NAMESPACE')
        self.cluster_id = os.environ.get('ZMON_AGENT_KUBERNETES_CLUSTER_ID')
        self.alias = os.environ.get('ZMON_AGENT_KUBERNETES_CLUSTER_ALIAS', '')
        self.environment = os.environ.get('ZMON_AGENT_KUBERNETES_CLUSTER_ENVIRONMENT', '')
        self.hosted_zone_format_string = os.environ.get('ZMON_HOSTED_ZONE_FORMAT_STRING', '{}.{}.example.org')
        self.postgres_user = os.environ.get('ZMON_AGENT_POSTGRES_USER')
        self.postgres_pass = os.environ.get('ZMON_AGENT_POSTGRES_PASS')
        if not (self.postgres_user and self.postgres_pass):
            logger.warning('No credentials provided for PostgreSQL database discovery!')

        if not self.cluster_id:
            raise RuntimeError('Cannot determine cluster ID. Please set env variable ZMON_AGENT_KUBERNETES_CLUSTER_ID')

        self.config_path = os.environ.get('ZMON_AGENT_KUBERNETES_CONFIG_PATH')
        self.kube_client = kube.Client(config_file_path=self.config_path)
        self.pg_client = PostgreSQLClient(config_file_path=self.config_path)

        self.region = region
        self.infrastructure_account = infrastructure_account

    def get_discovery_tags(self) -> dict:
        return {'cluster_id': self.cluster_id, 'alias': self.alias, 'environment': self.environment}

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

    @trace(tags={'discovery': 'kubernetes'}, pass_span=True)
    def get_entities(self, **kwargs) -> list:

        current_span = extract_span_from_kwargs(**kwargs)

        self.kube_client.invalidate_namespace_cache()

        pod_container_entities = get_cluster_pods_and_containers(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            namespace=self.namespace)

        # Pass pod_entities in order to get node_pod_count!
        node_entities = get_cluster_nodes(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            pod_container_entities, namespace=self.namespace)

        namespace_entities = get_cluster_namespaces(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            namespace=self.namespace)

        service_entities = get_cluster_services(
            self.kube_client,
            self.cluster_id,
            self.alias,
            self.environment,
            self.region,
            self.infrastructure_account,
            self.hosted_zone_format_string,
            namespace=self.namespace)

        deployment_entities = get_cluster_deployments(
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

        persistentvolumeclaim_entities = get_cluster_persistentvolumeclaims(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            namespace=self.namespace)

        job_entities = get_cluster_jobs(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            namespace=self.namespace)

        cronjob_entities = get_cluster_cronjobs(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            namespace=self.namespace)

        hpa_entities = get_cluster_hpas(
            self.kube_client, self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
            namespace=self.namespace
        )

        pcs_entities = get_cluster_credential_sets(
            self.kube_client, self.cluster_id, self.alias, self.environment,
            self.region, self.infrastructure_account, namespace=self.namespace
        )

        postgresql_entities = []
        postgresql_cluster_entities = []
        postgresql_cluster_member_entities = []
        postgresql_database_entities = []

        try:
            if is_postgresql_operator_present(self.config_path):
                postgresql_entities = get_postgresqls(
                    self.pg_client, self.cluster_id, self.alias, self.environment, self.region,
                    self.infrastructure_account, namespace=self.namespace)

                postgresql_cluster_entities = get_postgresql_clusters(
                    self.kube_client, self.cluster_id, self.alias, self.environment, self.region,
                    self.infrastructure_account, self.hosted_zone_format_string, postgresql_entities,
                    statefulset_entities, namespace=self.namespace)

                postgresql_cluster_member_entities = get_postgresql_cluster_members(
                    self.kube_client, self.cluster_id, self.alias, self.environment, self.region,
                    self.infrastructure_account, self.hosted_zone_format_string, namespace=self.namespace)

                postgresql_database_entities = get_postgresql_databases(
                    self.cluster_id, self.alias, self.environment, self.region, self.infrastructure_account,
                    self.postgres_user, self.postgres_pass, postgresql_cluster_entities)
        except Exception:
            current_span.set_tag('postgres_failed', True)
            current_span.log_kv({'exception': traceback.format_exc()})
            logger.exception('Failed postgresql discovery!')

        return list(itertools.chain(
            pod_container_entities, node_entities, namespace_entities, service_entities,
            deployment_entities, replicaset_entities, daemonset_entities, statefulset_entities,
            ingress_entities, job_entities, cronjob_entities, persistentvolumeclaim_entities,
            postgresql_cluster_entities, postgresql_cluster_member_entities,
            postgresql_database_entities, postgresql_entities, hpa_entities, pcs_entities))


@trace()
def get_all(kube_client, kube_func, namespace=None, **kwargs) -> list:
    items = []
    namespaces = [namespace] if namespace else [ns.name for ns in kube_client.get_namespaces()]

    for ns in namespaces:
        items += list(kube_func(namespace=ns))

    return items


def parse_resources(resources):
    result = {}
    for resource_group in ('requests', 'limits'):
        raw_resources = resources.get(resource_group)
        if raw_resources:
            converted = {name: kube_resources.parse_resource(value) for name, value in raw_resources.items()}
            result[resource_group] = converted
    return result


def entity_labels(obj: dict, *sources: str) -> dict:
    result = {}

    for key in sources:
        for label, val in obj['metadata'].get(key, {}).items():
            if label in PROTECTED_FIELDS:
                logger.warning('Skipping label [{}:{}] as it is in Protected entity fields {}'.format(
                    label, val, PROTECTED_FIELDS))
            elif label in SKIPPED_ANNOTATIONS:
                pass
            else:
                result[label] = val

    return result


@trace(tags={'kubernetes': 'pod'}, pass_span=True)
def get_cluster_pods_and_containers(
        kube_client, cluster_id, alias, environment, region, infrastructure_account, namespace=None, **kwargs) -> list:
    """
    Return all Pods as ZMON entities.
    """
    entities = []
    current_span = extract_span_from_kwargs(**kwargs)

    pods = get_all(kube_client, kube_client.get_pods, namespace, span=current_span)

    for pod in pods:
        if not pod.ready:
            continue

        obj = pod.obj

        containers = obj['spec'].get('containers', [])
        container_statuses = {c['name']: c for c in obj['status']['containerStatuses']}
        conditions = {c['type']: c['status'] for c in obj['status']['conditions']}

        pod_labels = entity_labels(obj, 'labels')
        pod_annotations = entity_labels(obj, 'annotations')

        # Properties shared between pod entity and container entity
        shared_properties = {
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

            'pod_phase': obj['status'].get('phase'),
            'pod_initialized': conditions.get('Initialized', False),
            'pod_ready': conditions.get('Ready', True),
            'pod_scheduled': conditions.get('PodScheduled', False),
            'pod_start': obj['status'].get('startTime')
        }

        pod_entity = {
            'id': 'pod-{}-{}[{}]'.format(pod.name, pod.namespace, cluster_id),
            'type': POD_TYPE,
            'containers': {}
        }

        pod_entity.update(shared_properties)
        pod_entity.update(pod_labels)
        pod_entity.update(pod_annotations)

        for container in containers:
            container_name = container['name']
            container_image = container['image']
            container_ready = container_statuses.get(container['name'], {}).get('ready', False)
            container_restarts = container_statuses.get(container['name'], {}).get('restartCount', 0)
            container_ports = [p['containerPort'] for p in container.get('ports', []) if 'containerPort' in p]

            try:
                container_resources = parse_resources(container.get('resources', {}))
            except Exception:
                container_resources = None

            container_entity = {
                'id': 'container-{}-{}-{}[{}]'.format(pod.name, pod.namespace, container_name, cluster_id),
                'type': CONTAINER_TYPE,
                'container_name': container_name,
                'container_image': container_image,
                'container_ready': container_ready,
                'container_restarts': container_restarts,
                'container_ports': container_ports,
                'resources': container_resources
            }
            pod_entity['containers'][container_name] = {
                'image': container_image,
                'ready': container_ready,
                'restarts': container_restarts,
                'ports': container_ports,
                'resources': container_resources
            }

            container_entity.update(pod_labels)
            container_entity.update(shared_properties)
            entities.append(container_entity)

        entities.append(pod_entity)

    return entities


@trace(tags={'kubernetes': 'service'}, pass_span=True)
def get_cluster_services(
        kube_client, cluster_id, alias, environment, region,
        infrastructure_account, hosted_zone, namespace=None, **kwargs) -> list:
    entities = []

    current_span = extract_span_from_kwargs(**kwargs)

    endpoints = get_all(kube_client, kube_client.get_endpoints, namespace, span=current_span)
    # number of endpoints per service
    endpoints_map = {e.name: len(e.obj['subsets']) for e in endpoints if e.obj.get('subsets')}

    services = get_all(kube_client, kube_client.get_services, namespace, span=current_span)

    for service in services:
        obj = service.obj

        host = obj['spec'].get('clusterIP', None)
        service_type = obj['spec']['type']
        if service_type == 'LoadBalancer':
            ingress = obj['status'].get('loadBalancer', {}).get('ingress', [])
            hostname = ingress[0].get('hostname') if ingress else None
            if hostname:
                host = hostname
        elif service_type == 'ExternalName':
            host = obj['spec']['externalName']

        service_namespace = obj['metadata']['namespace']
        labels = obj['metadata'].get('labels', {})
        version = labels.get('version', '')

        entity = {
            'id': 'service-{}-{}[{}]'.format(service.name, service.namespace, cluster_id),
            'type': SERVICE_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'ip': obj['spec'].get('clusterIP', None),
            'host': host,
            'port': next(iter(obj['spec'].get('ports', [])), None),  # Assume first port is the used one.

            'service_name': service.name,
            'service_namespace': service_namespace,
            'service_type': service_type,
            'service_ports': obj['spec'].get('ports', None),  # Could be useful when multiple ports are exposed.

            'endpoints_count': endpoints_map.get(service.name, 0)
        }

        entity.update(entity_labels(obj, 'labels', 'annotations'))

        if labels.get('application') == 'spilo':
            # postgres related part
            entity.update({
                'deeplink1': '{}/#/clusters/{}/{}'.format(
                    hosted_zone.format('pgview', alias),
                    service_namespace,
                    version)
            })

        entities.append(entity)

    return entities


@trace(tags={'kubernetes': 'node'}, pass_span=True)
def get_cluster_nodes(
        kube_client, cluster_id, alias, environment, region, infrastructure_account, pod_entities=None, namespace=None,
        **kwargs) -> list:
    current_span = extract_span_from_kwargs(**kwargs)  # noqa

    entities = []

    nodes = kube_client.get_nodes()

    if not pod_entities:
        logger.warning('No pods supplied, Nodes will not show pod count!')

    node_pod_count = {}
    for pod in pod_entities:
        if pod['type'] == POD_TYPE:
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

        entity.update(entity_labels(obj, 'labels', 'annotations'))

        entities.append(entity)

    return entities


@trace(tags={'kubernetes': 'namespace'}, pass_span=True)
def get_cluster_namespaces(
        kube_client, cluster_id, alias, environment, region, infrastructure_account, namespace=None, **kwargs) -> list:
    current_span = extract_span_from_kwargs(**kwargs)  # noqa

    entities = []

    for ns in kube_client.get_namespaces():
        obj = ns.obj
        if namespace and namespace != ns.name:
            continue

        entity = {
            'id': 'namespace-{}[{}]'.format(ns.name, cluster_id),
            'type': NAMESPACE_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'namespace_name': ns.name,
        }

        entity.update(entity_labels(obj, 'labels', 'annotations'))

        entities.append(entity)

    return entities


@trace(tags={'kubernetes': 'deployment'}, pass_span=True)
def get_cluster_deployments(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                            namespace=None, **kwargs) -> list:
    current_span = extract_span_from_kwargs(**kwargs)

    entities = []

    deployments = get_all(kube_client, kube_client.get_deployments, namespace, span=current_span)

    for deployment in deployments:
        obj = deployment.obj

        containers = obj['spec']['template']['spec']['containers']

        entity = {
            'id': 'deployment-{}-{}[{}]'.format(deployment.name, deployment.namespace, cluster_id),
            'type': DEPLOYMENT_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'deployment_name': deployment.name,
            'deployment_namespace': obj['metadata']['namespace'],

            'containers': {c['name']: c.get('image', '') for c in containers if 'name' in c},

            'replicas': obj['spec'].get('replicas', 0),
            'ready_replicas': obj['status'].get('readyReplicas', 0),
            'updated_replicas': obj['status'].get('updatedReplicas', 0),
        }

        entity.update(entity_labels(obj, 'labels', 'annotations'))

        entities.append(entity)

    return entities


@trace(tags={'kubernetes': 'replicaset'}, pass_span=True)
def get_cluster_replicasets(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                            namespace=None, **kwargs) -> list:
    current_span = extract_span_from_kwargs(**kwargs)

    entities = []

    replicasets = get_all(kube_client, kube_client.get_replicasets, namespace, span=current_span)

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

        entity.update(entity_labels(obj, 'labels', 'annotations'))

        entities.append(entity)

    return entities


@trace(tags={'kubernetes': 'statefulset'}, pass_span=True)
def get_cluster_statefulsets(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                             namespace='default', **kwargs) -> list:
    current_span = extract_span_from_kwargs(**kwargs)

    entities = []

    statefulsets = get_all(kube_client, kube_client.get_statefulsets, namespace, span=current_span)

    for statefulset in statefulsets:
        obj = statefulset.obj

        # Stale replic set?!
        if obj['spec'].get('replicas', 0) == 0:
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
            'actual_replicas': obj['status'].get('readyReplicas'),
            'version': obj['metadata'].get('labels', {}).get('version', '')
        }

        entity.update(entity_labels(obj, 'labels', 'annotations'))

        entities.append(entity)

    return entities


@trace(tags={'kubernetes': 'daemonset'}, pass_span=True)
def get_cluster_daemonsets(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                           namespace='default', **kwargs) -> list:
    current_span = extract_span_from_kwargs(**kwargs)

    entities = []

    daemonsets = get_all(kube_client, kube_client.get_daemonsets, namespace, span=current_span)

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

        entity.update(entity_labels(obj, 'labels', 'annotations'))

        entities.append(entity)

    return entities


@trace(tags={'kubernetes': 'ingress'}, pass_span=True)
def get_cluster_ingresses(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                          namespace='default', **kwargs) -> list:
    current_span = extract_span_from_kwargs(**kwargs)

    entities = []

    ingresses = get_all(kube_client, kube_client.get_ingresses, namespace, span=current_span)

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

        entity.update(entity_labels(obj, 'labels'))

        entities.append(entity)

    return entities


@trace(tags={'kubernetes': 'persistentvolumeclaim'}, pass_span=True)
def get_cluster_persistentvolumeclaims(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                                       namespace='default', **kwargs) -> list:
    current_span = extract_span_from_kwargs(**kwargs)

    entities = []

    pvcs = get_all(kube_client, kube_client.get_persistentvolumeclaims, namespace, span=current_span)
    pvs = get_all(kube_client, kube_client.get_persistentvolumes, namespace, span=current_span)

    for pvc in pvcs:
        obj = pvc.obj

        try:
            pv = [p for p in pvs if p.name == obj['spec'].get('volumeName')][0]
        except Exception:
            current_span.log_kv({'message': 'Cannot find volume for PVC: '.format(pvc.name)})
            continue

        entity = {
            'id': 'pvc-{}-{}[{}]'.format(pvc.name, pvc.namespace, cluster_id),
            'type': PVC_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'persistentvolumeclaim_name': pvc.name,
            'persistentvolumeclaim_namespace': obj['metadata']['namespace'],

            'phase': obj['status'].get('phase'),
            'access_modes': obj['status'].get('accessModes'),
        }

        pv_fields = volumes.get_persistentvolume_fields(pv)
        entity.update(pv_fields)

        entity.update(entity_labels(obj, 'labels', 'annotations'))

        entities.append(entity)

    return entities


@trace(tags={'kubernetes': 'job'}, pass_span=True)
def get_cluster_jobs(kube_client, cluster_id, alias, environment, region, infrastructure_account, namespace='default',
                     **kwargs) -> list:
    current_span = extract_span_from_kwargs(**kwargs)

    entities = []

    jobs = get_all(kube_client, kube_client.get_jobs, namespace, span=current_span)

    for job in jobs:
        obj = job.obj

        entity = {
            'id': 'job-{}-{}[{}]'.format(job.name, job.namespace, cluster_id),
            'type': JOB_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'job_name': job.name,
            'job_namespace': job.namespace,

            'parallelism': obj['spec'].get('parallelism'),
            'completions': obj['spec'].get('completions'),
            'backoffLimit': obj['spec'].get('backoffLimit'),

            'failed': obj['status'].get('failed', 0),
        }

        entity.update(entity_labels(obj, 'labels'))

        entities.append(entity)

    return entities


@trace(tags={'kubernetes': 'cronjob'}, pass_span=True)
def get_cluster_cronjobs(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                         namespace='default', **kwargs) -> list:
    current_span = extract_span_from_kwargs(**kwargs)

    entities = []

    cronjobs = get_all(kube_client, kube_client.get_cronjobs, namespace, span=current_span)

    for cronjob in cronjobs:
        obj = cronjob.obj

        entity = {
            'id': 'cronjob-{}-{}[{}]'.format(cronjob.name, cronjob.namespace, cluster_id),
            'type': CRONJOB_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'cronjob_name': cronjob.name,
            'cronjob_namespace': cronjob.namespace,

            'concurrencyPolicy': obj['spec'].get('concurrencyPolicy'),
            'schedule': obj['spec'].get('schedule'),
            'successfulJobsHistoryLimit': obj['spec'].get('successfulJobsHistoryLimit'),
            'suspend': obj['spec'].get('suspend'),

            'active_jobs': [j.get('name') for j in obj['status'].get('active', [])]
        }

        entity.update(entity_labels(obj, 'labels', 'annotations'))

        entities.append(entity)

    return entities


########################################################################################################################
# POSTGRESQL   | TODO: move to separate discovery                                                                      #
########################################################################################################################
@trace(tags={'kubernetes': 'postgres'}, pass_span=True)
def list_postgres_databases(*args, **kwargs) -> list:
    current_span = extract_span_from_kwargs(**kwargs)
    kwargs = remove_span_from_kwargs(**kwargs)

    try:
        query = """
            SELECT datname
              FROM pg_database
             WHERE datname NOT IN('postgres', 'template0', 'template1')
        """

        current_span.set_tag(ot_tags.PEER_ADDRESS,
                             'psql://{}:{}'.format(kwargs.get('host'), kwargs.get('port')))
        current_span.set_tag(ot_tags.DATABASE_INSTANCE, kwargs.get('dbname'))
        current_span.set_tag(ot_tags.DATABASE_STATEMENT, query)

        kwargs.update({'connect_timeout': POSTGRESQL_CONNECT_TIMEOUT})

        conn = psycopg2.connect(*args, **kwargs)

        cur = conn.cursor()
        cur.execute(query)
        return [row[0] for row in cur.fetchall()]
    except Exception:
        current_span.set_tag('error', True)
        current_span.log_kv({'exception': traceback.format_exc()})
        logger.exception('Failed to list DBs on %s', kwargs.get('host', '{no host specified}'))
        return []


def is_postgresql_operator_present(config_path=None) -> bool:
    pg = PostgreSQLClient(config_path)
    return pg.is_operator_present


@trace(tags={'kubernetes': 'postgres'}, pass_span=True)
def get_postgresqls(pg_client, cluster_id, alias, environment, region, infrastructure_account,
                    namespace=None, **kwargs) -> list:
    entities = []
    postgresqls = get_all(pg_client, pg_client.get_postgresqls, namespace)

    for postgres in postgresqls:
        pg = postgres.obj
        metadata = pg.get('metadata', {})

        entity = {
            'name': metadata.get('name', ''),
            'id': 'postgresql-{}-{}[{}]'.format(metadata.get('name', ''), metadata.get('namespace', ''), cluster_id),
            'type': POSTGRESQL_CRD_TYPE,
            'team': metadata.get('labels', {}).get('team', ''),
            'uid': metadata.get('uid'),
            'expected_instance_count': pg.get('spec', {}).get('numberOfInstances'),
            'namespace': metadata.get('namespace', '')
        }

        entities.append(entity)

    return entities


@trace(tags={'kubernetes': 'postgres'}, pass_span=True)
def get_postgresql_clusters(kube_client, cluster_id, alias, environment, region, infrastructure_account, hosted_zone,
                            postgresqls, statefulsets, namespace=None, **kwargs) -> list:
    entities = []
    current_span = extract_span_from_kwargs(**kwargs)

    ssets = [ss for ss in statefulsets if 'version' in ss]
    pgs = list(postgresqls)

    services = get_all(kube_client, kube_client.get_services, namespace, span=current_span)

    for service in services:
        obj = service.obj

        labels = obj['metadata'].get('labels', {})
        version = labels.get('version', '')

        # we skip non-Spilos and replica services
        if labels.get('application') != 'spilo' or labels.get('spilo-role') == 'replica':
            continue

        service_namespace = obj['metadata']['namespace']
        service_dns_name = '{}.{}.svc.cluster.local'.format(service.name, service_namespace)

        statefulset_error = ''
        ss = {}
        statefulset = [ss for ss in ssets if ss.get('version') == version]

        if not statefulset:  # can happen when the replica count is 0.In this case we don't have a running cluster.
            statefulset_error = 'There is no statefulset attached'
        else:
            ss = statefulset[0]

        postgresql = [pg for pg in pgs if pg['name'] == version]

        pg = {}
        if postgresql:
            pg = postgresql[0]

        entity = {
            'id': 'pg-{}[{}]'.format(service.name, cluster_id),
            'type': POSTGRESQL_CLUSTER_TYPE,
            'kube_cluster': cluster_id,
            'account_alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,
            'spilo_cluster': version,
            'application': "spilo",
            'version': version,
            'dnsname': service_dns_name,
            'shards': {
                'postgres': '{}:{}/postgres'.format(service_dns_name, POSTGRESQL_DEFAULT_PORT)
            },
            'expected_replica_count': ss.get('replicas', 0),
            'current_replica_count': ss.get('actual_replicas', 0),
            'statefulset_error': statefulset_error,
            'deeplink1': '{}/#/status/{}/{}'.format(hosted_zone.format('pgui', alias), service_namespace, version),
            'icon1': 'fa-server',
            'deeplink2': '{}/#/clusters/{}/{}'.format(hosted_zone.format('pgview', alias), service_namespace, version),
            'icon2': 'fa-line-chart',
            'uid': pg.get('uid', ''),
            'namespace': service_namespace
        }

        entities.append(entity)

    return entities


@trace(tags={'kubernetes': 'postgres'}, pass_span=True)
def get_postgresql_cluster_members(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                                   hosted_zone, namespace=None, **kwargs) -> list:
    entities = []
    current_span = extract_span_from_kwargs(**kwargs)

    pods = get_all(kube_client, kube_client.get_pods, namespace, span=current_span)
    pvcs = get_all(kube_client, kube_client.get_persistentvolumeclaims, namespace, span=current_span)
    pvs = get_all(kube_client, kube_client.get_persistentvolumes, span=current_span)

    for pod in pods:
        obj = pod.obj

        # TODO: filter in the API call
        labels = obj['metadata'].get('labels', {})
        if labels.get('application') != 'spilo' or labels.get('version') is None:
            continue

        pod_number = pod.name.split('-')[-1]
        pod_namespace = obj['metadata']['namespace']
        service_dns_name = '{}.{}.svc.cluster.local'.format(labels['version'], pod_namespace)

        container = obj['spec']['containers'][0]  # we don't assume more than one container
        cluster_name = [env['value'] for env in container.get('env', []) if env['name'] == 'SCOPE'][0]

        ebs_volume_id = ''
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
        except KeyError:
            pass

        entity = {
            'id': 'pg-{}-{}[{}]'.format(service_dns_name, pod_number, cluster_id),
            'type': POSTGRESQL_CLUSTER_MEMBER_TYPE,
            'kube_cluster': cluster_id,
            'account_alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,
            'cluster_dns_name': service_dns_name,
            'pod': pod.name,
            'pod_phase': obj.get('status', {}).get('phase', ''),
            'image': container['image'],
            'container_name': container['name'],
            'ip': obj.get('status', {}).get('podIP', ''),
            'spilo_cluster': cluster_name,
            'spilo_role': labels.get('spilo-role', ''),
            'application': 'spilo',
            'version': cluster_name,
            'volume': ebs_volume_id,
            'deeplink1': '{}/#/status/{}/{}'.format(hosted_zone.format('pgui', alias), pod_namespace, cluster_name),
            'icon1': 'fa-server',
            'deeplink2': '{}/#/clusters/{}/{}/{}'.format(hosted_zone.format('pgview', alias), pod_namespace,
                                                         cluster_name, pod.name),
            'icon2': 'fa-line-chart'
        }

        entities.append(entity)

    return entities


@trace(tags={'kubernetes': 'postgres'})
def get_postgresql_databases(cluster_id, alias, environment, region, infrastructure_account,
                             postgres_user, postgres_pass, postgresql_clusters) -> list:
    entities = []
    if not (postgres_user and postgres_pass):
        return entities

    for pgcluster in postgresql_clusters:
        dbnames = list_postgres_databases(host=pgcluster['dnsname'],
                                          port=POSTGRESQL_DEFAULT_PORT,
                                          user=postgres_user,
                                          password=postgres_pass,
                                          dbname='postgres',
                                          sslmode='require')
        for db in dbnames:
            entity = {
                'id': '{}-{}'.format(db, pgcluster.get('id')),
                'type': POSTGRESQL_DATABASE_TYPE,
                'kube_cluster': cluster_id,
                'alias': alias,
                'environment': environment,
                'created_by': AGENT_TYPE,
                'infrastructure_account': infrastructure_account,
                'region': region,
                'version': pgcluster.get('version'),
                'postgresql_cluster': pgcluster.get('id'),
                'database_name': db,
                'shards': {
                    db: '{}:{}/{}'.format(pgcluster['dnsname'], POSTGRESQL_DEFAULT_PORT, db)
                },
                'role': 'master'
            }

            entities.append(entity)

            if pgcluster.get('expected_replica_count', 0) > 1:  # the first k8s replica is the master itself
                name_parts = pgcluster.get('dnsname').split('.')
                repl_dnsname = '.'.join([name_parts[0] + '-repl'] + name_parts[1:])
                replicatype_entity = {
                    'id': '{}-repl-{}'.format(db, pgcluster.get('id')),
                    'type': POSTGRESQL_DATABASE_REPLICA_TYPE,
                    'kube_cluster': cluster_id,
                    'alias': alias,
                    'environment': environment,
                    'created_by': AGENT_TYPE,
                    'infrastructure_account': infrastructure_account,
                    'region': region,
                    'version': pgcluster.get('version'),
                    'postgresql_cluster': pgcluster.get('id'),
                    'database_name': db,
                    'shards': {
                        db: '{}:{}/{}'.format(repl_dnsname, POSTGRESQL_DEFAULT_PORT, db)
                    },
                    'role': 'replica'
                }

                entities.append(replicatype_entity)

    return entities


@trace(tags={'kubernetes': 'hpa'}, pass_span=True)
def get_cluster_hpas(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                     namespace=None, **kwargs) -> list:
    current_span = extract_span_from_kwargs(**kwargs)
    entities = []

    hpas = get_all(kube_client, kube_client.get_hpas, namespace, span=current_span)

    for h in hpas:
        obj = h.obj

        conditions_annotation = obj['metadata']['annotations']['autoscaling.alpha.kubernetes.io/conditions']
        conditions_list = json.loads(conditions_annotation)
        conditions = {c['type']: c['status'] == 'True' for c in conditions_list}

        entity = {
            'id': 'hpa-{}-{}[{}]'.format(h.name, h.namespace, cluster_id),
            'type': HPA_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,

            'hpa_name': h.name,
            'hpa_namespace': obj['metadata']['namespace'],

            'hpa_desired_replicas': obj['status']['desiredReplicas'],
            'hpa_current_replicas': obj['status']['currentReplicas'],
            'hpa_min_replicas': obj['spec']['minReplicas'],
            'hpa_max_replicas': obj['spec']['maxReplicas'],
            'able_to_scale': conditions.get('AbleToScale', False),
            'scaling_active': conditions.get('ScalingActive', False),
            'scaling_limited': conditions.get('ScalingLimited', False),
        }

        entity.update(entity_labels(obj, 'labels', 'annotations'))

        entities.append(entity)

    return entities


@trace(tags={'kubernetes': 'credentialset'}, pass_span=True)
def get_cluster_credential_sets(kube_client, cluster_id, alias, environment, region, infrastructure_account,
                                namespace=None, **kwargs) -> list:
    current_span = extract_span_from_kwargs(**kwargs)
    entities = []

    credential_sets = get_all(kube_client, kube_client.get_platformcredentialsets, namespace, span=current_span)
    for cs in credential_sets:
        obj = cs.obj
        entity = {
            'id': 'pcs-{}-{}[{}]'.format(cs.name, cs.namespace, cluster_id),
            'type': CREDENTIALSET_TYPE,
            'kube_cluster': cluster_id,
            'alias': alias,
            'environment': environment,
            'created_by': AGENT_TYPE,
            'infrastructure_account': infrastructure_account,
            'region': region,
            'pcs_name': cs.name,
            'pcs_namespace': obj['metadata']['namespace'],
            'errors': obj['status']['errors'],
            'problems': obj['status']['problems'],
            'tokens': obj['status'].get('tokens', {})
        }
        entity.update(entity_labels(obj, 'labels', 'annotations'))
        entities.append(entity)
    return entities
