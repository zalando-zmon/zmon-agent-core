"""
Helper functions for Kubernetes Volume Plugins (PVC & PV)

https://kubernetes.io/docs/concepts/storage/persistent-volumes/#types-of-persistent-volumes
"""

from pykube.objects import PersistentVolume


def get_persistentvolume_fields(pv: PersistentVolume) -> dict:
    """
    Extract relevant Persistent Volume fields to be added in an entity.
    """
    fields = {}

    plugins = set(pv.obj['spec'].keys()) & set(SUPPORTED_VOLUMES_PLUGINS.keys())

    # Is it possible to have multiple plugins for a single pvc?
    for plugin in plugins:
        fields.update(SUPPORTED_VOLUMES_PLUGINS[plugin](pv))

    fields['storage_size'] = pv.obj['spec'].get('capacity', {}).get('storage')

    return fields


def get_aws_ebs_fields(pv: PersistentVolume) -> dict:
    """Return volume fields for AWSElasticBlockStore plugin"""

    spec = pv.obj['spec']['awsElasticBlockStore']

    return {
        'volume_type': 'ebs',
        'volume_id': spec.get('volumeID', '').split('/')[-1],
        'fs_type': spec.get('fsType'),
    }


SUPPORTED_VOLUMES_PLUGINS = {
    'awsElasticBlockStore': get_aws_ebs_fields,
}
