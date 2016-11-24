from . import cluster


def get_discovery_agent_klass():
    return cluster.Discovery


def get_discovery_agent_name():
    return cluster.AGENT_TYPE


__all__ = (
    cluster.Discovery,
    get_discovery_agent_klass,
    get_discovery_agent_name,
)
