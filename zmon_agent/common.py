from zmon_agent import __version__


def get_user_agent():
    return 'zmon-k8s-agent/{}'.format(__version__)
