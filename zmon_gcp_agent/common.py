from zmon_gcp_agent import __version__


def get_user_agent():
    return 'zmon-gcp-agent/{}'.format(__version__)
