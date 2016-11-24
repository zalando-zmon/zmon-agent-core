"""
Wrapper client for Kubernetes API using ``pykube``
"""
import pykube


DEFAULT_SERVICE_ACC = '/var/run/secrets/kubernetes.io/serviceaccount'

DEFAULT_NAMESPACE = 'default'


class Client:

    def __init__(self, config_file_path=None, service_acc_path=DEFAULT_SERVICE_ACC):
        self.config_file_path = config_file_path
        self.service_acc_path = service_acc_path

        self.__pykube = None

        self.__namespaces = None

    @property
    def client(self):
        if not self.__pykube:
            if self.config_file_path:
                config = pykube.KubeConfig.from_file(self.config_file_path)
            else:
                config = pykube.KubeConfig.from_service_account(path=self.service_acc_path)

            self.__pykube = pykube.HTTPClient(config)

        return self.__pykube

    def get_namespaces(self) -> list:
        if not self.__namespaces:
            self.__namespaces = list(pykube.Namespace.objects(self.client).filter())
        return self.__namespaces

    def get_nodes(self) -> pykube.query.Query:
        return pykube.Node.objects(self.client).filter()

    def get_pods(self, namespace=DEFAULT_NAMESPACE) -> pykube.query.Query:
        return pykube.Pod.objects(self.client).filter(namespace=namespace)

    def get_petsets(self, namespace=DEFAULT_NAMESPACE) -> pykube.query.Query:
        return pykube.PetSet.objects(self.client).filter(namespace=namespace)

    def get_daemonsets(self, namespace=DEFAULT_NAMESPACE) -> pykube.query.Query:
        return pykube.DaemonSet.objects(self.client).filter(namespace=namespace)

    def get_replicasets(self, namespace=DEFAULT_NAMESPACE) -> pykube.query.Query:
        return pykube.ReplicaSet.objects(self.client).filter(namespace=namespace)

    def get_services(self, namespace=DEFAULT_NAMESPACE) -> pykube.query.Query:
        return pykube.Service.objects(self.client).filter(namespace=namespace)

    def get_endpoints(self, namespace=DEFAULT_NAMESPACE) -> pykube.query.Query:
        return pykube.Endpoint.objects(self.client).filter(namespace=namespace)
