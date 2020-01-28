from pykube.objects import NamespacedAPIObject


class PlatformCredentialSet(NamespacedAPIObject):
    version = 'zalando.org/v1'
    endpoint = 'platformcredentialssets'
    kind = 'PlatformCredentialSet'


class AWSIAMRole(NamespacedAPIObject):
    version = 'zalando.org/v1'
    endpoint = 'awsiamroles'
    kind = 'AWSIAMRole'


class Stack(NamespacedAPIObject):
    version = 'zalando.org/v1'
    endpoint = 'stacks'
    kind = 'Stack'


class StackSet(NamespacedAPIObject):
    version = 'zalando.org/v1'
    endpoint = 'stacksets'
    kind = 'StackSet'
