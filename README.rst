ZMON source code on GitHub is no longer in active development. Zalando will no longer actively review issues or merge pull-requests.

ZMON is still being used at Zalando and serves us well for many purposes. We are now deeper into our observability journey and understand better that we need other telemetry sources and tools to elevate our understanding of the systems we operate. We support the `OpenTelemetry <https://opentelemetry.io>`_ initiative and recommended others starting their journey to begin there.

If members of the community are interested in continuing developing ZMON, consider forking it. Please review the licence before you do.

===============
ZMON AGENT CORE
===============

.. image:: https://travis-ci.org/zalando-zmon/zmon-agent-core.svg?branch=master
    :target: https://travis-ci.org/zalando-zmon/zmon-agent-core
    :alt: Build status

.. image:: https://img.shields.io/badge/OpenTracing-enabled-blue.svg
    :target: http://opentracing.io
    :alt: OpenTracing enabled


**WIP**

ZMON agent core for infrastructure discovery.

Supports:

- Kubernetes discovery
- Kubernetes Spilo Postgresql clusters/databases
