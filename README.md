# alertmanager2kafka

[![license](https://img.shields.io/github/license/webdevops/alertmanager2kafka.svg)](https://github.com/fpytloun/alertmanager2kafka/blob/master/LICENSE)
[![DockerHub](https://img.shields.io/badge/DockerHub-webdevops%2Falertmanager2kafka-blue)](https://hub.docker.com/r/fpytloun/alertmanager2kafka/)

This is a forked version of [webdevops's
alertmanager2es](https://github.com/webdevops/alertmanager2es) modified to
work with Kafka instead of Elasticsearch.

alertmanager2kafka receives [HTTP webhook][] notifications from [AlertManager][]
and inserts them into an [Kafka][] index for searching and analysis. It
runs as a daemon.

The alerts are stored in Kafka as [alert groups][].

[alert groups]: https://prometheus.io/docs/alerting/alertmanager/#grouping
[AlertManager]: https://github.com/prometheus/alertmanager
[Kafka]: https://kafka.apache.org/
[HTTP webhook]: https://prometheus.io/docs/alerting/configuration/#webhook-receiver-<webhook_config>

## Usage

```
Usage:
  alertmanager2kafka [OPTIONS]

Application Options:
      --debug                   debug mode [$DEBUG]
  -v, --verbose                 verbose mode [$VERBOSE]
      --log.json                Switch log output to json format [$LOG_JSON]
      --kafka.host=             Kafka host, eg. kafka-0:9092 [$KAFKA_HOST]
      --kafka.topic=            Kafka topic, eg. alertmanager [$KAFKA_TOPIC]
      --kafka.ssl.cert=         Kafka client SSL certificate file [$KAFKA_SSL_CERT]
      --kafka.ssl.key=          Kafka client SSL key file [$KAFKA_SSL_KEY]
      --kafka.ssl.cacert=       Kafka server CA certificate file [$KAFKA_SSL_CACERT]
      --kafka.sasl.mechanism=   Kafka server authentication [$KAFKA_SASL_MECHANISM]
      --kafka.scram.username=   Kafka server username when sasl mechanism is scram [$KAFKA_SCRAM_USERNAME]
      --kafka.scram.password=   Kafka server user password when sasl mechanism is scram [$KAFKA_SCRAM_PASSWORD]
      --bind=                   Server address (default: :9097) [$SERVER_BIND]

Help Options:
  -h, --help                    Show this help message

```


## Rationale

It can be useful to see which alerts fired over a given time period, and
perform historical analysis of when and where alerts fired. Having this data
can help:

- tune alerting rules
- understand the impact of an incident
- understand which alerts fired during an incident

You can configure Kafkaconnect or some other Kafka consumer that will process
events from Kafka and store them eg. in Elasticsearch.

## Limitations

- alertmanager2kafka will not capture [silenced][] or [inhibited][] alerts; the alert
  notifications stored in Elasticsearch will closely resemble the notifications
  received by a human.

[silenced]: https://prometheus.io/docs/alerting/alertmanager/#silences
[inhibited]: https://prometheus.io/docs/alerting/alertmanager/#inhibition

## Prerequisites

To use alertmanager2kafka, you'll need:

- an [Kafka][] cluster

To build alertmanager2kafka, you'll need:

- [Make][]
- [Go][] 1.14 or above
- a working [GOPATH][]

[Make]: https://www.gnu.org/software/make/
[Go]: https://golang.org/dl/
[GOPATH]: https://golang.org/cmd/go/#hdr-GOPATH_environment_variable

## Building

    git clone github.com/fpytloun/alertmanager2kafka
    cd alertmanager2kafka
    make vendor
    make build

## Configuration

### alertmanager2kafka usage

alertmanager2kafka is configured using commandline flags. It is assumed that
alertmanager2kafka has unrestricted access to your Elasticsearch cluster.

alertmanager2kafka does not perform any user authentication.

Run `./alertmanager2kafka -help` to view the configurable commandline flags.

### Example Alertmanager configuration

#### Receiver configuration

```yaml
- name: alertmanager2kafka
  webhook_configs:
    - url: https://alertmanager2kafka.example.com/webhook
```

#### Route configuration

By omitting a matcher, this route will match all alerts:

```yaml
- receiver: alertmanager2kafka
  continue: true
```

## Metrics

alertmanager2kafka exposes [Prometheus][] metrics on `/metrics`.

[Prometheus]: https://prometheus.io/

## Example Elasticsearch queries

    alerts.labels.alertname:"Disk_Likely_To_Fill_Next_4_Days"

## Contributions

Pull requests, comments and suggestions are welcome.

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for more information.
