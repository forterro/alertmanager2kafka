package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	kafka "github.com/segmentio/kafka-go"
	scram "github.com/segmentio/kafka-go/sasl/scram"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

const supportedWebhookVersion = "4"

type (
	AlertmanagerKafkaExporter struct {
		kafkaWriter *kafka.Writer

		prometheus struct {
			alertsReceived   *prometheus.CounterVec
			alertsInvalid    *prometheus.CounterVec
			alertsSuccessful *prometheus.CounterVec
		}
	}

	KafkaSSLConfig struct {
		EnableSSL  bool
		CertFile   string
		KeyFile    string
		CACertFile string
	}

	KafkaSaslConfig struct {
		SecurityProtocol string
		SaslMechanism string
		ScramUsername string
		ScramPassword string
	}

	AlertmanagerEntry struct {
		Alerts []struct {
			Annotations  map[string]string `json:"annotations"`
			EndsAt       time.Time         `json:"endsAt"`
			GeneratorURL string            `json:"generatorURL"`
			Labels       map[string]string `json:"labels"`
			StartsAt     time.Time         `json:"startsAt"`
			Status       string            `json:"status"`
		} `json:"alerts"`
		CommonAnnotations map[string]string `json:"commonAnnotations"`
		CommonLabels      map[string]string `json:"commonLabels"`
		ExternalURL       string            `json:"externalURL"`
		GroupLabels       map[string]string `json:"groupLabels"`
		Receiver          string            `json:"receiver"`
		Status            string            `json:"status"`
		Version           string            `json:"version"`
		GroupKey          string            `json:"groupKey"`

		// Timestamp records when the alert notification was received
		Timestamp string `json:"@timestamp"`
	}
)

func (e *AlertmanagerKafkaExporter) Init() {
	e.prometheus.alertsReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "alertmanager2kafka_alerts_received",
			Help: "alertmanager2kafka received alerts",
		},
		[]string{},
	)
	prometheus.MustRegister(e.prometheus.alertsReceived)

	e.prometheus.alertsInvalid = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "alertmanager2kafka_alerts_invalid",
			Help: "alertmanager2kafka invalid alerts",
		},
		[]string{},
	)
	prometheus.MustRegister(e.prometheus.alertsInvalid)

	e.prometheus.alertsSuccessful = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "alertmanager2kafka_alerts_successful",
			Help: "alertmanager2kafka successful stored alerts",
		},
		[]string{},
	)
	prometheus.MustRegister(e.prometheus.alertsSuccessful)
}

func (e *AlertmanagerKafkaExporter) ConnectKafka(host string, topic string, sslConfig *KafkaSSLConfig, saslConfig *KafkaSaslConfig) {
	dialer := kafka.DefaultDialer
	log.Debugf("Starting Kafka connection")
	if sslConfig.EnableSSL {
		cert, err := tls.LoadX509KeyPair(sslConfig.CertFile, sslConfig.KeyFile)
		if err != nil {
			log.Fatalf("cannot load SSL key/certificate pair (key=%s, cert=%s): %s", sslConfig.KeyFile, sslConfig.CertFile, err)
		}

		if sslConfig.CACertFile == "" {
			sslConfig.CACertFile = "/etc/ssl/certs/ca-certificates.crt"
		}

		caCertPEM, err := ioutil.ReadFile(sslConfig.CACertFile)
		if err != nil {
			log.Fatalf("cannot read SSL CA certificate file %s: %s", sslConfig.CACertFile, err)
		}

		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM([]byte(caCertPEM)); !ok {
			log.Fatalf("cannot load SSL CA certificates from file %s: %s", sslConfig.CACertFile, err)
		}

		log.Infof("configured client-side SSL: key=%s, cert=%s, cacert=%s", sslConfig.KeyFile, sslConfig.CertFile, sslConfig.CACertFile)
		dialer.TLS = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
	}

	if strings.Contains(saslConfig.SecurityProtocol, "SASL") {
		log.Debugf("Configuring SASL")
		if strings.Contains(saslConfig.SaslMechanism, "SCRAM") {
			log.Debugf("Configuring SCRAM")
			if saslConfig.ScramUsername == "" || saslConfig.ScramPassword == "" {
				log.Fatalf("Username and password have to be provided if Sasl mechanism is scram")
			}			

			
			if ! sslConfig.EnableSSL {

				if sslConfig.CACertFile == "" {
					sslConfig.CACertFile = "/etc/ssl/certs/ca-certificates.crt"
				}
				
				caCertPEM, err := ioutil.ReadFile(sslConfig.CACertFile)
				if err != nil {
					log.Fatalf("cannot read SSL CA certificate file %s: %s", sslConfig.CACertFile, err)
				}
				
				caCertPool := x509.NewCertPool()
				if ok := caCertPool.AppendCertsFromPEM([]byte(caCertPEM)); !ok {
					log.Fatalf("cannot load SSL CA certificates from file %s: %s", sslConfig.CACertFile, err)
				}

				log.Infof("configured client-side SSL: cacert=%s", sslConfig.CACertFile)
				dialer.TLS = &tls.Config{
					RootCAs:      caCertPool,
				}
			}

			mechanism, err := scram.Mechanism(scram.SHA512, saslConfig.ScramUsername, saslConfig.ScramPassword)
			dialer.SASLMechanism = mechanism
			if err != nil {
				panic(err)
			}
		}
	}

	e.kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: strings.Split(host, ","),
		Topic:   topic,
		Dialer:  dialer,
	})
}

func (e *AlertmanagerKafkaExporter) HttpHandler(w http.ResponseWriter, r *http.Request) {
	e.prometheus.alertsReceived.WithLabelValues().Inc()

	if r.Body == nil {
		e.prometheus.alertsInvalid.WithLabelValues().Inc()
		err := errors.New("got empty request body")
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Error(err)
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		e.prometheus.alertsInvalid.WithLabelValues().Inc()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error(err)
		return
	}
	defer r.Body.Close()

	var msg AlertmanagerEntry
	err = json.Unmarshal(b, &msg)
	if err != nil {
		e.prometheus.alertsInvalid.WithLabelValues().Inc()
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Error(err)
		return
	}

	if msg.Version != supportedWebhookVersion {
		e.prometheus.alertsInvalid.WithLabelValues().Inc()
		err := fmt.Errorf("do not understand webhook version %q, only version %q is supported", msg.Version, supportedWebhookVersion)
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Error(err)
		return
	}

	now := time.Now()
	msg.Timestamp = now.Format(time.RFC3339)

	incidentJson, _ := json.Marshal(msg)
	err = e.kafkaWriter.WriteMessages(context.Background(), kafka.Message{Value: incidentJson})
	if err != nil {
		switch kafkaErr := err.(type) {
		case kafka.WriteErrors:
			err = kafkaErr[0]
		}

		errMsg := fmt.Errorf("unable to write into kafka: %s", err)
		e.prometheus.alertsInvalid.WithLabelValues().Inc()
		http.Error(w, errMsg.Error(), http.StatusBadRequest)
		log.Error(errMsg)
		return
	}

	log.Debugf("received and stored alert: %v", msg.CommonLabels)
	e.prometheus.alertsSuccessful.WithLabelValues().Inc()
}
