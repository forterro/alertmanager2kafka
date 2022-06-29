package main

import (
	"fmt"
	"github.com/fpytloun/alertmanager2kafka/config"
	"github.com/jessevdk/go-flags"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"path"
	"runtime"
	"strings"
)

const (
	Author = "Filip Pytloun"
)

var (
	argparser *flags.Parser
	opts      config.Opts

	// Git version information
	gitCommit = "<unknown>"
	gitTag    = "<unknown>"
)

func main() {
	initArgparser()

	log.Infof("starting alertmanager2kafka v%s (%s; %s; by %v)", gitTag, gitCommit, runtime.Version(), Author)
	log.Info(string(opts.GetJson()))

	log.Infof("init exporter")
	exporter := &AlertmanagerKafkaExporter{}
	exporter.Init()

	sslConfig := &KafkaSSLConfig{}
	if opts.Kafka.SSLKey != "" {
		sslConfig.EnableSSL = true
		sslConfig.CertFile = opts.Kafka.SSLCert
		sslConfig.KeyFile = opts.Kafka.SSLKey
		sslConfig.CACertFile = opts.Kafka.SSLCACert
	} else {
		sslConfig.EnableSSL = false
		if opts.Kafka.SSLCACert != "" {
			sslConfig.CACertFile = opts.Kafka.SSLCACert
		}
	}
	
	saslConfig := &KafkaSaslConfig{}
	if opts.Kafka.SaslMechanism != "" {
		log.Debugf("Configuring SASL")
		saslConfig.SecurityProtocol = "SASL_SSL"
		saslConfig.SaslMechanism = opts.Kafka.SaslMechanism
		saslConfig.ScramUsername = opts.Kafka.ScramUsername
		saslConfig.ScramPassword = opts.Kafka.ScramPassword
	} else {
		saslConfig.SecurityProtocol = "plaintext"
	}

	exporter.ConnectKafka(opts.Kafka.Host, opts.Kafka.Topic, sslConfig, saslConfig)
	defer exporter.kafkaWriter.Close()

	// daemon mode
	log.Infof("starting http server on %s", opts.ServerBind)
	startHttpServer(exporter)
}

// init argparser and parse/validate arguments
func initArgparser() {
	argparser = flags.NewParser(&opts, flags.Default)
	_, err := argparser.Parse()

	// check if there is an parse error
	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		} else {
			fmt.Println()
			argparser.WriteHelp(os.Stdout)
			os.Exit(1)
		}
	}

	// verbose level
	if opts.Logger.Verbose {
		log.SetLevel(log.DebugLevel)
	}

	// debug level
	if opts.Logger.Debug {
		log.SetReportCaller(true)
		log.SetLevel(log.TraceLevel)
		log.SetFormatter(&log.TextFormatter{
			CallerPrettyfier: func(f *runtime.Frame) (string, string) {
				s := strings.Split(f.Function, ".")
				funcName := s[len(s)-1]
				return funcName, fmt.Sprintf("%s:%d", path.Base(f.File), f.Line)
			},
		})
	}

	// json log format
	if opts.Logger.LogJson {
		log.SetReportCaller(true)
		log.SetFormatter(&log.JSONFormatter{
			DisableTimestamp: false,
			CallerPrettyfier: func(f *runtime.Frame) (string, string) {
				s := strings.Split(f.Function, ".")
				funcName := s[len(s)-1]
				return funcName, fmt.Sprintf("%s:%d", path.Base(f.File), f.Line)
			},
		})
	}
}

// start and handle prometheus handler
func startHttpServer(exporter *AlertmanagerKafkaExporter) {
	// healthz
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if _, err := fmt.Fprint(w, "Ok"); err != nil {
			log.Error(err)
		}
	})
	http.HandleFunc("/webhook", exporter.HttpHandler)
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(opts.ServerBind, nil))
}
