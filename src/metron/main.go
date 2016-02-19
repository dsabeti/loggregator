package main

import (
	"crypto/tls"
	"doppler/dopplerservice"
	"doppler/listeners"
	"flag"
	"fmt"
	"os"
	"time"

	"metron/clientpool"
	"metron/networkreader"
	"metron/writers/batch"
	"metron/writers/dopplerforwarder"
	"metron/writers/eventunmarshaller"
	"metron/writers/messageaggregator"
	"metron/writers/tagger"

	"logger"
	"metron/eventwriter"
	"runtime"

	"github.com/cloudfoundry/dropsonde/metric_sender"
	"github.com/cloudfoundry/dropsonde/metricbatcher"
	"github.com/cloudfoundry/dropsonde/metrics"
	"github.com/cloudfoundry/dropsonde/runtime_stats"
	"github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/gunk/workpool"
	"github.com/cloudfoundry/storeadapter"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"

	"metron/config"
	"metron/writers/picker"
	"profiler"
	"signalmanager"
)

var (
	logFilePath    = flag.String("logFile", "", "The agent log file, defaults to STDOUT")
	configFilePath = flag.String("config", "config/metron.json", "Location of the Metron config json file")
	debug          = flag.Bool("debug", false, "Debug logging")
	cpuprofile     = flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile     = flag.String("memprofile", "", "write memory profile to this file")
)

func main() {
	os.Exit(run())
}

func run() int {
	// Metron is intended to be light-weight so we occupy only one core
	runtime.GOMAXPROCS(1)

	flag.Parse()
	config, err := config.ParseConfig(*configFilePath)
	if err != nil {
		panic(err)
	}

	log := logger.NewLogger(*debug, *logFilePath, "metron", config.Syslog)
	log.Info("Startup: Setting up the Metron agent")

	profiler := profiler.New(*cpuprofile, *memprofile, 1*time.Second, log)
	profiler.Profile()
	defer profiler.Stop()

	picker, err := initializeDopplerPool(config, log)
	if err != nil {
		log.Errorf("Could not initialize doppler connection pool: %s", err)
		return -1
	}
	messageTagger := tagger.New(config.Deployment, config.Job, config.Index, picker)
	aggregator := messageaggregator.New(messageTagger, log)

	statsStopChan := make(chan struct{})
	initializeMetrics(messageTagger, config, statsStopChan, log)

	dropsondeUnmarshaller := eventunmarshaller.New(aggregator, log)
	metronAddress := fmt.Sprintf("127.0.0.1:%d", config.IncomingUDPPort)
	dropsondeReader, err := networkreader.New(metronAddress, "dropsondeAgentListener", dropsondeUnmarshaller, log)
	if err != nil {
		log.Errorf("Failed to listen on %s: %s", metronAddress, err)
		return 1
	}

	log.Info("metron started")
	go dropsondeReader.Start()

	dumpChan := signalmanager.RegisterGoRoutineDumpSignalChannel()
	killChan := signalmanager.RegisterKillSignalChannel()

	for {
		select {
		case <-dumpChan:
			signalmanager.DumpGoRoutine()
		case <-killChan:
			log.Info("Shutting down")
			close(statsStopChan)
			return 0
		}
	}
}

func initializeDopplerPool(conf *config.Config, logger *gosteno.Logger) (*picker.Picker, error) {
	adapter, err := storeAdapterProvider(conf.EtcdUrls, conf.EtcdMaxConcurrentRequests)
	if err != nil {
		return nil, err
	}
	err = adapter.Connect()
	if err != nil {
		logger.Warnd(map[string]interface{}{
			"error": err.Error(),
		}, "Failed to connect to etcd")
	}

	udpCreator := clientpool.NewUDPClientCreator(logger)
	udpWrapper := dopplerforwarder.NewUDPWrapper([]byte(conf.SharedSecret), logger)
	udpPool := clientpool.NewDopplerPool(logger, udpCreator)
	udpForwarder := dopplerforwarder.New(udpWrapper, udpPool, logger)
	writers := []picker.WeightedByteWriter{udpForwarder}
	defaultWriter := writers[0]

	var tcpPool *clientpool.DopplerPool
	var tlsConfig *tls.Config
	if conf.PreferredProtocol != "udp" {
		if conf.PreferredProtocol == "tls" {
			c := conf.TLSConfig
			tlsConfig, err = listeners.NewTLSConfig(c.CertFile, c.KeyFile, c.CAFile)
			if err != nil {
				return nil, err
			}
			tlsConfig.ServerName = "doppler"
		}

		var writer picker.WeightedByteWriter
		writer, tcpPool, err = newTCPWriter(tlsConfig, conf, logger)
		if err != nil {
			return nil, err
		}
		writers = append(writers, writer)
		defaultWriter = writer
	}

	picker, err := picker.New(logger, defaultWriter, writers...)
	if err != nil {
		return nil, fmt.Errorf("Failed to initialize the doppler picker: %s", err)
	}

	finder := dopplerservice.NewFinder(adapter, conf.LoggregatorDropsondePort, string(conf.PreferredProtocol), conf.Zone, logger)
	finder.Start()
	go func() {
		for {
			event := finder.Next()
			udpPool.SetAddresses(event.UDPDopplers)

			if tcpPool != nil {
				tcpPool.SetAddresses(event.TCPDopplers)
			}
		}
	}()
	return picker, nil
}

func newTCPWriter(tlsConfig *tls.Config, conf *config.Config, logger *gosteno.Logger) (picker.WeightedByteWriter, *clientpool.DopplerPool, error) {
	tlsCreator := clientpool.NewTCPClientCreator(logger, tlsConfig)
	tlsWrapper := dopplerforwarder.NewTLSWrapper(logger)
	tlsPool := clientpool.NewDopplerPool(logger, tlsCreator)
	tlsForwarder := dopplerforwarder.New(tlsWrapper, tlsPool, logger)
	tcpBatchInterval := time.Duration(conf.TCPBatchIntervalMilliseconds) * time.Millisecond
	batchWriter, err := batch.NewWriter(tlsForwarder, conf.TCPBatchSizeBytes, tcpBatchInterval, logger)
	if err != nil {
		return nil, nil, err
	}
	return batchWriter, tlsPool, nil
}

func initializeMetrics(messageTagger *tagger.Tagger, config *config.Config, stopChan chan struct{}, logger *gosteno.Logger) {
	metricsAggregator := messageaggregator.New(messageTagger, logger)

	eventWriter := eventwriter.New("MetronAgent", metricsAggregator)
	metricSender := metric_sender.NewMetricSender(eventWriter)
	metricBatcher := metricbatcher.New(metricSender, time.Duration(config.MetricBatchIntervalMilliseconds)*time.Millisecond)
	metrics.Initialize(metricSender, metricBatcher)

	stats := runtime_stats.NewRuntimeStats(eventWriter, time.Duration(config.RuntimeStatsIntervalMilliseconds)*time.Millisecond)
	go stats.Run(stopChan)
}

func storeAdapterProvider(urls []string, concurrentRequests int) (storeadapter.StoreAdapter, error) {
	workPool, err := workpool.NewWorkPool(concurrentRequests)
	if err != nil {
		return nil, err
	}

	options := &etcdstoreadapter.ETCDOptions{
		ClusterUrls: urls,
	}
	etcdAdapter, err := etcdstoreadapter.New(options, workPool)
	if err != nil {
		return nil, err
	}

	return etcdAdapter, nil
}

type metronHealthMonitor struct{}

func (*metronHealthMonitor) Ok() bool {
	return true
}
