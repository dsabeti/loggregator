package batch

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/cloudfoundry/dropsonde/emitter"
	"github.com/cloudfoundry/dropsonde/envelope_extensions"
	"github.com/cloudfoundry/dropsonde/metrics"
	"github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"
)

const (
	maxOverflowTries  = 5
	minBufferCapacity = 1024
)

type messageBuffer struct {
	bytes.Buffer
	messages uint64
}

func newMessageBuffer(bufferBytes []byte) *messageBuffer {
	child := bytes.NewBuffer(bufferBytes)
	return &messageBuffer{
		Buffer: *child,
	}
}

func (b *messageBuffer) Write(msg []byte) (int, error) {
	b.messages++
	return b.Buffer.Write(msg)
}

func (b *messageBuffer) Reset() {
	b.messages = 0
	b.Buffer.Reset()
}

type Writer struct {
	flushDuration time.Duration
	outWriter     WeightedWriter
	writerLock    sync.Mutex
	msgBuffer     *messageBuffer
	msgBufferLock sync.Mutex
	timer         *time.Timer
	logger        *gosteno.Logger
}

//go:generate hel --type WeightedWriter --output mock_weighted_writer_test.go

type WeightedWriter interface {
	Write(bytes []byte) (sentLength int, err error)
	Weight() int
}

func NewWriter(writer WeightedWriter, bufferCapacity uint64, flushDuration time.Duration, logger *gosteno.Logger) (*Writer, error) {
	if bufferCapacity < minBufferCapacity {
		return nil, fmt.Errorf("batch.Writer requires a buffer of at least %d bytes", minBufferCapacity)
	}

	// Initialize the timer with a long duration so we can stop it before
	// it triggers.  Ideally, we'd initialize the timer without starting
	// it, but that doesn't seem possible in the current library.
	batchTimer := time.NewTimer(time.Second)
	batchTimer.Stop()
	batchWriter := &Writer{
		flushDuration: flushDuration,
		outWriter:     writer,
		msgBuffer:     newMessageBuffer(make([]byte, 0, bufferCapacity)),
		timer:         batchTimer,
		logger:        logger,
	}
	go batchWriter.flushOnTimer()
	return batchWriter, nil
}

func (w *Writer) Write(msgBytes []byte) (int, error) {
	buffer := bytes.NewBuffer(make([]byte, 0, len(msgBytes)*2))
	err := binary.Write(buffer, binary.LittleEndian, uint32(len(msgBytes)))
	if err != nil {
		w.logger.Errorf("Error encoding message length: %v\n", err)
		metrics.BatchIncrementCounter("tls.sendErrorCount")
		return 0, err
	}
	_, err = buffer.Write(msgBytes)
	if err != nil {
		return 0, err
	}
	w.msgBufferLock.Lock()
	switch {
	case w.msgBuffer.Len()+buffer.Len() > w.msgBuffer.Cap():
		messages := w.msgBuffer.messages
		w.msgBufferLock.Unlock()
		go func() {
			_, dropped, err := w.retryWrites(buffer.Bytes())
			if err != nil {
				metrics.BatchAddCounter("MessageBuffer.droppedMessageCount", uint64(dropped))
				logMsg, marshalErr := proto.Marshal(w.droppedLogMessage(uint64(dropped)))
				if marshalErr != nil {
					w.logger.Fatalf("Failed to marshal generated log message: %s", logMsg)
				}
				w.Write(logMsg)
			}
		}()
		return int(messages), nil
	default:
		defer w.msgBufferLock.Unlock()
		if w.msgBuffer.Len() == 0 {
			w.timer.Reset(w.flushDuration)
		}
		return w.msgBuffer.Write(buffer.Bytes())
	}
}

func (w *Writer) Stop() {
	w.timer.Stop()
}

func (w *Writer) buffer(addedMessage []byte) ([]byte, uint) {
	toWrite := make([]byte, 0, w.msgBuffer.Len()+len(addedMessage))
	toWrite = append(toWrite, w.msgBuffer.Bytes()...)
	toWrite = append(toWrite, addedMessage...)

	bufferMessageCount := w.msgBuffer.messages
	if len(addedMessage) > 0 {
		bufferMessageCount++
	}
	w.msgBuffer.Reset()
	return toWrite, uint(bufferMessageCount)
}


func (w *Writer) flushWrite(bytes []byte, messages uint) (int, error) {
	w.writerLock.Lock()
	defer w.writerLock.Unlock()
	sent, err := w.outWriter.Write(bytes)
	if err != nil {
		w.logger.Warnf("Received error while trying to flush TCP bytes: %s", err)
		return 0, err
	}

	metrics.BatchAddCounter("DopplerForwarder.sentMessages", uint64(messages))
	return sent, nil
}

func (w *Writer) flushOnTimer() {
	for range w.timer.C {
		w.flushBuffer()
	}
}

func (w *Writer) flushBuffer() {
	w.msgBufferLock.Lock()
	if w.msgBuffer.messages == 0 {
		w.msgBufferLock.Unlock()
		return
	}
	flushBytes, messages := w.buffer(nil)
	w.msgBufferLock.Unlock()
	if _, err := w.flushWrite(flushBytes, messages); err != nil {
		metrics.BatchIncrementCounter("DopplerForwarder.retryCount")
		w.timer.Reset(w.flushDuration)
	}
}

func (w *Writer) Weight() int {
	return w.outWriter.Weight()
}

func (w *Writer) retryWrites(message []byte) (sent, dropped int, err error) {
	w.msgBufferLock.Lock()
	flushBytes, messages := w.buffer(message)
	w.msgBufferLock.Unlock()
	for i := 0; i < maxOverflowTries; i++ {
		if i > 0 {
			metrics.BatchIncrementCounter("DopplerForwarder.retryCount")
		}
		sent, err = w.flushWrite(flushBytes, messages)
		if err == nil {
			return sent, 0, nil
		}
	}
	return 0, int(messages), err
}

func (w *Writer) droppedLogMessage(droppedMessages uint64) *events.Envelope {
	logMessage := &events.LogMessage{
		Message:     []byte(fmt.Sprintf("Dropped %d message(s) from MetronAgent to Doppler", droppedMessages)),
		MessageType: events.LogMessage_ERR.Enum(),
		AppId:       proto.String(envelope_extensions.SystemAppId),
		Timestamp:   proto.Int64(time.Now().UnixNano()),
	}
	env, err := emitter.Wrap(logMessage, "MetronAgent")
	if err != nil {
		w.logger.Fatalf("Failed to emitter.Wrap a log message: %s", err)
	}
	return env
}
