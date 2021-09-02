package sinks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/opsgenie/kubernetes-event-exporter/pkg/kube"
	"io/ioutil"
	"net/http"
)

const apiEndpoint = "/v2/markers/create-markers"

type LogzioConfig struct {
	BaseApi string `yaml:"baseApi"`
	Token   string `yaml:"token"`
	Debug   bool   `yaml:"debug"`
}

type LogzioMarker struct {
	Title       string      `json:"title"` // required
	Tag         string      `json:"tag,omitempty"`
	Timestamp   int64       `json:"timestamp"`
	Description string      `json:"description"` // required
	Metadata    interface{} `json:"metadata,omitempty"`
}

type LogzioMarkersRequest struct {
	Markers []LogzioMarker `json:"markers"`
}

func NewLogzioSink(cfg *LogzioConfig) (Sink, error) {
	err := validateConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &Logzio{cfg: cfg}, nil
}

type Logzio struct {
	cfg    *LogzioConfig
}

func (l *Logzio) Close() {
	l.Close()
}

func (l *Logzio) Send(ctx context.Context, ev *kube.EnhancedEvent) error {
	marker := parseEventToLogzioMarker(ev)
	markerRequest := LogzioMarkersRequest{Markers: []LogzioMarker{*marker}}
	reqBody, err := json.Marshal(markerRequest)
	if err != nil {
		return err
	}

	url := l.cfg.BaseApi + apiEndpoint
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-API-TOKEN", l.cfg.Token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		return errors.New("Error from Logz.io: " + string(body))
	}

	return nil
}

func validateConfig(cfg *LogzioConfig) error {
	if len(cfg.Token) == 0 {
		return fmt.Errorf("logzio token must be set")
	}
	if len(cfg.BaseApi) == 0 {
		return fmt.Errorf("logzio listener must be set")
	}

	return nil
}

func parseEventToLogzioMarker(ev *kube.EnhancedEvent) *LogzioMarker {
	// TODO - pick the relevant events.
	// TODO - flatten the event. Can't handle nested objects in metadata
	return &LogzioMarker{
		Title:       "k8s event",
		Tag:         "DEPLOYMENT",
		Timestamp:   ev.CreationTimestamp.Unix(),
		Description: ev.Message,
		Metadata:    ev,
	}
}
