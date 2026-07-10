package app

import (
	"context"
	api "ingestion/pkg/api/v1"
	"ingestion/pkg/service"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
	"github.com/nats-io/nats.go"
)

type diContainer struct {
	Server           *http.Server
	natsConn         *nats.Conn
	natsJSContext    nats.JetStreamContext
	ingestionService service.IngestionService
	publisher        service.Publisher
}

func NewDiContainer() *diContainer {
	return &diContainer{}
}

func (d *diContainer) NewServer(ctx context.Context, config *Config) (*http.Server, error) {
	r := chi.NewRouter()

	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(10 * time.Second))
	r.Use(render.SetContentType(render.ContentTypeJSON))

	ingestionService, err := d.NewIngestionService(config)
	if err != nil {
		return nil, err
	}
	a := api.NewApi(ingestionService)

	r.Route("/", func(r chi.Router) {
		r.Post("/track", a.ReceiveEventHandler())
	})

	d.Server = &http.Server{
		Addr:              net.JoinHostPort(config.HttpHost, config.HttpPort),
		Handler:           r,
		ReadHeaderTimeout: config.ReadHeaderTimeout,
	}
	return d.Server, nil
}
func (d *diContainer) NewNatsConn(config *Config) (*nats.Conn, error) {
	if d.natsConn == nil {
		opts := []nats.Option{
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(5),
			nats.ReconnectWait(2 * time.Second),
		}
		nc, err := nats.Connect(config.NatsUrl, opts...)
		if err != nil {
			return nil, err
		}
		d.natsConn = nc
	}

	return d.natsConn, nil
}

func (d *diContainer) NewNatsJSContext(config *Config) (nats.JetStreamContext, error) {
	if d.natsJSContext == nil {
		nc, err := d.NewNatsConn(config)
		if err != nil {
			return nil, err
		}

		js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
		if err != nil {
			return nil, err
		}

		d.natsJSContext = js
	}

	return d.natsJSContext, nil
}

func (d *diContainer) NewIngestionService(config *Config) (service.IngestionService, error) {
	if d.ingestionService == nil {
		publisher, err := d.NewPublisher(config)
		if err != nil {
			return nil, err
		}
		d.ingestionService = service.NewService(publisher)
	}

	return d.ingestionService, nil
}

func (d *diContainer) NewPublisher(config *Config) (service.Publisher, error) {
	if d.publisher == nil {
		js, err := d.NewNatsJSContext(config)
		if err != nil {
			return nil, err
		}
		d.publisher = service.NewPublisher(js)
	}
	return d.publisher, nil
}
