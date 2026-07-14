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
	"github.com/oschwald/geoip2-golang"
)

type diContainer struct {
	Server           *http.Server
	natsConn         *nats.Conn
	natsJSContext    nats.JetStreamContext
	ingestionService service.IngestionService
	publisher        service.Publisher
	geoipDB          *geoip2.Reader
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
		r.Post("/track", a.ReceiveEventHandler(config.AppSecret))
	})

	d.Server = &http.Server{
		Addr:              net.JoinHostPort(config.HttpHost, config.HttpPort),
		Handler:           r,
		ReadHeaderTimeout: config.ReadHeaderTimeout,
	}
	return d.Server, nil
}
func (d *diContainer) NewNatsConn(config *Config) error {
	if d.natsConn == nil {
		opts := []nats.Option{
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(5),
			nats.ReconnectWait(2 * time.Second),
		}
		nc, err := nats.Connect(config.NatsUrl, opts...)
		if err != nil {
			return err
		}
		d.natsConn = nc
	}

	return nil
}

func (d *diContainer) InitNatsJSContext(config *Config) error {
	if d.natsJSContext == nil {
		err := d.NewNatsConn(config)
		if err != nil {
			return err
		}

		js, err := d.natsConn.JetStream(nats.PublishAsyncMaxPending(256))
		if err != nil {
			return err
		}

		d.natsJSContext = js
	}

	return nil
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
		err := d.InitNatsJSContext(config)
		if err != nil {
			return nil, err
		}
		err = d.NewGeoipDB(config)
		if err != nil {
			return nil, err
		}
		d.publisher = service.NewPublisher(d.natsJSContext, d.geoipDB)
	}
	return d.publisher, nil
}

func (d *diContainer) NewGeoipDB(config *Config) error {
	if d.geoipDB == nil {
		db, err := geoip2.Open(config.GeoipPath)
		if err != nil {
			return err
		}
		d.geoipDB = db
	}

	return nil
}
