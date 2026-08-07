package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/mileusna/useragent"
	"github.com/nats-io/nats.go"
	"github.com/oschwald/geoip2-golang"
)

type Publisher interface {
	Publish(context context.Context, event Event, userAgent, remoteAddr, appSecret string) error
	Drain() error
}

var _ Publisher = (*NcPublisher)(nil)

type NcPublisher struct {
	jsContext nats.JetStreamContext
	geoipDB   *geoip2.Reader
}

func NewPublisher(nc nats.JetStreamContext, geoDB *geoip2.Reader) *NcPublisher {
	return &NcPublisher{jsContext: nc, geoipDB: geoDB}
}

func (ncp NcPublisher) Publish(ctx context.Context, event Event, userAgent, remoteAddr, appSecret string) error {
	batchingEvent := IngestionEventToBatchEvent(event)

	ua := useragent.Parse(userAgent)
	batchingEvent.Browser = ua.Name
	batchingEvent.OS = ua.OS
	batchingEvent.EventId = uuid.NewString()
	batchingEvent.Device = getDevice(ua)
	batchingEvent.VisitorID = generateVisitorID(remoteAddr, userAgent, appSecret)
	batchingEvent.CountryIsoCode = ncp.getCountryIsoCode(remoteAddr)

	payload, err := json.Marshal(batchingEvent)

	if err != nil {
		log.Println("Критическая ошибка сериализации:", err)
		return &InfrastructureError{Operation: "json.Marshal", Err: err}
	}
	_, err = ncp.jsContext.PublishAsync("hh.events", payload)

	if err != nil {
		log.Println("Ошибка отправки в NATS:", err)
		return &InfrastructureError{Operation: "Ошибка отправки в NATS: ", Err: err}
	}

	return nil
}

func (ncp NcPublisher) getCountryIsoCode(remoteAddr string) string {
	if ncp.geoipDB == nil {
		return ""
	}

	ip := net.ParseIP(remoteAddr)
	if ip == nil {
		return ""
	}

	record, err := ncp.geoipDB.Country(ip)
	if err != nil {
		log.Printf("getCountryIsoCode failed: %v", err)
		return ""
	}

	return record.Country.IsoCode
}

func getDevice(ua useragent.UserAgent) string {
	switch {
	case ua.Desktop:
		return "desktop"
	case ua.Mobile:
		return "mobile"
	case ua.Tablet:
		return "tablet"
	case ua.Bot:
		return "bot"
	default:
		return "unknown"
	}
}

func generateVisitorID(ip, userAgent, appSecret string) string {
	dailySalt := time.Now().UTC().Format(time.DateOnly)
	rawData := dailySalt + appSecret + ip + userAgent
	h := sha256.Sum256([]byte(rawData))

	return hex.EncodeToString(h[:])
}

// Drain дожидается всех in-flight публикаций
func (ncp NcPublisher) Drain() error {
	done := ncp.jsContext.PublishAsyncComplete()
	select {
	case <-done:
		return nil
	case <-time.After(10 * time.Second):
		return errors.New("timeout while draining pending publishes")
	}
}
