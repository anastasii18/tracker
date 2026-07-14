package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/mileusna/useragent"
	"github.com/nats-io/nats.go"
	"github.com/oschwald/geoip2-golang"
)

type Publisher interface {
	Publish(context context.Context, event Event, userAgent, remoteAddr, appSecret string) error
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
	batchingEvent.Device = getDevice(ua)
	batchingEvent.VisitorID = generateVisitorID(remoteAddr, userAgent, appSecret)
	batchingEvent.CountryIsoCode = ncp.getCountryIsoCode(remoteAddr)

	payload, err := json.Marshal(batchingEvent)

	if err != nil {
		log.Println(fmt.Errorf("ошибка расшифровки event: %w", err))
		return err
	}

	_, err = ncp.jsContext.PublishAsync("hh.events", payload)

	if err != nil {
		log.Println("Ошибка отправки в NATS:", err)
		return err
	}

	return nil
}

func (ncp NcPublisher) getCountryIsoCode(remoteAddr string) string {
	ip := net.ParseIP(remoteAddr)

	record, err := ncp.geoipDB.City(ip)
	if err != nil {
		log.Fatalf("Lookup failed: %v", err)
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
