package service

import batchingService "batching/pkg/service"

func IngestionEventToBatchEvent(event Event) batchingService.Event {
	return batchingService.Event{
		EventType:  batchingService.EventType(event.EventType),
		UserID:     event.UserID,
		SessionID:  event.SessionID,
		TargetID:   event.TargetID,
		MetaData:   event.MetaData,
		ClientTime: event.ClientTime,
	}
}
