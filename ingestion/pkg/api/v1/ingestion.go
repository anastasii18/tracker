package v1

import (
	"encoding/json"
	"ingestion/pkg/service"
	"net/http"

	"github.com/go-chi/render"
)

type Api struct {
	ingestionService service.IngestionService
}

func NewApi(service service.IngestionService) *Api {

	return &Api{ingestionService: service}
}

func (a *Api) ReceiveEventHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var event service.Event
		err := json.NewDecoder(r.Body).Decode(&event)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		userAgent := r.Header.Get("User-Agent")
		err = a.ingestionService.BatchEvent(ctx, event, userAgent, r.RemoteAddr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		render.Status(r, http.StatusAccepted)
	}
}
