package v1

import (
	"encoding/json"
	"errors"
	"ingestion/pkg/service"
	"net"
	"net/http"
	"strings"
)

type Api struct {
	ingestionService service.IngestionService
}

func NewApi(service service.IngestionService) *Api {

	return &Api{ingestionService: service}
}

func (a *Api) ReceiveEventHandler(appSecret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var event service.Event
		err := json.NewDecoder(r.Body).Decode(&event)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		userAgent := r.Header.Get("User-Agent")
		err = a.ingestionService.BatchEvent(ctx, event, userAgent, GetRealIP(r), appSecret)
		if err != nil {
			var valErr *service.ValidationError
			var infraErr *service.InfrastructureError

			switch {
			case errors.As(err, &valErr):
				// Ошибка валидации данных (бизнес-правила)
				http.Error(w, "validation failed", http.StatusBadRequest)

			case errors.As(err, &infraErr):
				// Ошибка инфраструктуры (БД, брокер сообщений упал)
				http.Error(w, "service unavailable", http.StatusServiceUnavailable)

			default:
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}

			return
		}

		w.WriteHeader(http.StatusAccepted)
	}
}

func GetRealIP(r *http.Request) string {
	// ищем стандартный заголовок от прокси-серверов
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded != "" {
		// X-Forwarded-For может содержать цепочку IP через запятую.
		// Нас интересует самый первый (оригинальный клиент)
		ips := strings.Split(forwarded, ",")
		return strings.TrimSpace(ips[0])
	}

	// Ищем специфичный заголовок Nginx (если настроен)
	realIP := r.Header.Get("X-Real-IP")
	if realIP != "" {
		return strings.TrimSpace(realIP)
	}

	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}

	return ip
}
