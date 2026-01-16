package transport

import "github.com/andriibeee/iotdemo/internal/entity"

type Sink interface {
	Append(ev entity.Event) error
}
