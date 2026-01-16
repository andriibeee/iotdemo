package entity

//go:generate msgp
type Event struct {
	IdempotencyID string `msg:"idempotency_id" json:"idempotency_id"`
	Sensor        string `msg:"sensor" json:"sensor"`
	Value         int    `msg:"val" json:"val"`
	UnixTimestamp int64  `msg:"ts" json:"ts"`
}
