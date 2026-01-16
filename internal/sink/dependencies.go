package sink

import "github.com/andriibeee/iotdemo/pkg/journal"

//go:generate mockgen -source=dependencies.go -destination=mock_journal_test.go -package=sink
type Journal interface {
	Write(k, v []byte) (uint64, error)
	WriteBatch(entries []journal.Entry) ([]uint64, error)
}
