package models

type Record struct {
	Key   []byte
	Value []byte
	Topic string
}

type ConsumerConfig struct {
	Brokers               []string
	Name                  string
	Topic                 string
	EachPartitionChanSize int
	RecordsPerPoll        int
}
