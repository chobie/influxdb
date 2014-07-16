package engine

import (
	"github.com/influxdb/influxdb/parser"
	p "github.com/influxdb/influxdb/protocol"

	log "code.google.com/p/log4go"
)

type SinglePointEngine struct {
	query        *parser.SelectQuery
	processor    QueryProcessor
	shouldFilter bool
}

func NewSinglePointEngine(query *parser.SelectQuery, processor QueryProcessor) *SinglePointEngine {
	shouldFilter := query.GetWhereCondition() != nil
	return &SinglePointEngine{query, processor, shouldFilter}
}

// optimize for yield series and use it here
func (self *SinglePointEngine) YieldPoint(seriesName *string, columnNames []string, point *p.Point) bool {
	return self.YieldSeries(&p.Series{
		Name:   seriesName,
		Fields: columnNames,
		Points: []*p.Point{point},
	})
}

func (self *SinglePointEngine) YieldSeries(seriesIncoming *p.Series) bool {
	if !self.shouldFilter {
		return self.processor.YieldSeries(seriesIncoming)
	}

	series, err := Filter(self.query, seriesIncoming)
	if err != nil {
		log.Error("Error while filtering points: %s [query = %s]", err, self.query.GetQueryString())
		return false
	}
	if len(series.Points) == 0 {
		return true
	}
	return self.processor.YieldSeries(series)
}

func (self *SinglePointEngine) Close() {
	self.processor.Close()
}

func (self *SinglePointEngine) SetShardInfo(shardId int, shardLocal bool) {
	self.processor.SetShardInfo(shardId, shardLocal)
}
func (self *SinglePointEngine) GetName() string {
	return self.processor.GetName()
}
