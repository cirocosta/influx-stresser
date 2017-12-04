package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/alexflint/go-arg"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"

	influx "github.com/influxdata/influxdb/client/v2"
)

type Stresser struct {
	client   influx.Client
	batches  uint64
	points   uint64
	database string
}

type StresserConfig struct {
	Database    string `arg:"env,required"`
	Host        string `arg:"env"`
	Port        int    `arg:"env"`
	User        string `arg:"env"`
	Password    string `arg:"env"`
	Concurrency uint   `arg:"help:number of workers"`
	Batches     uint64 `arg:"help:number of batches to send"`
	Points      uint64 `arg:"help:# of points to send per batch"`
}

var (
	cli = &StresserConfig{
		Database:    "",
		Host:        "localhost",
		Port:        8086,
		User:        "",
		Password:    "",
		Concurrency: 4,
		Points:      20,
		Batches:     100,
	}
	logger = zerolog.New(os.Stdout)
)

func NewStresser(cfg StresserConfig) (s Stresser, err error) {
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr:     fmt.Sprintf("http://%s:%d", cfg.Host, cfg.Port),
		Username: cfg.User,
		Password: cfg.Password,
	})

	if err != nil {
		err = errors.Wrapf(err,
			"failed to create influx client - %+v",
			cfg)
		return
	}

	s.client = c
	s.points = cfg.Points
	s.batches = cfg.Batches
	s.database = cfg.Database

	return
}

func (s Stresser) Start(bps chan<- influx.BatchPoints) (err error) {
	var (
		bp     influx.BatchPoints
		pt     *influx.Point
		tags          = map[string]string{"cpu": "cpu-total"}
		fields        = map[string]interface{}{}
		batch  uint64 = 0
		point  uint64 = 0
	)

	for batch = 0; batch < s.batches; batch++ {
		bp, err = influx.NewBatchPoints(influx.BatchPointsConfig{
			Database: s.database,
		})
		if err != nil {
			err = errors.Wrapf(err,
				"failed to create batch points for db %s",
				s.database)
			return
		}

		for point = 0; point < s.points; point++ {
			fields["idle"] = rand.Float64()
			fields["system"] = rand.Float64()
			fields["user"] = rand.Float64()

			pt, err = influx.NewPoint("cpu_usage", tags, fields, time.Now())
			if err != nil {
				err = errors.Wrapf(err, "failed to create point cpu_usage")
				return
			}

			bp.AddPoint(pt)
		}

		bps <- bp
	}
	close(bps)

	return
}

func (s Stresser) GenBatchWriter(bps <-chan influx.BatchPoints, errChan chan<- error) {
	var (
		err error
		bp  influx.BatchPoints
	)

	logger.Info().Msg("writer created")

	for bp = range bps {
		logger.Info().Msg("writing batch")

		err = s.client.Write(bp)
		if err != nil {
			errChan <- errors.Wrapf(err,
				"failed to write batch")
			continue
		}
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	arg.MustParse(cli)

	var (
		bpsChan     = make(chan influx.BatchPoints, 1000)
		errChan     = make(chan error, 10)
		wg          sync.WaitGroup
		concurrency = int(cli.Concurrency)
	)

	stresser, err := NewStresser(*cli)
	must(err)

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			stresser.GenBatchWriter(bpsChan, errChan)
		}()
	}

	go func() {
		for err := range errChan {
			logger.Error().Err(err).Msg("received err")
		}
	}()

	err = stresser.Start(bpsChan)
	must(err)

	logger.Info().Msg("finished BPS list")

	wg.Wait()
}
