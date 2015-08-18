package boltrrd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

var (
	delimiter = "|"
)

// Storage Main struct for bolt database file, contains methods for
// connecting to db and for  storages access
type Storage struct {
	db     *bolt.DB
	dbpath string
	rrd    map[string]RRDBucket
}

// NewStorage Open file with db and create connection with it, also
// load info about all buckets to Storage struct
func NewStorage(dbpath string) (*Storage, error) {
	stor := &Storage{
		dbpath: dbpath,
		rrd:    map[string]RRDBucket{},
	}
	err := stor.getDb()
	if err != nil {
		return nil, &ConnectionError{"Can't connect to db '" + dbpath + "'", err}
	}

	err = stor.load()
	if err != nil {
		return nil, err
	}
	return stor, nil
}

func (s *Storage) load() error {
	if s == nil || s.db == nil {
		return &ConnectionError{"Connection not established", nil}
	}

	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			// get bucket info
			v := b.Get([]byte("INFO"))

			if len(v) < 1 {
				return errors.New("No 'INFO' entry in bucket " + string(name))
			}
			info := info{}
			err := json.Unmarshal(v, &info)
			if err != nil || info.Capacity < 1 || info.Step < 1 || len(info.Ds) < 1 {
				return errors.New("Bad format of INFO entry in bucket " + string(name))
			}

			s.rrd[string(name)] = RRDBucket{
				stor:       s,
				bucketName: string(name),
				inf:        &info,
			}

			return nil
		})

	})
	if err != nil {
		return &DataError{"Can't load storage structure from db", err}
	}
	return nil
}

// Close Close connection to db file
func (s *Storage) Close() error {
	if s.db == nil {
		return &ConnectionError{"Connection not set", nil}
	}
	err := s.db.Close()
	if err != nil {
		return &ConnectionError{"Can't close connection", err}
	}
	return nil
}

// info Struct stores information about current storage (bucket) and contains in
// entry with key "INFO"
type info struct {
	Step     int
	Capacity int
	Ds       []string
}

// RRDBucket Struct to represent single bucket with stored data
type RRDBucket struct {
	stor       *Storage
	bucketName string
	inf        *info
}

// NewRRD Create RRD bucket with given name. Step is time in seconds and capacity
// is number of steps which stored in bucket. Other parameters is a list of all data storages
func (s *Storage) NewRRD(name string, step int, capacity int, DSs ...string) (*RRDBucket, error) {
	if s.db == nil {
		return nil, &ConnectionError{"Connection not established", nil}
	}

	if step < 1 {
		return nil, &ParamsError{"Step must be greater than 0", nil}
	}

	if capacity < 1 {
		return nil, &ParamsError{"Capacity must be greater than 0", nil}
	}

	if len(DSs) < 1 {
		return nil, &ParamsError{"No data storages specified", nil}
	}

	rrd := &RRDBucket{
		stor:       s,
		bucketName: name,
		inf: &info{
			Step:     step,
			Capacity: capacity,
		},
	}

	rrd.inf.Ds = make([]string, len(DSs))
	copy(rrd.inf.Ds, DSs)

	err := rrd.init()
	if err != nil {
		return nil, &ParamsError{"Can't create bucket '" + name + "'", err}
	}

	s.rrd[name] = *rrd
	return rrd, nil
}

// RRD Provide access to bucket by its name
func (s *Storage) RRD(name string) *RRDBucket {

	if rrd, ok := s.rrd[name]; ok {
		return &rrd
	}
	return &RRDBucket{nil, name, nil}
}

// ListRRD Returns list of all buckets in storage
func (s *Storage) ListRRD() (rrds []string, err error) {
	if s.db == nil {
		err = &ConnectionError{"Connection not established", nil}
		return
	}

	for k := range s.rrd {
		rrds = append(rrds, k)
	}
	return
}

// init Create bucket in bolt db
func (rrd *RRDBucket) init() error {
	if rrd.stor.db == nil {
		return &ConnectionError{"Connection not established", nil}
	}

	return rrd.stor.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte(rrd.bucketName))
		if err != nil {
			return err
		}

		value, err := json.Marshal(rrd.inf)
		if err != nil {
			return err
		}

		err = bucket.Put([]byte("INFO"), value)
		return err
	})

}

// Save Save dataset to bucket whith current time. Number of params must be equal to number of
// data storages in bucket
func (rrd *RRDBucket) Save(params ...float64) error {
	//Storage.RRD(name) returned empty value so bucket not found
	if rrd.inf == nil {
		return &ParamsError{"Bucket '" + rrd.bucketName + "' not found", nil}
	}

	if rrd.stor.db == nil {
		return &ConnectionError{"Connection not established", nil}
	}

	if len(params) < 0 {
		return &ParamsError{"No data specified", nil}
	}

	err := rrd.save(time.Now(), params...)
	if err != nil {
		return &ParamsError{"Can't save data", err}
	}
	return nil
}

func (rrd *RRDBucket) save(t time.Time, params ...float64) error {
	//Check if number of DSs match with number of params
	if len(rrd.inf.Ds) != len(params) {
		return errors.New(fmt.Sprint("Wrong number of params: ", len(params), ", must be: ", len(rrd.inf.Ds)))
	}

	return rrd.stor.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(rrd.bucketName))

		unixTime := fmt.Sprint(t.Unix())

		//If storage is full then delete older entries
		if bucket.Stats().KeyN-rrd.inf.Capacity > 0 {
			keyToDelete := fmt.Sprint(t.Unix() - int64(rrd.inf.Capacity*rrd.inf.Step))

			c := bucket.Cursor()
			for k, _ := c.First(); k != nil && bytes.Compare(k, []byte(keyToDelete)) <= 0; k, _ = c.Next() {
				err := bucket.Delete(k)
				if err != nil {
					return err
				}
			}

		}

		return bucket.Put([]byte(unixTime), formatValue(params))
	})
}

// Stat Structure to represent values stored in db in
// format which suitable for js charting libraries
type Stat struct {
	Labels   []string  `json:"labels"`
	Datasets []Dataset `json:"datasets"`
}

// Dataset Part of json data structure. Represents values for every data storage
type Dataset struct {
	Label string    `json:"label"`
	Data  []float64 `json:"data"`
}

// GetStats Return data for the specified time interval and scaled with step
func (rrd *RRDBucket) GetStats(from, to time.Time, step int) (stat *Stat, err error) {
	//Storage.RRD(name) returned empty value so bucket not found
	if rrd.inf == nil {
		return nil, &ParamsError{"Bucket '" + rrd.bucketName + "' not found", nil}
	}

	if rrd.stor.db == nil {
		return nil, &ConnectionError{"Connection not established", nil}
	}

	if to.Before(from) {
		return nil, &ParamsError{"End of interval must be after its beginning", nil}
	}

	if step < 1 {
		return nil, &ParamsError{"Step must be greater than 0", nil}
	}

	defer func() {
		if r := recover(); r != nil {
			err = r.(error)

		}
	}()

	values, labels := rrd.getRange(from, to, step)

	stat = &Stat{}
	stat.Labels = labels

	for i := range rrd.inf.Ds {

		ds := Dataset{}
		ds.Label = rrd.inf.Ds[i]
		for _, v := range values {
			ds.Data = append(ds.Data, v[i])
		}
		stat.Datasets = append(stat.Datasets, ds)

	}

	return

}

func (rrd *RRDBucket) getRange(from, to time.Time, step int) (result [][]float64, labels []string) {
	toBolt := func(value int64) []byte {
		return []byte(fmt.Sprint(value))
	}

	// In one step will be many entries
	if step >= rrd.inf.Step {
		rrd.stor.db.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(rrd.bucketName))
			c := bucket.Cursor()

			startInterval := from.Unix()

			//Loop through given time interval (from - to) with given step
			for endInterval := from.Unix() + int64(step); startInterval < to.Unix(); startInterval += int64(step) {
				endInterval = startInterval + int64(step)

				valuesInStep := [][]float64{}
				for k, v := c.Seek(toBolt(startInterval)); k != nil && string(k) != "INFO" && bytes.Compare(k, toBolt(endInterval)) <= 0; k, v = c.Next() {
					valuesInStep = append(valuesInStep, splitToFloat(string(v)))
				}

				if len(valuesInStep) < 1 {
					continue
				}

				//Use middle of interval as label for dataset
				t := time.Unix((startInterval+endInterval)/2, 0)
				labels = append(labels, t.Format(time.Stamp))

				result = append(result, averageтInStep(valuesInStep))
			}
			return nil
		})
		//Steps will be between entries
	} else {
		rrd.stor.db.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(rrd.bucketName))
			c := bucket.Cursor()

			//Loop through given time interval (from - to) with given step
			for cur := from.Unix(); cur < to.Unix(); cur += int64(step) {

				valuesInStep := [][]float64{}

				//c.Seek() returns key of next entry by default
				nextKey, nextVal := c.Seek(toBolt(cur))
				prevKey, prevVal := c.Prev()

				if string(nextKey) == "INFO" || string(prevKey) == "INFO" {
					continue
				}

				if len(prevVal) > 0 {
					valuesInStep = append(valuesInStep, splitToFloat(string(prevVal)))
				}

				if len(nextVal) > 0 {
					valuesInStep = append(valuesInStep, splitToFloat(string(nextVal)))
				}

				if len(valuesInStep) < 1 {
					continue
				}
				t := time.Unix(cur, 0)
				labels = append(labels, t.Format(time.Stamp))

				result = append(result, averageтInStep(valuesInStep))
			}
			return nil
		})
	}

	return
}

// averageтInStep Returns average for every value in one step
func averageтInStep(source [][]float64) []float64 {
	length := len(source)
	if len(source[0]) < 1 {
		panic(&DataError{"No values in entry", nil})
	}

	average := make([]float64, len(source[0]))
	for _, values := range source {
		for i := range values {
			average[i] += values[i]
		}
	}

	for i := range average {
		average[i] /= float64(length)
	}
	return average
}

// splitToFloat Split line by delimiter and parse values to float64
func splitToFloat(source string) (result []float64) {
	values := strings.Split(source, delimiter)

	for _, v := range values {
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			panic(&DataError{"Can't get values from entry. Bad format: " + source, nil})
		}
		result = append(result, parsed)
	}
	return
}

// getDb Connect to bolt db
func (s *Storage) getDb() error {
	var err error
	s.db, err = bolt.Open(s.dbpath, 0600, nil)
	return err
}

// formatValue Format slice of float to string separated
// by delimiter and convert it to []byte
func formatValue(params []float64) []byte {
	strParams := []string{}
	for _, v := range params {
		strParams = append(strParams, fmt.Sprint(v))
	}
	return []byte(strings.Join(strParams, delimiter))
}

//----Errors----

// ConnectionError Problems with connection to db
type ConnectionError struct {
	Text string
	Err  error
}

func (e *ConnectionError) Error() string {
	if e.Err != nil {
		return e.Text + ": " + e.Err.Error()
	}
	return e.Text
}

// DataError Bad format of data in storage
type DataError struct {
	Text string
	Err  error
}

func (e *DataError) Error() string {
	if e.Err != nil {
		return e.Text + ": " + e.Err.Error()
	}
	return e.Text
}

// ParamsError Wrong params in call of interface functions
type ParamsError struct {
	Text string
	Err  error
}

func (e *ParamsError) Error() string {
	if e.Err != nil {
		return e.Text + ": " + e.Err.Error()
	}
	return e.Text
}
