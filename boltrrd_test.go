package boltrrd

import (
	"os"
	"testing"
	"time"
)

var (
	testDB = "test.db"
)

func delDb() {
	os.Remove(testDB)
}

func Test_NewStorage(t *testing.T) {
	stor, err := NewStorage(testDB)
	defer func() { stor.Close(); delDb() }()
	if err != nil {
		t.Error(err)
	}
}

func Test_NewStorageWithError(t *testing.T) {
	//try to open not valid path
	_, err := NewStorage("/asfkajd/kskd.adf")
	defer func() { delDb() }()
	if err == nil {
		t.Error("For file '/asfkajd/kskd.adf' error must be not nil")
	}
}

func Test_RRD(t *testing.T) {
	stor, err := NewStorage(testDB)
	defer func() { stor.Close(); delDb() }()
	if err != nil {
		t.Error(err)
	}

	buckName := "testing"
	buckStep := 2
	buckCapacity := 5
	DSs := []string{"test1", "test2"}

	if _, err := stor.NewRRD(buckName, buckStep, buckCapacity, DSs...); err != nil {
		t.Error(err)
	}

	//try to create bucket with same name
	if _, err := stor.NewRRD(buckName, buckStep, buckCapacity, DSs...); err == nil {
		t.Error("Trying to create bucket with existing name, err must be not nil")
	}

	//try get created bucket
	buck := stor.RRD("testing")
	if buck.bucketName != buckName ||
		buck.inf.Capacity != buckCapacity ||
		buck.inf.Step != buckStep ||
		buck.inf.Ds[1] != DSs[1] {
		t.Error("Bucket not stored properly")
	}

	//try to save some data
	counter := 0
	endTime := time.Now().Add(10 * time.Second)
	for ti := time.Now(); ti.Before(endTime); ti = ti.Add(time.Duration(buckStep) * time.Second) {
		if err := buck.save(ti, float64(counter), float64(counter*2)); err != nil {
			t.Error(err)
		}
		counter++
		//try to save not valid data:
		if err := buck.Save(5); err == nil {
			t.Error("Vrong number of params, err must be not nil")
		}
		if err := buck.Save(5, 6, 5, 8); err == nil {
			t.Error("Vrong number of params, err must be not nil")
		}
	}

	//And get our data back
	expected := Stat{[]Dataset{Dataset{DSs[0], []float64{0, 1, 2, 3, 4}}, Dataset{DSs[1], []float64{0, 2, 4, 6, 8}}}}

	stat, err := buck.GetStats(endTime.Add(-10*time.Second), endTime, buckStep)
	if err != nil {
		t.Error(err)
	}
	for i := range stat.Datasets {
		if stat.Datasets[i].Label != expected.Datasets[i].Label {
			t.Error("Wrong result")
		}
		for j := range stat.Datasets[i].Data {
			if stat.Datasets[i].Data[j] != expected.Datasets[i].Data[j] {
				t.Error("Wrong result")
			}
		}
	}

	//get scaled data
	expected = Stat{[]Dataset{Dataset{DSs[0], []float64{0, 0.5, 0.5, 1.5, 1.5, 2.5, 2.5, 3.5, 3.5}}, Dataset{DSs[1], []float64{0, 1, 1, 3, 3, 5, 5, 7, 7}}}}

	stat, err = buck.GetStats(endTime.Add(-10*time.Second), endTime, 1)
	if err != nil {
		t.Error(err)
	}
	for i := range stat.Datasets {
		if stat.Datasets[i].Label != expected.Datasets[i].Label {
			t.Error("Wrong result")
		}
		for j := range stat.Datasets[i].Data {
			if stat.Datasets[i].Data[j] != expected.Datasets[i].Data[j] {
				t.Error("Wrong result")
			}
		}
	}

}

func Test_load(t *testing.T) {
	//Create db and fill it with some data
	stor, err := NewStorage(testDB)
	defer func() { delDb() }()
	if err != nil {
		t.Error(err)
	}

	buckName := "testing"
	buckStep := 2
	buckCapacity := 5
	DSs := []string{"test1", "test2"}

	if _, err := stor.NewRRD(buckName, buckStep, buckCapacity, DSs...); err != nil {
		t.Error(err)
	}

	stor.Close()

	stor2, err := NewStorage(testDB)
	if err != nil {
		t.Error(err)
	}

	rrds, err := stor2.ListRRD()
	if err != nil {
		t.Error(err)
	}
	if rrds[0] != buckName {
		t.Error("Can't load db from file")
	}

}
