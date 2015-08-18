package boltrrd_test

import (
	"fmt"
	"os"
	"time"

	"github.com/geekbros/SHM-Backend/boltrrd"
)

func ExampleNewStorage() {
	// Structure of storage:
	// file_bolt.db
	// |-bucket
	// | |-entry_1
	// | | |-data_storage1 - value
	// | | |-data_storage2 - value
	// | |-entry_2
	// | | |-data_storage1 - value
	// | | |-data_storage2 - value
	// | |-info_entry

	//Create bolt db file
	stor, err := boltrrd.NewStorage("bolt.db")
	defer func() {
		stor.Close()
	}()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	//Create new rrd storage-bucket
	buckName := "Data"              //Name of bucket
	buckStep := 1                   //Step in seconds
	buckCapacity := 10              //Number of steps which can store in bucket
	DSs := []string{"Val1", "Val2"} //Data storages

	stor.NewRRD(buckName, buckStep, buckCapacity, DSs...)
	stor.NewRRD("Network", 10, 6, "in", "out")

	//Save data to storage
	for i := 0; i < 10; i++ {
		err := stor.RRD(buckName).Save(float64(i), float64(i*2)) // Number of parameters for .save() must be equal to number of data storages
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		time.Sleep(time.Duration(buckStep) * time.Second) // Every entry saves with parameter time.Now() so wait a second
	}

	//Get saved data
	stat, err := stor.RRD(buckName).GetStats(time.Now().Add(-10*time.Second), time.Now(), buckStep)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println(stat)

	//Output: &{[{Val1 [0 1 2 3 4 5 6 7 8 9]} {Val2 [0 2 4 6 8 10 12 14 16 18]}]}
}
