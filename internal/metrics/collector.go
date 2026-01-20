package metrics

import "log"

func CollectFileMetrics(in <-chan FileMetric, done chan<- struct{}) {
	for m := range in {
		log.Println("======================================")
		log.Println("FILE METRICS")
		log.Printf("File        : %s\n", m.FileName)
		log.Printf("Status      : %s\n", m.Status)
		log.Printf("Lines       : %d\n", m.TotalLines)
		log.Printf("Parsed Rows : %d\n", m.ParsedRows)
		log.Printf("Errors      : %d\n", m.ErrorCount)
		log.Printf("Duration    : %s\n", m.Duration)
		log.Println("======================================")
	}
	close(done)
}
