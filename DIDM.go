package main

import (
	"fmt"
	"time"
	"sync"
	"math/rand"
	"math"
	"strconv"
	"net/http"
	"net/url"
	"os"
	"io/ioutil"
	"io"
	"path"
	"github.com/Prasanth-G/splitdownload"
	"encoding/json"
	"regexp"
	"strings"
)

type bytesrange struct{
	order int
	start int64
	end int64
}

type job struct{
	workerid int
	work bytesrange
	worker string
}

type master struct{
	neighbourDevices map[int]string
	url string
}

func AskPermission(addr string, url string, byterange string) bool{
	
	fmt.Print(addr, " Device want to download ", url, byterange, "\t : ")
	var input string
	fmt.Scanln(&input)
	if input == "yes" || input == "y"{
		return true
	}else{
		return false
	}
}

func Handler(writer http.ResponseWriter, request *http.Request){
	
	switch request.Method{
		case "POST":
			var dict map[string]string
			data, _ := ioutil.ReadAll(request.Body)
			json.Unmarshal(data, &dict)
			fmt.Println(dict)
			if _, ok := dict["NoOfParts"]; ! ok{
				dict["NoOfParts"] = "8"
			}
			if _, ok := dict["Url"]; !ok{
				writer.WriteHeader(400)
				writer.Write([]byte("Request Body must contain an Url"))
			}
			if _, ok := dict["Range"]; !ok{
				writer.WriteHeader(400)
				writer.Write([]byte("Request Body must contain a Range"))
			}
			/*if ! AskPermission(request.RemoteAddr, dict["Url"], dict["Range"]){
				writer.WriteHeader(400)
				writer.Write([]byte("<h1>Host Machine Rejected the Request</h1>"))
				break
			}*/
			re, _ := regexp.Compile("[0-9]+")
			a := re.FindAllString(dict["Range"], -1)
			start, _ := strconv.ParseInt(a[0], 10, 64)
			end, _ := strconv.ParseInt(a[1], 10, 64)
			parts64, _ := strconv.ParseInt(dict["NoOfParts"],10,64)
			i := splitdownload.SDR{int(parts64), dict["Url"]}
			i.PartialDownload([2]int64{start,end}, "file", "")
			out, _ := ioutil.ReadFile("file")
			writer.WriteHeader(200)
			writer.Write(out)

		case "GET":
			s := strings.Split(request.URL.Path, "/")
			url := strings.Join(s[3:],"/")
			url = strings.Join([]string{s[2],url}, "//")
			q := request.URL.RawQuery
			/*if ! AskPermission(request.RemoteAddr, url, q){
				writer.WriteHeader(400)
				writer.Write([]byte("<h1>Host Machine Rejected the Request</h1>"))
				break
			}*/
			i := splitdownload.SDR{8, url}
			if q == ""{
				fmt.Println("Complete download")
				i.CompleteDownload("file", "")
			}else{
				re, _ := regexp.Compile("[0-9]+")
				a := re.FindAllString(q, -1)
				start, _ := strconv.ParseInt(a[0], 10, 64)
				end, _ := strconv.ParseInt(a[1], 10, 64)
				//fmt.Println("Partial Download")
				i.PartialDownload([2]int64{start,end},"file","")
			}
			out, _ := ioutil.ReadFile("file")
			writer.WriteHeader(200)
			writer.Write(out)
	}
}

func (m *master)worker(jobsChannel <-chan job, completed chan<- int) {
	slave := &http.Client{}
	for j := range jobsChannel {
		fmt.Println("worker", j.workerid, "started  job", j.work)
		rangeString := "bytes="+ strconv.FormatInt(j.work.start, 10) + "-" + strconv.FormatInt(j.work.end, 10)
		request, err := http.NewRequest("GET","http://"+ j.worker +":8000/slave/" + m.url + "?" + rangeString, nil)
		if err != nil{
			fmt.Println(err)
		}

		response, err := slave.Do(request)
		if err != nil{
			fmt.Println(err)
		}

		final, err := os.OpenFile("Part_"+strconv.Itoa(j.work.order), os.O_CREATE | os.O_TRUNC | os.O_WRONLY, 0600)
		if err != nil{
			fmt.Println(err)
		}
		data, err := ioutil.ReadAll(response.Body)
		if err != nil{
			fmt.Println(err)
		}
		final.Write(data)
		final.Close()
		//add device back to queue
		m.neighbourDevices[j.workerid] = j.worker
		completed <- j.work.order
	}
}

func (m *master)Start(noOfParts int, saveto string, saveas string) {

	rand.Seed(time.Now().UTC().UnixNano())
	response, err := http.Head(m.url)
	if err != nil{
		fmt.Println(err)
	}

	var mutex = &sync.Mutex{}
	jobsChannel := make(chan job, 32)
	completed := make(chan int, 32)
	
	//divide the file into 8 parts; update - split file depends on DOWNLOAD SPEED, and NUMBER OF NEIGHBOUR DEVICE
	chunkSize := int64(math.Ceil(float64(response.ContentLength) / float64(noOfParts)))
	NoOfWorkers := len(m.neighbourDevices)

	for w := 0; w < NoOfWorkers; w++ {
		go m.worker(jobsChannel, completed)
	}
	var jobs = make([]bytesrange, 8)
	var start int64
	index := 0
	for start = 0; start < response.ContentLength; start += chunkSize{
		end := start + chunkSize - 1
		if end > response.ContentLength + 1{
			end = response.ContentLength
		}
		jobs[index] = bytesrange{index, start, end}
		index++
	}
	for _, j := range jobs{
		mutex.Lock()
		randNumber := 0
		if len(m.neighbourDevices) != 0{
			randNumber = rand.Intn(len(m.neighbourDevices))
		}
		for _, ok := m.neighbourDevices[randNumber]; !ok ;{
			randNumber++
			if randNumber > NoOfWorkers{
				randNumber = 0
			}
			_, ok = m.neighbourDevices[randNumber]
		}
		jobsChannel <- job{randNumber, j , m.neighbourDevices[randNumber]}
		delete(m.neighbourDevices, randNumber)
		mutex.Unlock()
	}
	for i := 0; i < noOfParts; i++{
		fmt.Println(<-completed)
	}
	defer close(jobsChannel)

	final, err := os.OpenFile(path.Join(saveto,saveas), os.O_CREATE | os.O_APPEND | os.O_WRONLY, 0600)
	if err != nil{
		fmt.Println("ERROR OPENING FINAL FILE")
	}
	defer final.Close()
	for i:= 0;i < noOfParts; i++{
		file := "Part_"+strconv.Itoa(i)
		out, _ := ioutil.ReadFile(file)
		defer os.Remove(file)
		final.Write(out)
	}
}

func hasRangeSupport(url string, splittable chan <- bool){
	client := &http.Client{}
	request, err := http.NewRequest("HEAD", url, nil)
	if err != nil{
		splittable <- false
	}
	response, err := client.Do(request)
	if err != nil{
		splittable <- false
	}
	if response.Header.Get("Accept-Ranges") == "bytes"{
		request.Method = "GET"
		request.Header.Set("Range", "bytes="+strconv.FormatInt(0, 10)+"-"+strconv.FormatInt(1, 10))
		response, err = client.Do(request)
		switch{
		case err != nil:
			splittable <- false
		case response.ContentLength != 2:
			splittable <- false
		}
		splittable <- true
	}
	splittable <- false
}

func main(){

	http.HandleFunc("/slave/", Handler)
	go http.ListenAndServe(":8000", nil)
	
	var downloadLink string
	fmt.Print("Download Link :\t")
	fmt.Scanln(&downloadLink)

	splittable := make(chan bool, 1)
	go hasRangeSupport(downloadLink, splittable)

	fmt.Println("Enter ip of your neighbouring devices in same network (eg : 127.0.0.1)\n(press ENTER to skip entering)")
	neighbourDevices := make(map[int]string)
	m := master{neighbourDevices, downloadLink}
	var neighbour string
	for i := 0; true; i++{
		fmt.Print(i, " : ")
		fmt.Scanln(&neighbour)
		fmt.Println(neighbour)
		if neighbour == ""{
			break
		}
		m.neighbourDevices[i] = neighbour
		neighbour = ""
	}
	parsedurl, _ := url.Parse(downloadLink)
	s := strings.Split(parsedurl.Path, "/")
	filename := s[len(s)-1]
	fmt.Println(filename)

	fmt.Println("Start Download ?")
	var input string
	fmt.Scanln(&input)
	
	if <-splittable{
		if len(neighbourDevices) == 0{
			i := splitdownload.SDR{8, downloadLink}
			fmt.Println("Downloading ... ")
			i.CompleteDownload(filename, "")
		}else{
			m.Start(8, ".", filename)
		}
	}else{
		file, err := os.Create(filename)
		if err != nil{
			fmt.Println("Error while creating file")
		}
		defer file.Close()
		response, err := http.Get(downloadLink)
		if err != nil{
			fmt.Println("Error downloading")
		}
		defer response.Body.Close()
		io.Copy(file, response.Body)
	}
	
	//"https://maggiemcneill.files.wordpress.com/2012/04/the-complete-sherlock-holmes.pdf"
}