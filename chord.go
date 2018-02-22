package main

import(
	"log"
	"strings"
	"bufio"
	"os"
)

const (
	DEFAULT_HOST = "localhost"
	SUCC_SIZE = 3
)

//type my_port string
type Node struct {
	finger [161]string
	successor [SUCC_SIZE]string
	predecessor string
}

type handler func(*Feed)
type Server chan<- handler

func call(address string, method string, request interface{}, response interface{}){
	client, err := rpc.DialHTTP("tcp",address)
	if err != nil {
		log.Printf("rpc.DialHTTP: %v", err)
		return err
	}
	defer client.Close()

	if err = client.Call(method, request, respose); err != nil {
		log.Fatalf("client.Call %s: %v", method, err)
		return err
	}
	return nil
}

func parse_input(){
	//var my_message []string
	//parse the shell input
	var port string

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan(){
		line := scanner.Text()
		line = strings.TrimSpace(line)
		//break the command into a slice of strings
		parts := strings.SplitN(line, " ", 2)	
		//if more than one word has been typed
		if len(parts) > 1{
			parts[1] = strings.TrimSpace(parts[1])
		}

		//if nothing is types, that's fine
		if len(parts) == 0{
			continue
		}

		switch parts[0] {
			case "help":
				log.Println("commands are help, port, quit")

			case "port":
				port = parts[1]
				log.Println("port is",port)
			case "create":
				if port == "" {
					port = "3410"
				}
				address := DEFAULT_HOST + ":" + port

				log.Println("connected to ", port)
				serve(address)

			case "ping":
				port_out := parts[1]


			case "quit":
				os.Exit(2)

		}

	}
	return nil
}


func main(){
	
	parse_input()
	
}

func serve(address string){
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", address)
	if e != nil {
		log.Fatal("listen error: ", e)
	}

	if err := http.Serve(i, nil); err != nil {
		log.Fatalf("http.Serve: %v", err)
	}
}

