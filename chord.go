package main

import(
	"log"
	"strings"
	"bufio"
	"os"
	"net/rpc"
	"net"
	"net/http"

)

const (
	DEFAULT_HOST = "localhost"
	SUCC_SIZE = 3
)

type Nothing struct {}

//type my_port string
type Node struct {
	finger []string
	successor []string
	predecessor string
	bucket map[string]string
}

type handler func(*Node)
type Server chan<- handler


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

		//log.Println(parts[0])

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
				go func(){
					serve(address)
					}()	

			case "ping":
				port_out := parts[1]
				address := DEFAULT_HOST + ":" + port_out
				var message string
				var junk Nothing
				log.Println("talking to:",port_out)
				if err := call(address, "Server.Ping", &junk, &message); err != nil{
					log.Fatalf("calling Server.List: %v", err)
				}

			case "get":
				key_address := strings.SplitN(parts[1], " ", 2)
				key := key_address[0]
				port_out := key_address[1]
				address := DEFAULT_HOST + ":" + port_out
				var value string
				log.Println("talking to:", port_out)
				if err := call(address, "Server.Get", key, &value); err != nil{
					log.Fatalf("calling Server.Get: %v", err)
				}
				log.Println(value)

			case "put":
				value_address := strings.SplitN(parts[1], " ", 3)
				key_value := value_address[0] + " " + value_address[1]
				//value := value_address[1]
				port_out := value_address[2]
				address := DEFAULT_HOST + ":" + port_out
				//log.Println(key_value)				
				log.Println("talking to:", port_out)
				var junk string
				if err := call(address, "Server.Put", key_value, &junk); err != nil{
					log.Fatalf("calling Server.Put: %v", err)
				}

			case "delete":
				key_address := strings.SplitN(parts[1], " ", 2)
				key := key_address[0]
				port_out := key_address[1]
				address := DEFAULT_HOST + ":" + port_out
				var junk string
				log.Println("talkiing to:", port_out)
				if err := call(address, "Server.Delete", key, &junk); err != nil{
					log.Fatalf("calling Server.Delete: %v", err)
				}


			case "quit":
				os.Exit(2)

		}

	}
}

func (s Server) Ping(input *Nothing, reply *string) error{
	log.Println("PING")
	return nil
}

func (s Server) Get(key string, value *string) error{
	finished := make(chan struct{})
	s <- func(n *Node){
		var response string
		response = n.bucket[key]
		*value = response
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) Put(key_value string, reply *string) error{
	finished := make(chan struct{})
	//log.Println("Am I making it this far?")
	s <- func(n *Node){
		//parse the message into its key and value
		message := strings.SplitN(key_value, " ", 2)
		key := message[0]
		value := message[1]
		n.bucket[key] = value
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) Delete(key string, reply *string) error{
	finished := make(chan struct{})
	s <- func(n *Node){
		delete(n.bucket, key)
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func createNode() *Node{

	finger := make([]string,161)
	successor := make([]string, SUCC_SIZE)
	predecessor := "pre"
	bucket := make(map[string]string)
	return &Node {
		finger,
		successor,
		predecessor,
		bucket}
}

func startActor() Server {
	ch := make(chan handler)
	state := createNode()
	go func() {
		for f := range ch {
			f(state) 
		}
	}()
	return ch
}

func call(address string, method string, request interface{}, response interface{}) error{
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Printf("rpc.DialHTTP: %v", err)
		return err
	}
	defer client.Close()

	if err = client.Call(method, request, response); err != nil {
		log.Fatalf("client.Call %s: %v", method, err)
		return err
	}
	return nil
}


func main(){
	
	parse_input()
	
}

func serve(address string){
	actor := startActor()
	//my_server := new(Server)
	rpc.Register(actor)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", address)
	if e != nil {
		log.Fatal("listen error: ", e)
	}

	if err := http.Serve(l, nil); err != nil {
		log.Fatalf("http.Serve: %v", err)
	}
}

