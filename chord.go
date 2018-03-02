package main

import(
	"log"
	"strings"
	"bufio"
	"os"
	"net/rpc"
	"net"
	"net/http"
	"fmt"
	"math/big"
	"crypto/sha1"
	"time"
)

const (
	DEFAULT_HOST = "localhost"
	SUCC_SIZE = 3
)

type Nothing struct {}

//type my_port string
type Node struct {
	id *big.Int
	address string
	finger []string
	successor []string
	predecessor string
	bucket map[string]string
}

type handler func(*Node)
type Server chan<- handler

func hashString(elt string) *big.Int {
    hasher := sha1.New()
    hasher.Write([]byte(elt))
    return new(big.Int).SetBytes(hasher.Sum(nil))
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

		//log.Println(parts[0])

		//if nothing is types, that's fine
		if len(parts) == 0{
			continue
		}

		switch parts[0] {
			case "help":
				log.Println("commands are help, port, create, ping, get, put, delete, dump, quit")

			case "port":
				port = parts[1]
				log.Println("port is",port)
			case "create":
				if port == "" {
					port = "3410"
				}
				address := getLocalAddress() + ":" + port

				log.Println("connected to", getLocalAddress() + ":" + port)
				go func(){
					serve(address, "", port)
					}()	

			case "join":
				if port == "" {
					log.Println("Please specify a port")
					break
				}

				address := getLocalAddress() + ":" + port
				successor := parts[1]

				log.Println("connected to", getLocalAddress() + ":" + port)
				go func(){
					serve(address, successor, port)//the address to listen, my successor,
				}()


			case "ping":
				port_out := parts[1]
				address := getLocalAddress() + ":" + port_out
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
				address := getLocalAddress() + ":" + port_out
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
				address := getLocalAddress() + ":" + port_out
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
				address := getLocalAddress() + ":" + port_out
				var junk string
				log.Println("talkiing to:", port_out)
				if err := call(address, "Server.Delete", key, &junk); err != nil{
					log.Fatalf("calling Server.Delete: %v", err)
				}

			case "dump":
				
				var junk Nothing
				var reply string
				
				address := getLocalAddress() + ":" + port
				//log.Println("Node address:", port)
				
				if err:= call(address, "Server.Dump", &junk, &reply); err != nil{
					log.Fatalf("calling Server.Dump: %v", err)
				}
				//local_address := getLocalAddress()
				//log.Println("local address:",local_address)

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

func (s Server) Dump(junk *Nothing, reply *string) error{
	finished := make(chan struct{})
	s <- func(n *Node){

		//print the address
		log.Println("Address:")
		log.Println(n.address)

		//print the bucket
		log.Println("Bucket:")
		for key, value := range n.bucket {
			log.Println(key, value)
			
		}
		
		log.Println("Successors:")
		//print the successors
		for _, elt := range n.successor {
			if elt != ""{
				log.Println(elt)
			}
		}

		//s := fmt.Sprintf("%040x", n.id)
		log.Println("id: "+ "x"+toHexInt(n.id))
		finished <- struct{}{}

		}
	<-finished
	return nil
}


func (s Server) Stabilize(input *Nothing, reply *Nothing) error{
	finished := make(chan struct{})
	s <- func(n *Node){
		my_succ_address := n.successor[0]
		//log.Println("my address is,", address)
		log.Println("the address of my successor is", my_succ_address)
		var succ_pred string
		var junk Nothing
		if(my_succ_address != ""){
			if err := call(my_succ_address, "Server.GetPredecessor", &junk, &succ_pred); err != nil {
				log.Fatalf("calling Server.GetPredecessor: %v", err)	
			}

			if between(n.id,hashString(succ_pred),hashString(n.successor[0]), false){
				n.successor[0] = succ_pred
			}

			var junk string
			if err := call(my_succ_address, "Server.Notify", n.address, &junk); err != nil {
				log.Fatalf("calling Server.Notify: %v", err)
			}
		}

		finished <- struct{}{}
		
	}
	<-finished
	return nil
}



func (s Server) GetPredecessor(input *Nothing, reply *string) error {
	finished := make(chan struct{})
	s <- func(n *Node){
		*reply = n.predecessor
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) Notify(node_address string, reply *string) error{
	//this node thinks I am its successor
	finished := make(chan struct{})
	s <- func(n *Node){
		if n.predecessor == "" || between(hashString(n.predecessor), hashString(node_address), n.id, false) {
			n.predecessor = node_address
		}
		finished <- struct{}{}
	}
	<-finished
	return nil
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

func toHexInt(n *big.Int) string {
    s := fmt.Sprintf("%040x", n) // or %X or upper case
    return s[:8]
}

func between(start, elt, end *big.Int, inclusive bool) bool {
    if end.Cmp(start) > 0 {
        return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
    } else {
        return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
    }
}

func getLocalAddress() string {
    var localaddress string

    ifaces, err := net.Interfaces()
    if err != nil {
        panic("init: failed to find network interfaces")
    }

    // find the first non-loopback interface with an IP address
    for _, elt := range ifaces {
        if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
            addrs, err := elt.Addrs()
            if err != nil {
                panic("init: failed to get addresses for network interface")
            }

           for _, addr := range addrs {
                if ipnet, ok := addr.(*net.IPNet); ok {
                    if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
                        localaddress = ip4.String()
                        break
                    }
                }
            }
        }
    }
    if localaddress == "" {
        panic("init: failed to find non-loopback interface with valid address on this node")
    }

    return localaddress
}


func main(){
	
	parse_input()
	
}

func createNode(succ_port string, port string) *Node{
	address := getLocalAddress() + ":" + port
	finger := make([]string,161)
	successor := make([]string, SUCC_SIZE)

	if(succ_port != ""){
		succ := getLocalAddress() + ":" + succ_port
		//log.Println("my successor is!!!!", succ)
		//successor = append(successor, succ)
		for i, elt := range(successor){
			if elt == "" {
				successor[i] = succ
				break
			}
		}
	}

	//log.Println("successor[0]", successor[0])
	predecessor := ""
	id := hashString(address)
	bucket := make(map[string]string)
	return &Node {
		id,
		address,
		finger,
		successor,
		predecessor,
		bucket }
}

func startActor(successor string, port string) Server {
	ch := make(chan handler)
	state := createNode(successor, port)
	go func() {
		for f := range ch {
			f(state) 
		}
	}()
	return ch
}

func serve(address string, successor string, port string){
	
	actor := startActor(successor, port)

	log.Println("my_address", address)


	go func(){
		for{
			var junk Nothing
			if err := call(address, "Server.Stabilize", &junk, &junk); err != nil {
				log.Println("Calling Server.Stabilize: %v", err)
			}

			time.Sleep(2*time.Second)
		}

	}()
	
		
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

