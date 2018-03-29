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
	//"math"
)

var is_joining bool

const (
	DEFAULT_HOST = "localhost"
	SUCC_SIZE = 3
)

type Nothing struct {}

var next = 0
const keySize = sha1.Size * 8
var two = big.NewInt(2)
var hashMod = new(big.Int).Exp(big.NewInt(2), big.NewInt(keySize), nil)

//type my_port string
type Node struct {
	id *big.Int
	address string
	finger []string
	successor string
	successors []string
	predecessor string
	bucket map[string]string
}

type handler func(*Node)
type Server chan<- handler

type FindSucc struct{
	Found bool
	Successor string
}

func hashString(elt string) *big.Int {
    hasher := sha1.New()
    hasher.Write([]byte(elt))
    return new(big.Int).SetBytes(hasher.Sum(nil))
}

//This computes the address of a position across the ring 
//that should be pointed to by the given finger table entry (using 1-based numbering).
func jump(address string, fingerentry int) *big.Int {
    n := hashString(address)
    fingerentryminus1 := big.NewInt(int64(fingerentry) - 1)
    jump := new(big.Int).Exp(two, fingerentryminus1, nil)
    sum := new(big.Int).Add(n, jump)

    return new(big.Int).Mod(sum, hashMod)
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

				is_joining = true
				log.Println("connected to", getLocalAddress() + ":" + port)
				go func(){
					serve(address, successor, port)//the address to listen, my successor,the port I am at
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
				//port_out := key_address[1]
				address := find(*hashString(key), getLocalAddress() + ":" + port)
				var value string
				//log.Println("talking to:", port_out)
				log.Println("I am asking", address, "about", key)
				if err := call(address, "Server.Get", key, &value); err != nil{
					log.Fatalf("calling Server.Get: %v", err)
				}
				log.Println(value)

			case "put":
				value_address := strings.SplitN(parts[1], " ", 3)
				key_value := value_address[0] + " " + value_address[1]
				//value := value_address[1] 
				//log.Println("it's making it this far")
				address := find(*hashString(value_address[0]), getLocalAddress() + ":" + port)
				//address := getLocalAddress() + ":" + port_out
				//log.Println(key_value)				
				//log.Println("talking to:", address)
				var junk string

				log.Println("the address that I am to call:", address)

				if err := call(address, "Server.Put", key_value, &junk); err != nil{
					log.Fatalf("calling Server.Put: %v", err)
				}

			case "delete":
				key_address := strings.SplitN(parts[1], " ", 2)
				key := key_address[0]
				port_out := key_address[1]
				address := getLocalAddress() + ":" + port_out
				var junk string
				log.Println("talking to:", port_out)
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
				//give my bucket to my successor
				address := getLocalAddress() + ":" + port

				var my_bucket map[string]string
				var my_successor string
				var junk Nothing

				log.Println("The address I'm about to connect to", address)
				call(address, "Server.Get_Bucket", &junk, &my_bucket)
				call(address, "Server.Get_Successor", &junk, &my_successor)

				for k,v := range my_bucket{
					log.Println(k, v)
				}

				log.Println("my successor is:", my_successor)
				call(my_successor, "Server.Put_All", &my_bucket, &junk)

				os.Exit(2)
		}
	}
}

func (s Server) Ping(input *Nothing, reply *string) error{
//	log.Println("PING")
	*reply = "hello"
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
		log.Println("Address:", n.address)
		//log.Println(n.address)

		log.Println("id:      "+ "x"+toHexInt(n.id))
		finished <- struct{}{}

		//print the bucket
		log.Println("Bucket:")
		for key, value := range n.bucket {
			log.Println("        ",key, value)			
		}
		
		log.Println("Successor:", n.successor)
		//print the successors
		log.Println("Successor 0:",n.successors[0])
		log.Println("Successor 1:",n.successors[1])
		log.Println("Successor 2:",n.successors[2])

		/*
		if n.successors[1] != "" {
			log.Println("Successor 2:", n.successors[1])
		}
		*/
		log.Println("Predecessor:", n.predecessor)
		log.Println("Fingers:")
		//reached := false
		for i := 160; i > 0; i-- {
			if i < 160{ 
				if n.finger[i] != "" && n.finger[i] != n.finger[i+1]{
					log.Println("        entry:",i,":",n.finger[i])
				}
			} else{
				log.Println("        entry:",i,":",n.finger[i])
			}
		}
		//s := fmt.Sprintf("%040x", n.id)		
		}
	<-finished
	return nil
}



func (s Server) Stabilize() error{
	finished := make(chan struct{})
	s <- func(n *Node){

		//If I just joined the ring, issue a get_all to my successor

		
			if is_joining {
				new_bucket := make(map[string]string)
				call(n.successor, "Server.Get_All", &n.address, &new_bucket)
				log.Println("sent:")
				for k,v := range new_bucket {
					log.Println(k,v)
					n.bucket[k] = v
				is_joining = false
			}
			is_joining = false
		}


		my_succ_address := n.successor
		//log.Println("my address is,", address)
		//log.Println("the address of my successor is", my_succ_address)
		var succ_pred string //my sucessor's predecessor; the predecessor of my successor
		var succ_succs []string //the successor list of my successor
		var junk Nothing 

		if my_succ_address != "" {//assuming I have a successor
			//get the predecessor of my successor
			if my_succ_address  != n.address {				
				var ping_reply string

				//log.Println("the address I am about to connect to", my_succ_address)
				call(my_succ_address, "Server.Ping", &junk, &ping_reply)
					//log.Fatalf("calling Server.Ping: %v", err)
				//ping_reply = "wuddup"			
				//log.Println("Ping results:", ping_reply)
				if ping_reply == "hello"{
					//rpc call to get my successor's predecessor
					call(my_succ_address, "Server.GetPredecessor", &junk, &succ_pred)
						//log.Fatalf("calling Server.GetPredecessor: %v", err)	
					
					//getting my successor's successor list
					call(my_succ_address, "Server.GetSuccessorList", &junk, &succ_succs)
						//log.Fatalf("calling Server.GetSuccessorList: %v", err)

					//shift everything up in my successor's successor list		
					succ_list_copy := succ_succs
					for i := SUCC_SIZE - 1; i > 0; i-- {
						if i != 0 {
							succ_list_copy[i] = succ_list_copy[i-1]
						}
					}
					succ_list_copy[0] = n.successor
					n.successors = succ_list_copy
					/*
				log.Println("the successor list of my successor:")
				for _,elt := range succ_succs {
					log.Println(elt)
				}
					*/
				} else { //if the ping to my successor failed					
					//shift my succesor list down
					for i := 0; i < SUCC_SIZE; i++{
						if i != SUCC_SIZE - 1 {
							n.successors[i] = n.successors[i+1]
						}
					}
					//if there are only a couple nodes, get rid of the stragglers at the end
					for i := 1; i < SUCC_SIZE; i++ {
						if n.successors[i] == n.successor{
							n.successors[i] = ""
						}else if n.successors[i] == my_succ_address{
							//if my successor that isn't there is later in my successor list, get rid of it
							n.successors[i] = "";
						}

					}
					//my new successor is whatever is next in my list
					n.successor = n.successors[0]


					//tell my new successor that it's predecessor is Dead
					if(n.successor != n.address){
						call(n.successor, "Server.ClearPredecessor", &junk, &junk)
					}

					//clear my predeceessor
					//n.predecessor = ""
					is_empty := true //for the special case of if I am the last node left in the ring
					//check if my successor list is empty
					for i :=0; i < SUCC_SIZE; i++ {
						if n.successors[i] != "" {
							is_empty = false
						}
					}
					if is_empty { 
						//log.Println("my successor list is empty")
						//if my successor list is empty, reset it so my first value is myself
						n.successors[0] = n.address
						//n.successor = n.address
						my_succ_address = n.address
						n.successor = n.address
						n.predecessor = ""
						log.Println("successor changed:", n.successor)
					}
				}
			}

			if n.successor == n.address {//if my successor is myself, then predecessor of my successor is my own predecessor
				succ_pred = n.predecessor
			}
			//if the predecessor of my successor is between me and my successor
			if succ_pred != "" && between(n.id,hashString(succ_pred),hashString(n.successor), false){
				//set my successor to the predecessor of what I thought was my successor
				n.successor = succ_pred
				log.Println("successor changed:", succ_pred)
				}
			}else{
				
			}
			//var junk string
			//notify my new successor that I believe I am it's predecessor
			//if my successor doesn't have a predecessor, and my successor is not myself, then notify my successor
			if succ_pred == "" {
				//if my successor does not have a predecessor 
				if n.successor != n.address {//notify my new successor that I am it's predecessor
					//log.Println("the address I am about to Notify", n.successor)
					call(n.successor, "Server.Notify", n.address, &junk)
						//log.Fatalf("calling Server.Notify: %v", err)	
				} else { //if my successor is myself, set my predecessor to myself?
					
				}
			//otherwise, if my successor does have a predecessor, and it isn't me, notify that address that I am its successor
			}else if succ_pred != n.address{
				call(n.successor, "Server.Notify", n.address, &junk)
					//log.Fatalf("calling Server.Notify: %v", err)
				
			} 

			
		
		//log.Println("Stabilized")
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

func (s Server) GetSuccessorList(input *Nothing, reply *[]string) error {
	finished := make(chan struct{})
	s <- func(n *Node){
		*reply = n.successors
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) Get_Bucket(input *Nothing, reply *map[string]string) error{
	finished := make(chan struct{})
	s <- func(n *Node){
		*reply = n.bucket
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) Get_Successor(input *Nothing, reply *string) error{
	finished := make(chan struct{})
	s <- func(n *Node){
		*reply = n.successor
		finished <- struct{}{}
	}
	<-finished
	return nil
}


func (s Server) Notify(node_address string, reply *Nothing) error{
	//this node thinks I am its successor; node_address thinks it is my predecessor
	finished := make(chan struct{})
	s <- func(n *Node){
		//if i do not yet have a predecessor, or node_address is between my predecessor and I, then set my predecessor to the new node
		if n.predecessor == "" || between(hashString(n.predecessor), hashString(node_address), n.id, false) {
			n.predecessor = node_address
			log.Println("predecessor changed:", node_address)
		}
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) ClearPredecessor(input *Nothing, reply *Nothing) error{
	finished := make(chan struct{})
	s <- func(n *Node){
		n.predecessor = "";
		finished <- struct{}{}
	}
	<-finished
	return nil
}

//ask node n to find the successor of id
//or a better node to continue the search with
func (s Server) Find_Successor(id *big.Int, reply *FindSucc) error{
	finished := make(chan struct{})
	s <- func(n *Node){
		var temp FindSucc
		if between(n.id, id, hashString(n.successor), true) {
			temp.Found = true
			temp.Successor = n.successor
		} else {
			temp.Found = false
			cpn := n.closest_preceding_node(*id)
			temp.Successor = cpn
		}
		*reply = temp
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) Put_All(input *map[string]string, reply *Nothing) error{
	finished := make(chan struct{})
	s <- func(n *Node){
		for k, v := range *input {
			//if the key is between me and my predecessor
			log.Println("the bucket that I am recieving:")
			for k, v := range(*input){
				log.Println(k, v)
			}

			
			n.bucket[k] = v
			
		}
		finished <- struct{}{}
	}
	<-finished
	return nil
}

func (s Server) Get_All(input *string, reply *map[string]string) error{
	finished := make(chan struct{})
	s <- func(n *Node){
		//input is an address of a node that is between my and my predecessor

		my_predecessor := n.predecessor

		temp := make(map[string]string)
		if n.predecessor == ""{
		//	my_predecessor = n.address
		}
		//log.Println("my pred",n.predecessor)
		//loop over my bucket
		for k, v := range n.bucket{
			if between(hashString(my_predecessor), hashString(k), hashString(*input), false){
			//if find(*hashString(k), n.successor) != n.address{//if this key shouldn't be here
				//if the key belongs to the new node
				temp[k] = v;
				delete(n.bucket, k)
			}
		}
		*reply = temp

		finished <- struct{}{}
	}
	<-finished
	return nil
}


func (n *Node) find_successor(id *big.Int) *FindSucc {
	var temp FindSucc
	//log.Println("my id is", toHexInt(n.id))
	//if the id being passed in is between me and my successor
	if between(n.id, id, hashString(n.successor), true) {
		temp.Found = true
		temp.Successor = n.successor
	}else {
		temp.Found = false
		cpn := n.closest_preceding_node(*id)
		//log.Println("the closest preceding node is ", cpn)
		temp.Successor = cpn
	}
	return &temp
}


func (s Server) Fix_Fingers() error {
	finished := make(chan struct{})
	s <- func(n *Node){
		for next := 1; next <= 160; next++ {
			var succ FindSucc

			succ = *n.find_successor(jump(n.address, next))
			if succ.Found{
				n.finger[next] =  succ.Successor
			}

			if next == 160 {
				//log.Println("my address:", n.address, "the successor that I found", succ.Successor)
			}
		}
		finished <- struct{}{}
	}
	<-finished
	return nil
}

//search the local table for the highest predecessor if id
func (n Node) closest_preceding_node(id big.Int) string{              
    for i := 160; i > 0; i -= 1 {
    	if between(n.id, hashString(n.finger[i]), &id, true) {
    		//log.Println("I am using the finger table")
    		return n.finger[i]
    	}
    }
	return n.successor
}

//find the successor if id
func find(id big.Int, start string) string {
	found := false
	nextNode := start
	maxSteps := 10
	i := 0
	var succ FindSucc
	for !found && i < maxSteps {
		if err := call(nextNode, "Server.Find_Successor", &id, &succ); err != nil{
			//log.Fatalf("calling Server.Find_Successor: %v", err)
		}

		found = succ.Found
		nextNode = succ.Successor
		//log.Println("succ.Successor", succ.Successor)
		i += 1
	}
	if found {
		return nextNode
	}
	return "did not find"
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

func createNode(succ_address string, address string) *Node{
	//address := getLocalAddress() + ":" + port
	finger := make([]string,162)
	successors := make([]string,SUCC_SIZE)
	var successor string

	if succ_address == "" {
		//if I am creating a ring, rather than joing
		//and I am not passing in a successor, make my successor myself
		succ_address = address
	}

	if(succ_address != ""){
		succ := succ_address
		successor = succ
		successors[0] = succ
	}

	predecessor := ""
	id := hashString(address)
	bucket := make(map[string]string)
	return &Node {
		id,
		address,
		finger,
		successor,
		successors,
		predecessor,
		bucket }
}

func startActor(successor string, address string) Server {
	ch := make(chan handler)
	state := createNode(successor, address)
	go func() {
		for f := range ch {
			f(state) 
		}
	}()
	return ch
}

func serve(address string, successor string, port string){
	
	actor := startActor(successor, address)

	log.Println("my_address", address)

	go func(){
		for{
			actor.Stabilize()
			actor.Fix_Fingers()

			time.Sleep(time.Second)
		}

	}()
		
	//my_server := new(Server)
	rpc.Register(actor)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", address)
	if e != nil {
		//log.Fatal("listen error: ", e)
	}

	if err := http.Serve(l, nil); err != nil {
		//log.Fatalf("http.Serve: %v", err)
	}
}

