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

		log.Println("Predecessor:", n.predecessor)

		log.Println("Fingers:")
		//reached := false
		for i := 161; i > 0; i-- {

			/*
			if elt != "" {
				if i == 1{
					log.Println("        entry:",i, ":", n.finger[i])
				}
				if i > 1 && n.finger[i] != n.finger[i-1] {
					log.Println("        entry:", i, ":", n.finger[i])
				}
			}
			if i > 0 && !reached && elt == "" {
				reached = true
				log.Println("        up to ",i-1)
			}
			*/


			if i < 161{ 
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
		my_succ_address := n.successor
		//log.Println("my address is,", address)
		//log.Println("the address of my successor is", my_succ_address)
		var succ_pred string //my sucessor's predecessor
		var junk Nothing 

		if my_succ_address != "" {//assuming I have a successor
			//get the predecessor of my successor
			if my_succ_address  != n.address {
				
				if err := call(my_succ_address, "Server.GetPredecessor", &junk, &succ_pred); err != nil {
					log.Fatalf("calling Server.GetPredecessor: %v", err)	
				}
				
			} else {
				succ_pred = n.predecessor
			}
			//if the predecessor of my successor is between me and my successor
			//log.Println("n.id", n.id, "hashString(succ_pred)", hashString(succ_pred), "hashString(n.successor[0])",hashString(n.successor[0]))
			if succ_pred != "" && between(n.id,hashString(succ_pred),hashString(n.successor), false){
				//set my successor to the predecessor of what I thought was my successor
				n.successor = succ_pred
				log.Println("successor changed:", succ_pred)
			}else{
				//still tell my successor about me
				//my succ_pred is just my successor
			}
			var junk string
			//notify my new successor that I believe I am it's predecessor
			//if my successor doesn't have a predecessor, and my successor is not myself, then notify my successor
			if succ_pred == "" {
				//if my successor does not have a predecessor 
				if my_succ_address != n.address {
					if err := call(my_succ_address, "Server.Notify", n.address, &junk); err != nil {
						log.Fatalf("calling Server.Notify: %v", err)
					}
				} else { //if my successor is myself, set my predecessor to myself?
					//log.Println("idk")
				}
			//otherwise, if my successor does have a predecessor, and it isn't me, notify that address that I am its successor
			}else if succ_pred != n.address{
				if err := call(n.successor, "Server.Notify", n.address, &junk); err != nil {
					log.Fatalf("calling Server.Notify: %v", err)
				}
			} 
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

func (s Server) Notify(node_address string, reply *string) error{
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

//ask node n to find the successor of id
//or a better node to continue the search with
func (s Server) Find_Successor(id *big.Int, reply *FindSucc) error{
	finished := make(chan struct{})
	s <- func(n *Node){
		//log.Println("Am I making it this far?")
		var temp FindSucc
		if between(n.id, id, hashString(n.successor), true) {
			//log.Println("Am I making it this far?")
			temp.Found = true
			temp.Successor = n.successor
		} else {
			temp.Found = false
			cpn := n.closest_preceding_node(*id)
			temp.Successor = cpn
		}
		*reply = temp

		finished <- struct{}{}
		//log.Println("Am I making it this far?")
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

/*
next = next + 1 ; if (next > m)
next = 1;
finger[next] = find successor(n + 2next−1 );
*/


/*
func (s Server) Fix_Fingers() error {
	finished := make(chan struct{})
	s <- func(n *Node){
		found := false
		next = next + 1
		if next > 161 {
			next = 1
		}	
		next_id := jump(n.address, next)
		var succ FindSucc
		for !found {
			succ = *n.find_successor(next_id)
			found = succ.Found
			//log.Println("Am I getting stuck in an infinite while loop?")
			if !succ.Found {
				next_id = hashString(succ.Successor)
				//log.Println("the next_id is", next_id)
			}
		}	
		n.finger[next] = succ.Successor 
		log.Println("finger", next, "updated")
		finished <- struct{}{}
	}
	<-finished
	return nil
}
*/

func (s Server) Fix_Fingers() error {

	finished := make(chan struct{})
	s <- func(n *Node){
		//log.Println("fix fingers is being called")
/*
		next = next + 1
		if next > 161 {
			next = 1
		}
*/
		for next := 1; next <= 161; next++ {
			var succ FindSucc
			succ = *n.find_successor(jump(n.address, next))
			//found := succ.Found


			n.finger[next] =  succ.Successor
		}

		finished <- struct{}{}
	}
	<-finished
	return nil
}

//search the local table for the highest predecessor if id
func (n Node) closest_preceding_node(id big.Int) string{
//psuedo-code
/*
	for i = m downto 1
            if (finger[i] ∈ (n,id])
                return finger[i];
*/
               
    for i := 161; i > 0; i -= 1 {
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
			log.Fatalf("calling Server.Find_Successor: %v", err)
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

/*
func (n Node) find_successor(id *big.Int) (bool, string) {
	//var temp FindSucc
	if between(n.id, id, hashString(n.successor[0]), true) {
		//log.Println("Am I making it this far?")
		//temp.Found = true
		//temp.Successor = n.successor[0]
		return true, n.successor[0]
	} else {
		//temp.Found = false
		cpn := n.closest_preceding_node(*id)
		//temp.Successor = cpn
		return false, cpn
	}
}
*/

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
	var successor string

	if succ_address == "" {
		//if I am creating a ring, rather than joing
		//and I am not passing in a successor, make my successor myself
		succ_address = address
	}

	if(succ_address != ""){
		succ := succ_address
		//log.Println("my successor is!!!!", succ)
		//successor = append(successor, succ)
		/*
		for i, elt := range(successor){
			if elt == "" {
				successor = succ
				break
			}
		}
		*/

		successor = succ
	}

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
			//var junk Nothing
			actor.Stabilize()

			/*
			if err := call(address, "Server.Fix_Fingers", &junk, &junk); err != nil {
				log.Println("Calling Server.Fix_Fingers: %v", err)
			}
			*/
			actor.Fix_Fingers()

			time.Sleep(time.Second)
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

