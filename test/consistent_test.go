package test

import (
	"fmt"
	"log"
	"stathat.com/c/consistent"
	"testing"
)

func checkNum(num, expected int, t *testing.T) {
	if num != expected {
		t.Errorf("got %d, expected %d", num, expected)
	}
}

func TestNew(t *testing.T) {
	x := consistent.New()
	if x == nil {
		t.Errorf("expected obj")
	}
	checkNum(x.NumberOfReplicas, 20, t)
}

func TestAdd(t *testing.T) {
	c := consistent.New()
	c.NumberOfReplicas = 30
	c.Add("127.0.0.1")
	//c.Add("127.0.0.2")
	//c.Add("127.0.0.3")
	users := []string{"9919090403694849", "9919090403694850", "9919090403694851", "9919090403694852", "9919090403694853"}
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server)
	}
	fmt.Println()
	fmt.Println()
	c.Add("127.0.0.2")
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server)
	}

	fmt.Println()
	fmt.Println()
	c.Add("127.0.0.3")
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server)
	}

	fmt.Println()
	fmt.Println()
	c.Remove("127.0.0.2")
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server)
	}

}
