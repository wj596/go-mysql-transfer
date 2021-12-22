package ss

import "strconv"

type Person struct {
	Name string
	Age  int
}

var Persons = make(map[string]*Person)

func Init() {
	for i := 0; i < 100; i++ {
		Persons[strconv.Itoa(i)] = &Person{
			Name: "wajjjj" + strconv.Itoa(i),
			Age:  i,
		}
	}
}

func Get(key string) (*Person, bool) {
	v, ok := Persons[key]
	return v, ok
}
