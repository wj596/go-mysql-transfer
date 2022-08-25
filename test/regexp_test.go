package test

import (
	"regexp"
	"testing"
)

func TestQuoteMeta(t *testing.T) {
	p := "**"
	println(regexp.MatchString(p, "act_de_databasechangelog"))
	println(regexp.MatchString("/*", "act_de_databasechangelog"))

}