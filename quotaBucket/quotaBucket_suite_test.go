package quotaBucket_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"testing"
)

func TestQuotaBucket(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "QuotaBucket Suite")
}

//var _ = BeforeSuite(func() {
//	fmt.Println("before suite")
//
//})
//
//var _ = AfterSuite(func() {
//	fmt.Println("after suite")
//})
