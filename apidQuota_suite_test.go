package apidQuota_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"testing"
)

func TestApidQuota(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ApidQuota Suite")
}
