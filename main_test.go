package bramble

import (
	"io/ioutil"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestMain(m *testing.M) {
	if os.Getenv("ENABLE_TEST_LOGS") == "" {
		log.SetOutput(ioutil.Discard)
	}
	os.Exit(m.Run())
}
