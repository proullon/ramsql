package agnostic

import (
	"testing"

	"github.com/google/uuid"
)

func TestGenRandomUUID(t *testing.T) {

	t.Logf("Test GenRandomUUID")

	a := NewAttribute("id", "uuid").WithDefaultRandomUUID()

	value := a.defaultValue().(string)
	if err := uuid.Validate(value); err != nil {
		t.Errorf("Expected valid UUID from defaultRandomUUID: got %s", err)
	}

	value2 := a.defaultValue().(string)
	if value == value2 {
		t.Errorf("Expected 2 different UUIDs")
	}
}
