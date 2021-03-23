package types

import "testing"

func TestTagsSet(t *testing.T) {
	tags := Tags{}
	// Should not panic about a nil map
	tags.Set("foo", "bar")
	// Should return the value
	if tags.Get("foo") != "bar" {
		t.FailNow()
	}
}
