package types

import "testing"

func TestTagsSet(t *testing.T) {
	task := Task{}
	// Should not panic about a nil map
	task.Tags.Set("foo", "bar")
	// Should return the value
	if task.Tags.Get("foo") != "bar" {
		t.FailNow()
	}
}
