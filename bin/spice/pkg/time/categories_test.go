package time

import (
	"testing"

	"github.com/spiceai/spiceai/bin/spice/pkg/testutils"
)

var snapshotter = testutils.NewSnapshotter("../../test/assets/snapshots/time/categories")

func TestGenerateTimeCategoryFieldsAll(t *testing.T) {
	fields := []string{CategoryDayOfMonth, CategoryMonth, CategoryDayOfWeek, CategoryHour}
	cols := GenerateTimeCategoryFields(fields...)

	for _, name := range fields {
		err := snapshotter.SnapshotMultiJson(name, cols)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestGenerateTimeCategoryFieldsMonth(t *testing.T) {
	cols := GenerateTimeCategoryFields(CategoryMonth)
	snapshotter.SnapshotTJson(t, cols)
}

func TestGenerateTimeCategoryFieldsDayOfMonth(t *testing.T) {
	cols := GenerateTimeCategoryFields(CategoryDayOfMonth)
	snapshotter.SnapshotTJson(t, cols)
}

func TestGenerateTimeCategoryFieldsDayOfWeek(t *testing.T) {
	cols := GenerateTimeCategoryFields(CategoryDayOfWeek)
	snapshotter.SnapshotTJson(t, cols)
}

func TestGenerateTimeCategoryFieldsHour(t *testing.T) {
	cols := GenerateTimeCategoryFields(CategoryHour)
	snapshotter.SnapshotTJson(t, cols)
}
