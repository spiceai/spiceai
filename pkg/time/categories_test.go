package time

import (
	"testing"

	"github.com/spiceai/spiceai/pkg/testutils"
)

var snapshotter = testutils.NewSnapshotter("../../test/assets/snapshots/time/categories")

func TestGenerateTimeCategoryFieldsAll(t *testing.T) {
	fields := []string { CategoryDayOfYear, CategoryDayOfMonth, CategoryMonth, CategoryDayOfWeek, CategoryHour }
	cols := GenerateTimeCategoryFields(fields...)
	snapshotter.SnapshotTJson(t, cols)
}

func TestGenerateTimeCategoryFieldsDayOfYear(t *testing.T) {
	cols := GenerateTimeCategoryFields(CategoryDayOfYear)
	snapshotter.SnapshotTJson(t, cols)
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