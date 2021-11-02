package time

import (
	"fmt"
	"time"
)

const (
	CategoryDayOfYear = "yearday"
	CategoryMonth = "month"
	CategoryDayOfMonth = "dayofmonth"
	CategoryDayOfWeek = "dayofweek"
	CategoryHour = "hour"
)

type TimeCategoryInfo struct {
	FieldName string
	Value int
}

func GenerateTimeCategoryFields(timeCategories ...string) map[string][]TimeCategoryInfo {
	timeCategoryColumns := make(map[string][]TimeCategoryInfo)
	for _, timeCategory := range timeCategories {
		cols := []TimeCategoryInfo{}
		switch timeCategory {
			case CategoryDayOfYear:
				for i := 1; i <= 366; i++ {
					cols = append(cols, TimeCategoryInfo{
						FieldName: fmt.Sprintf("time_yearday_%03d", i),
						Value: int(i),
					})
				}
			case CategoryMonth:
				for i := time.January; i <= time.December; i++ {
					cols = append(cols, TimeCategoryInfo{
						FieldName: fmt.Sprintf("time_month_%02d", i),
						Value: int(i),
					})
				}
			case CategoryDayOfMonth:
				for i := 1; i <= 31; i++ {
					cols = append(cols, TimeCategoryInfo{
						FieldName: fmt.Sprintf("time_monthday_%02d", i),
						Value: i,
					})
				}
			case CategoryDayOfWeek:
				for i := time.Sunday; i <= time.Saturday; i++ {
					cols = append(cols, TimeCategoryInfo{
						FieldName: fmt.Sprintf("time_weekday_%02d", i),
						Value: int(i),
					})
				}
			case CategoryHour:
				for i := 0; i < 24; i++ {
					cols = append(cols, TimeCategoryInfo{
						FieldName: fmt.Sprintf("time_hour_%02d", i),
						Value: i,
					})
				}
		}
		timeCategoryColumns[timeCategory] = cols
	}

	return timeCategoryColumns
}