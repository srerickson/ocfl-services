package utils

import (
	"fmt"
	"iter"
	"strings"
	"time"
)

func FileSize(byteSize int64) string {
	var units = []string{"Bytes", "KB", "MB", "GB", "TB"}
	scaled := float64(byteSize)
	unit := ""
	for _, u := range units {
		unit = u
		if scaled < 1000 {
			break
		}
		scaled = scaled / 1000
	}
	if unit == "Bytes" {
		return fmt.Sprintf("%d %s", int64(scaled), unit)
	}
	return fmt.Sprintf("%0.2f %s", scaled, unit)
}

func FormatDate(t time.Time) string {
	return t.Format(time.DateOnly)
}

func ShortDigest(digest string) string {
	if len(digest) > 8 {
		return digest[0:8]
	}
	return digest
}

func RelativeDate(t time.Time) string {
	now := time.Now()
	duration := now.Sub(t)

	if duration < time.Minute {
		return "now"
	}

	minutes := int(duration.Minutes())
	if minutes < 60 {
		if minutes == 1 {
			return "1 min ago"
		}
		return fmt.Sprintf("%d mins ago", minutes)
	}

	hours := int(duration.Hours())
	if hours < 24 {
		if hours == 1 {
			return "1 hour ago"
		}
		return fmt.Sprintf("%d hours ago", hours)
	}

	days := int(duration.Hours() / 24)
	if days < 30 {
		if days == 1 {
			return "yesterday"
		}
		return fmt.Sprintf("%d days ago", days)
	}

	months := days / 30
	if months < 12 {
		if months == 1 {
			return "a month ago"
		}
		return fmt.Sprintf("%d months ago", months)
	}

	years := months / 12
	if years == 1 {
		return "a year ago"
	}
	return fmt.Sprintf("%d years ago", years)
}

func Breadcrumb(currentPath string) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		if currentPath == "." || currentPath == "" {
			return
		}
		parts := strings.Split(strings.Trim(currentPath, "/"), "/")
		for i, part := range parts {
			if part == "" {
				continue
			}
			segmentPath := strings.Join(parts[:i+1], "/")
			if !yield(part, segmentPath) {
				return
			}

		}
	}
}
