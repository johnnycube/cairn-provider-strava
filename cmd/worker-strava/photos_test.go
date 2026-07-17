package main

import "testing"

func TestSelectPhoto(t *testing.T) {
	t.Run("largest numeric size wins, dims from sizes", func(t *testing.T) {
		url, w, h := selectPhoto(
			map[string]string{"256": "u256", "2048": "u2048", "1024": "u1024"},
			map[string][]int{"2048": {2048, 1536}, "256": {256, 192}},
		)
		if url != "u2048" || w != 2048 || h != 1536 {
			t.Fatalf("got (%q,%d,%d); want (u2048,2048,1536)", url, w, h)
		}
	})

	t.Run("missing dims → zero", func(t *testing.T) {
		url, w, h := selectPhoto(map[string]string{"600": "u600"}, nil)
		if url != "u600" || w != 0 || h != 0 {
			t.Fatalf("got (%q,%d,%d); want (u600,0,0)", url, w, h)
		}
	})

	t.Run("empty urls → empty", func(t *testing.T) {
		if url, _, _ := selectPhoto(map[string]string{}, nil); url != "" {
			t.Fatalf("got %q; want empty", url)
		}
	})

	t.Run("blank url values skipped", func(t *testing.T) {
		if url, _, _ := selectPhoto(map[string]string{"2048": ""}, nil); url != "" {
			t.Fatalf("got %q; want empty", url)
		}
	})

	t.Run("non-numeric key fallback", func(t *testing.T) {
		if url, _, _ := selectPhoto(map[string]string{"orig": "uorig"}, nil); url != "uorig" {
			t.Fatalf("got %q; want uorig", url)
		}
	})
}
