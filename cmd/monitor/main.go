package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"context"
	"unicode"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"

	"os/signal"
	"syscall"
)

type Feature struct {
	Type       string         `json:"type"`
	Geometry   map[string]any `json:"geometry"`
	Properties map[string]any `json:"properties"`
}

type FeatureCollection struct {
	Type     string    `json:"type"`
	Features []Feature `json:"features"`
}

type ApiResponse struct {
	Data any `json:"data"`
}

func getenv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// remove diacritics
func stripAccents(s string) string {
	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	res, _, _ := transform.String(t, s)
	return res
}

func normMunicipio(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = stripAccents(s)
	s = strings.ReplaceAll(s, "-", " ")
	s = strings.ReplaceAll(s, "_", " ")
	s = strings.Join(strings.Fields(s), " ")
	s = strings.ReplaceAll(s, " ", "")
	return s
}

var municipioSynonyms = map[string][]string{
	"proencaanova":      {"proenca a nova", "proenca-anova", "proenca nova"},
	"vilavelhaderodao":  {"vila velha de rodao", "v v rodao", "vv rodao"},
	"castanheiradepera": {"castanheira de pera", "castanheira pera"},
	"pedrogaogrande":    {"pedrogao grande", "pedrogao-grande"},
}

var defaultMunicipios = []string{
	"Sertã",
	"Oleiros",
	"Castanheira de Pera",
	"Proença-a-Nova",
	"Vila de Rei",
	"Vila Velha de Ródão",
	"Sardoal",
	"Figueiró dos Vinhos",
	"Pedrógão Grande",
	"Pampilhosa da Serra",
	"Ferreira do Zêzere",
	"Fundão",
	"Castelo Branco",
	"Idanha-a-Nova",
	"Penamacor",
	"Belmonte",
	"Covilhã",
}

func wantedMunicipiosFromEnv() []string {
	v := getenv("MUNICIPIOS", getenv("MUNICIPIO", strings.Join(defaultMunicipios, ",")))
	sep := ","
	if strings.Contains(v, ";") {
		sep = ";"
	}
	parts := strings.Split(v, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func makeWantedSet(names []string) (set map[string][]string, flat []string) {
	set = map[string][]string{}
	for _, n := range names {
		key := normMunicipio(n)
		alts := slices.Clone(municipioSynonyms[key])
		set[key] = append([]string{key}, func() []string {
			arr := make([]string, len(alts))
			for i, s := range alts {
				arr[i] = normMunicipio(s)
			}
			return arr
		}()...)
	}
	for k, alts := range set {
		flat = append(flat, k)
		flat = append(flat, alts...)
	}
	return
}

func defaultHeaders() http.Header {
	h := http.Header{}
	h.Set("Accept", "application/json")
	h.Set("User-Agent", "David-Bombeiros/0.3 (Go)")
	h.Set("Accept-Language", "pt-PT,pt;q=0.9,en;q=0.8")
	h.Set("Referer", "https://fogos.pt/")
	h.Set("Origin", "https://fogos.pt")
	h.Set("Cache-Control", "no-cache")
	if key := strings.TrimSpace(os.Getenv("FOGOS_API_KEY")); key != "" {
		h.Set("Authorization", "Bearer "+key)
	}
	return h
}

// Reusable HTTP client with sane timeout
var httpClient = &http.Client{Timeout: 20 * time.Second}

// ETag/Last-Modified cache (in-memory) for the primary endpoint
var lastETag string
var lastLastModified string
var cachedFeatures []Feature

// Lightweight debug logger (enable with LOG_LEVEL=debug or DEBUG=1)
func debugf(format string, a ...any) {
	if strings.EqualFold(getenv("LOG_LEVEL", ""), "debug") || getenv("DEBUG", "") != "" {
		fmt.Printf("[debug] "+format+"\n", a...)
	}
}

// Metrics
var (
	metricsEnabled  bool
	activeIncidents = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bombeiros_active_incidents",
		Help: "Active incidents count with labels",
	}, []string{"district", "concelho", "regiao", "natureza", "status"})
	statusTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bombeiros_status_transitions_total",
		Help: "Total number of status transitions",
	}, []string{"from", "to"})
	timeToConclusion = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "bombeiros_time_to_conclusion_seconds",
		Help:    "Time from first seen to conclusion",
		Buckets: prometheus.LinearBuckets(300, 900, 20), // 5min start, +15min, 20 buckets ~ 5h
	})
)

func doGet(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header = defaultHeaders()
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		// Read and close body to avoid leaking the connection
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("http %d GET %s: %s", resp.StatusCode, url, strings.TrimSpace(string(msg)))
	}
	return resp, nil
}

// GET with extra headers (for If-None-Match / If-Modified-Since)
func doGetWithHeaders(url string, extra http.Header) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header = defaultHeaders()
	for k, vals := range extra {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("http %d GET %s: %s", resp.StatusCode, url, strings.TrimSpace(string(msg)))
	}
	return resp, nil
}

func fetchActiveFeatures() ([]Feature, error) {
	base := strings.TrimSpace(getenv("FOGOS_URL", "https://api.fogos.pt/v2/incidents/active?geojson=true"))
	fallbacks := strings.FieldsFunc(strings.TrimSpace(os.Getenv("FOGOS_FALLBACK_URLS")), func(r rune) bool { return r == ',' || r == ' ' || r == ';' })
	urls := append([]string{base}, fallbacks...)
	var lastErr error
	for i, u := range urls {
		var resp *http.Response
		var err error
		if i == 0 {
			// Try conditional GET on the primary endpoint
			extra := http.Header{}
			if lastETag != "" {
				extra.Set("If-None-Match", lastETag)
			}
			if lastLastModified != "" {
				extra.Set("If-Modified-Since", lastLastModified)
			}
			resp, err = doGetWithHeaders(u, extra)
		} else {
			resp, err = doGet(u)
		}
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(200*(i+1)) * time.Millisecond)
			continue
		}
		if resp.StatusCode == http.StatusNotModified && cachedFeatures != nil {
			_ = resp.Body.Close()
			debugf("HTTP 304 Not Modified (using cached features)")
			return cachedFeatures, nil
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(200*(i+1)) * time.Millisecond)
			continue
		}
		features, err := toFeatures(data)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(200*(i+1)) * time.Millisecond)
			continue
		}
		if i == 0 { // update cache metadata only for primary endpoint
			lastETag = strings.TrimSpace(resp.Header.Get("ETag"))
			lastLastModified = strings.TrimSpace(resp.Header.Get("Last-Modified"))
			cachedFeatures = features
			if lastETag != "" || lastLastModified != "" {
				debugf("Cached validators set (ETag=%q, Last-Modified=%q)", lastETag, lastLastModified)
			}
		}
		return features, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no endpoints tried")
	}
	return nil, lastErr
}

func toFeatures(body []byte) ([]Feature, error) {
	// Try several shapes: FeatureCollection, {data: FeatureCollection}, array
	var fc FeatureCollection
	if err := json.Unmarshal(body, &fc); err == nil && fc.Type != "" {
		// Accept empty collections
		return fc.Features, nil
	}
	var wrap ApiResponse
	if err := json.Unmarshal(body, &wrap); err == nil && wrap.Data != nil {
		b, _ := json.Marshal(wrap.Data)
		if err := json.Unmarshal(b, &fc); err == nil && fc.Type != "" {
			return fc.Features, nil
		}
		var arr []Feature
		if err := json.Unmarshal(b, &arr); err == nil {
			return arr, nil
		}
	}
	var arr []Feature
	if err := json.Unmarshal(body, &arr); err == nil {
		return arr, nil
	}
	return nil, fmt.Errorf("unknown response shape")
}

func getID(p map[string]any) string {
	keys := []string{"id", "globalId", "globalid", "ogc_fid", "ogcId", "uid"}
	for _, k := range keys {
		if v, ok := p[k]; ok {
			switch t := v.(type) {
			case string:
				if t != "" {
					return t
				}
			case float64:
				if t != 0 {
					return fmt.Sprintf("%.0f", t)
				}
			}
		}
	}
	return ""
}

func getMunicipio(p map[string]any) string {
	for _, k := range []string{"concelho", "municipio", "county", "municipality"} {
		if v, ok := p[k].(string); ok && strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

type perMuniState map[string]map[string]struct{}
type perMuniSeen map[string]map[string]time.Time

// Additional state: status per ID and first/concluded timestamps (UTC)
type idStatusMap map[string]string
type idTimeMap map[string]time.Time

func loadLastState(path string) (perMuniState, perMuniSeen, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return perMuniState{}, perMuniSeen{}, err
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		return perMuniState{}, perMuniSeen{}, err
	}
	st := perMuniState{}
	if m, ok := raw["by"].(map[string]any); ok {
		for muni, idsAny := range m {
			set := map[string]struct{}{}
			if idsArr, ok := idsAny.([]any); ok {
				for _, v := range idsArr {
					if s, ok := v.(string); ok {
						set[s] = struct{}{}
					}
				}
			}
			st[muni] = set
		}
	}
	seen := perMuniSeen{}
	if sm, ok := raw["seen"].(map[string]any); ok {
		for muni, ids := range sm {
			m2 := map[string]time.Time{}
			if kv, ok := ids.(map[string]any); ok {
				for id, tsAny := range kv {
					if s, ok := tsAny.(string); ok {
						if t, err := time.Parse(time.RFC3339, s); err == nil {
							m2[id] = t
						}
					}
				}
			}
			if len(m2) > 0 {
				seen[muni] = m2
			}
		}
	}
	// Extended maps: status, first, concluded
	if m, ok := raw["status"].(map[string]any); ok {
		for id, v := range m {
			if s, ok := v.(string); ok {
				lastStatusByID[id] = s
			}
		}
	}
	if m, ok := raw["first"].(map[string]any); ok {
		for id, v := range m {
			if s, ok := v.(string); ok {
				if t, err := time.Parse(time.RFC3339, s); err == nil {
					firstSeenByID[id] = t
				}
			}
		}
	}
	if m, ok := raw["concluded"].(map[string]any); ok {
		for id, v := range m {
			if s, ok := v.(string); ok {
				if t, err := time.Parse(time.RFC3339, s); err == nil {
					concludedAtID[id] = t
				}
			}
		}
	}
	// Optional migration: legacy files may not have these keys; that's fine
	return st, seen, nil
}

func saveLastState(path string, st perMuniState, seen perMuniSeen) error {
	raw := map[string]any{"by": map[string][]string{}, "seen": map[string]map[string]string{}, "status": map[string]string{}, "first": map[string]string{}, "concluded": map[string]string{}}
	for muni, set := range st {
		ids := make([]string, 0, len(set))
		for id := range set {
			ids = append(ids, id)
		}
		raw["by"].(map[string][]string)[muni] = ids
	}
	seenOut := raw["seen"].(map[string]map[string]string)
	for muni, kv := range seen {
		out := map[string]string{}
		for id, ts := range kv {
			out[id] = ts.UTC().Format(time.RFC3339)
		}
		seenOut[muni] = out
	}
	// Save extended maps
	stOut := raw["status"].(map[string]string)
	for id, s := range lastStatusByID {
		if strings.TrimSpace(id) != "" && strings.TrimSpace(s) != "" {
			stOut[id] = s
		}
	}
	fstOut := raw["first"].(map[string]string)
	for id, ts := range firstSeenByID {
		fstOut[id] = ts.UTC().Format(time.RFC3339)
	}
	cOut := raw["concluded"].(map[string]string)
	for id, ts := range concludedAtID {
		cOut[id] = ts.UTC().Format(time.RFC3339)
	}
	b, _ := json.MarshalIndent(raw, "", "  ")
	if err := os.WriteFile(path, b, 0644); err != nil {
		return err
	}
	return nil
}

func filterByMunicipios(features []Feature, wantedFlat []string) []Feature {
	wset := map[string]struct{}{}
	for _, w := range wantedFlat {
		wset[w] = struct{}{}
	}
	out := make([]Feature, 0, len(features))
	for _, f := range features {
		mun := normMunicipio(getMunicipio(f.Properties))
		if _, ok := wset[mun]; ok {
			out = append(out, f)
		}
	}
	return out
}

// Optional filter by radius from a center point (in km). Disabled if radiusKm <= 0.
func filterByRadius(features []Feature, centerLat, centerLon, radiusKm float64) []Feature {
	if radiusKm <= 0 {
		return features
	}
	out := make([]Feature, 0, len(features))
	for _, f := range features {
		if lat, lon, ok := getCoords(f.Geometry); ok {
			if haversineKm(centerLat, centerLon, lat, lon) <= radiusKm {
				out = append(out, f)
			}
		}
	}
	return out
}

func haversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371.0
	toRad := func(d float64) float64 { return d * (math.Pi / 180) }
	dLat := toRad(lat2 - lat1)
	dLon := toRad(lon2 - lon1)
	a := (math.Sin(dLat/2) * math.Sin(dLat/2)) + math.Cos(toRad(lat1))*math.Cos(toRad(lat2))*(math.Sin(dLon/2)*math.Sin(dLon/2))
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return R * c
}

func muniLabel(names []string) string {
	if len(names) == 0 {
		return ""
	}
	if len(names) == 1 {
		return names[0]
	}
	return strings.Join(names[:len(names)-1], ", ") + " e " + names[len(names)-1]
}

func prettyTime(val any) string {
	switch v := val.(type) {
	case string:
		// Try common formats
		layouts := []string{time.RFC3339, "2006-01-02 15:04:05", "02/01/2006 15:04"}
		for _, layout := range layouts {
			if t, err := time.Parse(layout, v); err == nil {
				return t.Local().Format("02-01 15:04")
			}
		}
	case float64:
		// Epoch seconds
		if v > 0 {
			return time.Unix(int64(v), 0).Local().Format("02-01 15:04")
		}
	case map[string]any:
		// Support {"sec": ...}
		if sec, ok := v["sec"]; ok {
			if f, ok2 := toFloat(sec); ok2 && f > 0 {
				return time.Unix(int64(f), 0).Local().Format("02-01 15:04")
			}
		}
	}
	return ""
}

// Helpers for UI/UX and enhanced notifications
func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(t), 64); err == nil {
			return f, true
		}
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return f, true
		}
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case uint64:
		return float64(t), true
	}
	return 0, false
}

func getCoords(geom map[string]any) (lat, lon float64, ok bool) {
	if geom == nil {
		return
	}
	// GeoJSON: coordinates = [lon, lat]
	if coords, ok2 := geom["coordinates"].([]any); ok2 && len(coords) >= 2 {
		lonF, okLon := toFloat(coords[0])
		latF, okLat := toFloat(coords[1])
		if okLon && okLat {
			return latF, lonF, true
		}
	}
	return 0, 0, false
}

func mapsURLForFeature(f Feature, muni string) string {
	if lat, lon, ok := getCoords(f.Geometry); ok {
		return fmt.Sprintf("https://www.google.com/maps/search/?api=1&query=%f,%f", lat, lon)
	}
	if strings.TrimSpace(muni) != "" {
		return "https://www.google.com/maps/search/?api=1&query=" + url.QueryEscape(muni+", Portugal")
	}
	return ""
}

func inQuietHours() bool {
	// Formats like "23-7" or "22-07"
	win := strings.TrimSpace(getenv("QUIET_HOURS", ""))
	if win == "" {
		return false
	}
	parts := strings.Split(win, "-")
	if len(parts) != 2 {
		return false
	}
	parseHour := func(s string) (int, bool) {
		s = strings.TrimSpace(s)
		if strings.Contains(s, ":") {
			s = strings.SplitN(s, ":", 2)[0]
		}
		h, err := strconv.Atoi(s)
		return h, err == nil && h >= 0 && h <= 23
	}
	startH, ok1 := parseHour(parts[0])
	endH, ok2 := parseHour(parts[1])
	if !ok1 || !ok2 {
		return false
	}
	nowH := time.Now().Hour()
	if startH == endH {
		return true // 24h quiet if same hour
	}
	if startH < endH {
		return nowH >= startH && nowH < endH
	}
	// window crossing midnight
	return nowH >= startH || nowH < endH
}

func addTag(tags, t string) string {
	t = strings.TrimSpace(t)
	if t == "" {
		return tags
	}
	if strings.TrimSpace(tags) == "" {
		return t
	}
	for _, x := range strings.Split(tags, ",") {
		if strings.EqualFold(strings.TrimSpace(x), t) {
			return tags
		}
	}
	return tags + "," + t
}

func getPropStr(p map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := p[k]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return s
			}
			// Accept numbers
			if f, ok := toFloat(v); ok {
				return fmt.Sprintf("%.0f", f)
			}
		}
	}
	return ""
}

// Extract a Fogos.pt incident URL from a notification body, if present
func extractFogosURLFromBody(body string) string {
	const prefix = "https://fogos.pt/fogo/"
	i := strings.Index(body, prefix)
	if i < 0 {
		return ""
	}
	j := i + len(prefix)
	// read until whitespace or line end
	for j < len(body) {
		if body[j] == '\n' || body[j] == '\r' || body[j] == ' ' || body[j] == '\t' {
			break
		}
		j++
	}
	return body[i:j]
}

func extractURLAfterPrefix(body, prefix string) string {
	i := strings.Index(body, prefix)
	if i < 0 {
		return ""
	}
	j := i + len(prefix)
	for j < len(body) {
		if body[j] == '\n' || body[j] == '\r' || body[j] == ' ' || body[j] == '\t' {
			break
		}
		j++
	}
	return strings.TrimSpace(body[i+len(prefix) : j])
}

// Canonicalize seen map keys according to wantedSet and known corrections
func canonicalizeSeenKeys(seen perMuniSeen, wantedSet map[string][]string) perMuniSeen {
	if seen == nil {
		return perMuniSeen{}
	}
	aliasToCanon := map[string]string{}
	for canon, alts := range wantedSet {
		aliasToCanon[canon] = canon
		for _, a := range alts {
			aliasToCanon[a] = canon
		}
	}
	corrections := map[string]string{
		"sert":             "serta",
		"figueirdosvinhos": "figueirodosvinhos",
		"proenaanova":      "proencaanova",
		"vilavelhaderdo":   "vilavelhaderodao",
	}
	out := perMuniSeen{}
	for k, kv := range seen {
		nk := k
		if v, ok := corrections[nk]; ok {
			nk = v
		}
		if v, ok := aliasToCanon[nk]; ok {
			nk = v
		}
		if out[nk] == nil {
			out[nk] = map[string]time.Time{}
		}
		for id, ts := range kv {
			out[nk][id] = ts
		}
	}
	return out
}

// Extended ntfy with dry-run, quiet-hours and click URL
func postNtfyExt(ntfyURL, topic, title, body, tags, priority, clickURL string) {
	if strings.TrimSpace(topic) == "" {
		return
	}
	// Dry-run mode: log instead of posting
	if getenv("NTFY_DRYRUN", "") != "" {
		fmt.Printf("[dry-run ntfy] %s\n%s\n", title, body)
		return
	}
	// Quiet hours: lower priority and tag
	if inQuietHours() {
		if strings.TrimSpace(priority) == "" || priority > "3" {
			priority = "3"
		}
		tags = addTag(tags, "zzz")
	}
	endpoint := strings.TrimRight(ntfyURL, "/") + "/" + topic
	req, _ := http.NewRequest("POST", endpoint, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Title", title)
	if tags != "" {
		req.Header.Set("Tags", tags)
	}
	if priority != "" {
		req.Header.Set("Priority", priority)
	} else {
		req.Header.Set("Priority", "3")
	}
	if strings.TrimSpace(clickURL) != "" {
		req.Header.Set("Click", clickURL)
	}
	// Optional Actions: 'Abrir Mapa' and 'Abrir Fogos'
	actions := []string{}
	if clickURL != "" {
		actions = append(actions, fmt.Sprintf("view, Abrir Mapa, %s", clickURL))
	}
	if fogosID := getenv("FOGOS_ID_OVERRIDE", ""); false { // placeholder to keep structure
		_ = fogosID
	}
	if urlFogos := extractFogosURLFromBody(body); urlFogos != "" {
		actions = append(actions, fmt.Sprintf("view, Abrir Fogos, %s", urlFogos))
	}
	if areaURL := extractURLAfterPrefix(body, "Área URL: "); areaURL != "" {
		actions = append(actions, fmt.Sprintf("view, Abrir área, %s", areaURL))
	} else if areaURL2 := extractURLAfterPrefix(body, "Area URL: "); areaURL2 != "" { // no accent fallback
		actions = append(actions, fmt.Sprintf("view, Abrir area, %s", areaURL2))
	}
	if len(actions) > 0 {
		req.Header.Set("Actions", strings.Join(actions, "; "))
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ntfy erro:", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		fmt.Fprintf(os.Stderr, "ntfy HTTP %d: %s\n", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
}

// Canonicalize/migrate inconsistent municipality keys in state
func canonicalizeStateKeys(st perMuniState, wantedSet map[string][]string) perMuniState {
	if st == nil {
		return perMuniState{}
	}
	// Build alias -> canonical map from wantedSet
	aliasToCanon := map[string]string{}
	for canon, alts := range wantedSet {
		aliasToCanon[canon] = canon
		for _, a := range alts {
			aliasToCanon[a] = canon
		}
	}
	// Hard corrections for known typos seen in last_ids.json
	corrections := map[string]string{
		"sert":             "serta",
		"figueirdosvinhos": "figueirodosvinhos",
		"proenaanova":      "proencaanova",
		"vilavelhaderdo":   "vilavelhaderodao",
	}
	out := perMuniState{}
	for k, set := range st {
		nk := k
		if v, ok := corrections[nk]; ok {
			nk = v
		}
		if v, ok := aliasToCanon[nk]; ok {
			nk = v
		}
		if out[nk] == nil {
			out[nk] = map[string]struct{}{}
		}
		for id := range set {
			out[nk][id] = struct{}{}
		}
	}
	return out
}

// Helpers for filtering
func parseIntSetFromEnv(name string) map[int]struct{} {
	set := map[int]struct{}{}
	v := strings.TrimSpace(getenv(name, ""))
	if v == "" {
		return set
	}
	for _, p := range strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == ';' || r == ' ' }) {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if i, err := strconv.Atoi(p); err == nil {
			set[i] = struct{}{}
		}
	}
	return set
}

func parseStrSetFromEnv(name string) map[string]struct{} {
	set := map[string]struct{}{}
	v := strings.TrimSpace(getenv(name, ""))
	if v == "" {
		return set
	}
	for _, p := range strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == ';' || r == '|' }) {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		set[strings.ToLower(stripAccents(p))] = struct{}{}
	}
	return set
}

func shouldKeepByAdminUnits(p map[string]any) bool {
	// District
	if ds := parseStrSetFromEnv("DISTRICTS"); len(ds) > 0 {
		d := strings.ToLower(stripAccents(getPropStr(p, "district")))
		if _, ok := ds[d]; !ok {
			return false
		}
	}
	if rs := parseStrSetFromEnv("REGIOES"); len(rs) > 0 {
		r := strings.ToLower(stripAccents(getPropStr(p, "regiao")))
		if _, ok := rs[r]; !ok {
			return false
		}
	}
	if srs := parseStrSetFromEnv("SUBREGIOES"); len(srs) > 0 {
		sr := strings.ToLower(stripAccents(getPropStr(p, "sub_regiao")))
		if _, ok := srs[sr]; !ok {
			return false
		}
	}
	if frs := parseStrSetFromEnv("FREGUESIAS"); len(frs) > 0 {
		f := strings.ToLower(stripAccents(getPropStr(p, "freguesia")))
		if _, ok := frs[f]; !ok {
			return false
		}
	}
	return true
}

func shouldKeepByNatureAndStatus(p map[string]any) bool {
	// EXCLUDE_STATUS_CODES = comma-int list
	if exc := parseIntSetFromEnv("EXCLUDE_STATUS_CODES"); len(exc) > 0 {
		if scF, ok := toFloat(p["statusCode"]); ok {
			sc := int(scF)
			if _, bad := exc[sc]; bad {
				return false
			}
		}
	}
	// INCLUDE_NATUREZA = case-insensitive list; match on natureza or naturezaCode
	if inc := parseStrSetFromEnv("INCLUDE_NATUREZA"); len(inc) > 0 {
		nz := strings.ToLower(stripAccents(getPropStr(p, "natureza")))
		nzc := strings.ToLower(stripAccents(getPropStr(p, "naturezaCode")))
		if _, ok := inc[nz]; ok {
			return true
		}
		if _, ok := inc[nzc]; ok {
			return true
		}
		// allow substring match
		for want := range inc {
			if want != "" && strings.Contains(nz, want) {
				return true
			}
		}
		return false
	}
	return true
}

// Enrichment: compute dynamic tags and suggested priority from means
func enrichMeansTagsAndPriority(p map[string]any, baseTags, basePriority string) (string, string) {
	get := func(name string) int {
		if f, ok := toFloat(p[name]); ok {
			return int(f)
		}
		return 0
	}
	man := get("man")
	ter := get("terrain")
	air := get("aerial")
	aq := get("meios_aquaticos")
	// thresholds (0 disables)
	thMan, _ := strconv.Atoi(getenv("MIN_MAN", "0"))
	thTer, _ := strconv.Atoi(getenv("MIN_TERRAIN", "0"))
	thAir, _ := strconv.Atoi(getenv("MIN_AERIAL", "0"))
	thAq, _ := strconv.Atoi(getenv("MIN_AQUATIC", "0"))
	tags := baseTags
	prio := basePriority
	// lower numbers are higher priority in ntfy (1 max, 5 min)
	bump := func(n int) {
		if n <= 0 {
			return
		}
		cur, _ := strconv.Atoi(func() string {
			if strings.TrimSpace(prio) == "" {
				return "5"
			}
			return prio
		}())
		if n < cur {
			prio = strconv.Itoa(n)
		}
	}
	if thMan > 0 && man >= thMan {
		tags = addTag(tags, "man")
		bump(2)
	}
	if thTer > 0 && ter >= thTer {
		tags = addTag(tags, "terrain")
		bump(3)
	}
	if thAir > 0 && air >= thAir {
		tags = addTag(tags, "aerial")
		bump(2)
	}
	if thAq > 0 && aq >= thAq {
		tags = addTag(tags, "aquatic")
		bump(3)
	}
	return tags, prio
}

func parseExtraTags(extra string) (tags []string, highlight string) {
	s := strings.ToLower(stripAccents(extra))
	if strings.Contains(s, "reabert") {
		tags = append(tags, "white_check_mark")
	}
	if strings.Contains(s, "cortad") || strings.Contains(s, "encerrad") || strings.Contains(s, "fechad") || strings.Contains(s, "corte") {
		tags = append(tags, "no_entry")
	}
	// keep original as highlight
	highlight = extra
	return
}

// KML VOST handling: save and compute area/perimeter
func saveKMLAndCompute(kmlStr, saveDir, id string) (areaKm2, perimeterKm float64, fileURL string, saved bool, err error) {
	if strings.TrimSpace(kmlStr) == "" || strings.TrimSpace(saveDir) == "" {
		return 0, 0, "", false, nil
	}
	if err = os.MkdirAll(saveDir, 0755); err != nil {
		return 0, 0, "", false, err
	}
	fname := fmt.Sprintf("%s.kml", id)
	full := filepath.Join(saveDir, fname)
	if writeErr := os.WriteFile(full, []byte(kmlStr), 0644); writeErr != nil {
		return 0, 0, "", false, writeErr
	}
	// Make file URL
	abs, _ := filepath.Abs(full)
	uri := abs
	if os.PathSeparator == '\\' {
		uri = strings.ReplaceAll(abs, "\\", "/")
		if !strings.HasPrefix(uri, "/") {
			// Ensure leading slash like /C:/...
			uri = "/" + uri
		}
		uri = "file://" + uri
	} else {
		uri = "file://" + uri
	}
	// Very simple polygon extraction
	coordsStart := strings.Index(strings.ToLower(kmlStr), "<coordinates>")
	coordsEnd := strings.Index(strings.ToLower(kmlStr), "</coordinates>")
	if coordsStart > 0 && coordsEnd > coordsStart {
		coordsText := kmlStr[coordsStart+13 : coordsEnd]
		// parse lon,lat[,alt] tuples separated by space or newline
		type pt struct{ lat, lon float64 }
		var pts []pt
		for _, tok := range strings.Fields(coordsText) {
			parts := strings.Split(tok, ",")
			if len(parts) >= 2 {
				lon, e1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
				lat, e2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
				if e1 == nil && e2 == nil {
					pts = append(pts, pt{lat: lat, lon: lon})
				}
			}
		}
		if len(pts) >= 3 {
			// Compute area/perimeter with equirectangular projection around mean lat
			var lat0 float64
			for _, p := range pts {
				lat0 += p.lat
			}
			lat0 /= float64(len(pts))
			const R = 6371000.0
			toXY := func(p pt) (x, y float64) {
				x = (p.lon * math.Pi / 180) * R * math.Cos(lat0*math.Pi/180)
				y = (p.lat * math.Pi / 180) * R
				return
			}
			// Shoelace area and perimeter
			var area2 float64
			var per float64
			for i := 0; i < len(pts); i++ {
				j := (i + 1) % len(pts)
				x1, y1 := toXY(pts[i])
				x2, y2 := toXY(pts[j])
				area2 += x1*y2 - x2*y1
				dx := x2 - x1
				dy := y2 - y1
				per += math.Hypot(dx, dy)
			}
			areaKm2 = math.Abs(area2) / 2 / 1e6
			perimeterKm = per / 1000
		}
	}
	return areaKm2, perimeterKm, uri, true, nil
}

// In-memory status tracking for transitions and summaries
var (
	lastStatusByID  = map[string]string{}
	firstSeenByID   = map[string]time.Time{}
	concludedAtID   = map[string]time.Time{}
	lastSummaryHour int
	lastSummaryDay  string
)

func runOnce(statePath string, wantedNames []string) (changed bool, err error) {
	features, err := fetchActiveFeatures()
	if err != nil {
		return false, err
	}
	wantedSet, wantedFlat := makeWantedSet(wantedNames)
	filtered := filterByMunicipios(features, wantedFlat)
	// Additional admin filters
	tmp := make([]Feature, 0, len(filtered))
	for _, f := range filtered {
		if shouldKeepByAdminUnits(f.Properties) && shouldKeepByNatureAndStatus(f.Properties) {
			tmp = append(tmp, f)
		}
	}
	filtered = tmp
	// Optional radius filter
	centerLat, _ := strconv.ParseFloat(strings.TrimSpace(getenv("CENTER_LAT", "")), 64)
	centerLon, _ := strconv.ParseFloat(strings.TrimSpace(getenv("CENTER_LON", "")), 64)
	radiusKm, _ := strconv.ParseFloat(strings.TrimSpace(getenv("RADIUS_KM", "0")), 64)
	if radiusKm > 0 && !math.IsNaN(centerLat) && !math.IsNaN(centerLon) && centerLat != 0 {
		filtered = filterByRadius(filtered, centerLat, centerLon, radiusKm)
	}
	debugf("Fetched %d features; filtered to %d", len(features), len(filtered))

	// load state
	st, seen, _ := loadLastState(statePath)
	if st == nil {
		st = perMuniState{}
	}
	if seen == nil {
		seen = perMuniSeen{}
	}
	// migrate/canonicalize keys
	st = canonicalizeStateKeys(st, wantedSet)
	seen = canonicalizeSeenKeys(seen, wantedSet)

	// compute new IDs per muni
	now := time.Now()
	ntfyURL := getenv("NTFY_URL", "https://ntfy.sh")
	topic := getenv("NTFY_TOPIC", "bombeiros-serta")
	priority := getenv("NTFY_PRIORITY", "5")
	tags := getenv("NTFY_TAGS", "fire,rotating_light")

	perMuniNew := map[string][]Feature{}
	for _, f := range filtered {
		mun := normMunicipio(getMunicipio(f.Properties))
		// map syns to canonical key if needed
		canon := mun
		for k, alts := range wantedSet {
			for _, a := range alts {
				if a == mun {
					canon = k
					break
				}
			}
		}
		perMuniNew[canon] = append(perMuniNew[canon], f)
	}

	// init existing
	for k := range wantedSet {
		if _, ok := st[k]; !ok {
			st[k] = map[string]struct{}{}
		}
		if _, ok := seen[k]; !ok {
			seen[k] = map[string]time.Time{}
		}
	}

	// update last-seen for current active IDs and collect events for new ones and status changes
	type newEvent struct {
		muniKey string
		disp    string
		id      string
		when    string
		f       Feature
		prev    string
		cur     string
	}
	events := make([]newEvent, 0, 8)
	statusEvents := make([]newEvent, 0, 8)
	for muniKey, feats := range perMuniNew {
		for _, f := range feats {
			id := getID(f.Properties)
			if id == "" {
				continue
			}
			// mark last seen for ids present in this cycle
			if seen[muniKey] == nil {
				seen[muniKey] = map[string]time.Time{}
			}
			seen[muniKey][id] = now
			if _, ok := st[muniKey][id]; !ok {
				st[muniKey][id] = struct{}{}
				when := prettyTime(f.Properties["dateTime"])
				disp := getMunicipio(f.Properties)
				if disp == "" {
					disp = muniKey
				}
				events = append(events, newEvent{muniKey: muniKey, disp: disp, id: id, when: when, f: f})
				// first seen tracking
				if _, ok := firstSeenByID[id]; !ok {
					firstSeenByID[id] = now
				}
			}
			// Status change detection
			curStatus := getPropStr(f.Properties, "status")
			prev := lastStatusByID[id]
			if curStatus != "" && curStatus != prev {
				statusEvents = append(statusEvents, newEvent{muniKey: muniKey, disp: getMunicipio(f.Properties), id: id, when: prettyTime(f.Properties["updated"]), f: f, prev: prev, cur: curStatus})
				if prev != "" {
					statusTransitions.WithLabelValues(prev, curStatus).Inc()
				}
				lastStatusByID[id] = curStatus
				// conclusion timing
				if strings.EqualFold(curStatus, "Conclusão") || strings.Contains(strings.ToLower(stripAccents(curStatus)), "conclus") {
					concludedAtID[id] = now
					if t0, ok := firstSeenByID[id]; ok && now.After(t0) {
						timeToConclusion.Observe(now.Sub(t0).Seconds())
					}
				}
				// Reactivation: previously concluded now active again
				if (strings.Contains(strings.ToLower(stripAccents(prev)), "conclus") || strings.Contains(strings.ToLower(stripAccents(prev)), "vigil")) && (strings.Contains(strings.ToLower(stripAccents(curStatus)), "curso") || strings.Contains(strings.ToLower(stripAccents(curStatus)), "despacho")) {
					// mark with special tag later via event fields
				}
			}
		}
	}

	anyChange := len(events) > 0 || len(statusEvents) > 0

	// notify (aggregate or per-incident)
	if anyChange {
		// Optional aggregation threshold (0 = disabled)
		summaryThreshold := 0
		fmt.Sscanf(getenv("NTFY_SUMMARY_THRESHOLD", "0"), "%d", &summaryThreshold)
		if summaryThreshold > 0 && len(events) >= summaryThreshold {
			counts := map[string]int{}
			sampleIDs := map[string][]string{}
			for _, ev := range events {
				counts[ev.disp]++
				if len(sampleIDs[ev.disp]) < 5 {
					sampleIDs[ev.disp] = append(sampleIDs[ev.disp], ev.id)
				}
			}
			lines := make([]string, 0, len(counts))
			for muni, c := range counts {
				line := fmt.Sprintf("%s: %d", muni, c)
				if len(sampleIDs[muni]) > 0 {
					line += " (" + strings.Join(sampleIDs[muni], ", ") + ")"
				}
				lines = append(lines, line)
			}
			sort.Strings(lines)
			title := fmt.Sprintf("Novos incidentes (%d)", len(events))
			body := strings.Join(lines, "\n") + fmt.Sprintf("\nTotal ativo no alvo: %d", len(filtered))
			postNtfyExt(ntfyURL, topic, title, body, tags, priority, "")
		} else {
			for _, ev := range events {
				p := ev.f.Properties
				status := getPropStr(p, "status", "phase", "estado")
				nature := getPropStr(p, "natureza", "type", "tipo")
				man := getPropStr(p, "man")
				ter := getPropStr(p, "terrain")
				air := getPropStr(p, "aerial")
				aq := getPropStr(p, "meios_aquaticos")
				title := fmt.Sprintf("Novo em %s — %s", ev.disp, nature)
				if ev.when != "" {
					title += " (" + ev.when + ")"
				}
				body := fmt.Sprintf("ID: %s\nMunicípio: %s\nEstado: %s\nMeios: man=%s, ter=%s, air=%s, aq=%s", ev.id, ev.disp, status, man, ter, air, aq)
				// Extra
				if extra := getPropStr(p, "extra"); extra != "" {
					_, hi := parseExtraTags(extra)
					if hi != "" {
						body += "\nExtra: " + hi
					}
				}
				// KML area
				if kml := getPropStr(p, "kmlVost", "kml"); kml != "" {
					if areaKm2, perKm, areaURL, saved, _ := saveKMLAndCompute(kml, getenv("SAVE_KML_DIR", ""), ev.id); saved {
						body += fmt.Sprintf("\nÁrea: %.2f km², Perímetro: %.1f km", areaKm2, perKm)
						// Add area action by passing as clickURL? We'll prefer an action; we add URL in body so extractor can build action
						body += "\nÁrea URL: " + areaURL
					}
				}
				body += fmt.Sprintf("\nTotal ativo no alvo: %d", len(filtered))
				clickURL := mapsURLForFeature(ev.f, ev.disp)
				// Embed Fogos URL hint
				if ev.id != "" {
					body += "\nFogos: https://fogos.pt/fogo/" + ev.id
				}
				// Enrich tags/priority based on means
				tg, pr := enrichMeansTagsAndPriority(p, tags, priority)
				// Extra tags from 'extra'
				if extra := getPropStr(p, "extra"); extra != "" {
					if more, _ := parseExtraTags(extra); len(more) > 0 {
						for _, t := range more {
							tg = addTag(tg, t)
						}
					}
				}
				postNtfyExt(ntfyURL, topic, title, body, tg, pr, clickURL)
			}
			// Send status-change notifications
			for _, ev := range statusEvents {
				p := ev.f.Properties
				curStatus := getPropStr(p, "status")
				prev := ev.prev
				title := fmt.Sprintf("%s → %s — %s", func() string {
					if strings.TrimSpace(prev) == "" {
						return "Novo"
					}
					return prev
				}(), curStatus, ev.disp)
				man := getPropStr(p, "man")
				ter := getPropStr(p, "terrain")
				air := getPropStr(p, "aerial")
				aq := getPropStr(p, "meios_aquaticos")
				body := fmt.Sprintf("ID: %s\nMeios: man=%s, ter=%s, air=%s, aq=%s", ev.id, man, ter, air, aq)
				// Extra
				if extra := getPropStr(p, "extra"); extra != "" {
					_, hi := parseExtraTags(extra)
					if hi != "" {
						body += "\nExtra: " + hi
					}
				}
				// Fogos link
				if ev.id != "" {
					body += "\nFogos: https://fogos.pt/fogo/" + ev.id
				}
				// Adjust priority based on status
				pr := priority
				s := strings.ToLower(stripAccents(curStatus))
				if strings.Contains(s, "despacho") {
					pr = "4" // aviso inicial, menor prioridade
				} else if strings.Contains(s, "em curso") || strings.Contains(s, "em resolucao") {
					pr = "2" // ativo, prioridade alta
				} else if strings.Contains(s, "vigilancia") || strings.Contains(s, "conclus") {
					pr = "3"
				}
				tg, pr2 := enrichMeansTagsAndPriority(p, tags, pr)
				// Reactivation or conclusion tags
				prevS := strings.ToLower(stripAccents(prev))
				if (strings.Contains(prevS, "conclus") || strings.Contains(prevS, "vigil")) && (strings.Contains(s, "curso") || strings.Contains(s, "despacho")) {
					tg = addTag(tg, "repeat")
					title = "Reativado: " + title
					pr2 = "2"
				}
				if strings.Contains(s, "conclus") {
					tg = addTag(tg, "white_check_mark")
				}
				// Extra tags
				if extra := getPropStr(p, "extra"); extra != "" {
					if more, _ := parseExtraTags(extra); len(more) > 0 {
						for _, t := range more {
							tg = addTag(tg, t)
						}
					}
				}
				postNtfyExt(ntfyURL, topic, title, body, tg, pr2, mapsURLForFeature(ev.f, ev.disp))
			}
		}
	}

	// TTL retention: prune old IDs
	ttlHours, _ := strconv.ParseFloat(strings.TrimSpace(getenv("STATE_TTL_HOURS", "0")), 64)
	pruned := 0
	if ttlHours > 0 {
		cutoff := now.Add(-time.Duration(ttlHours * float64(time.Hour)))
		for muni, set := range st {
			for id := range set {
				ts, ok := seen[muni][id]
				if !ok || ts.Before(cutoff) {
					delete(st[muni], id)
					delete(seen[muni], id)
					pruned++
				}
			}
		}
	}

	// Metrics gauges: reset then set counts for current filtered
	if getenv("METRICS_DISABLE", "") == "" {
		metricsEnabled = true
		activeIncidents.Reset()
		for _, f := range filtered {
			p := f.Properties
			activeIncidents.WithLabelValues(
				getPropStr(p, "district"),
				getPropStr(p, "concelho"),
				getPropStr(p, "regiao"),
				getPropStr(p, "natureza"),
				getPropStr(p, "status"),
			).Inc()
		}
	}

	// Periodic summary (hourly/daily)
	nowHour := now.Hour()
	nowDay := now.Format("2006-01-02")
	if getenv("SUMMARY_HOURLY", "1") != "0" && lastSummaryHour != nowHour {
		// Build summary by concelho, natureza, estado
		byConc := map[string]int{}
		byNat := map[string]int{}
		bySta := map[string]int{}
		for _, f := range filtered {
			p := f.Properties
			byConc[getPropStr(p, "concelho")]++
			byNat[getPropStr(p, "natureza")]++
			bySta[getPropStr(p, "status")]++
		}
		mk := func(m map[string]int) string {
			type kv struct {
				k string
				v int
			}
			arr := []kv{}
			for k, v := range m {
				arr = append(arr, kv{k, v})
			}
			sort.Slice(arr, func(i, j int) bool { return arr[i].v > arr[j].v })
			parts := []string{}
			for i, e := range arr {
				if i >= 6 {
					break
				}
				parts = append(parts, fmt.Sprintf("%s: %d", e.k, e.v))
			}
			if len(parts) == 0 {
				return "(n/a)"
			}
			return strings.Join(parts, ", ")
		}
		title := fmt.Sprintf("Sumário horário (%02d:00)", nowHour)
		body := fmt.Sprintf("Ativos: %d\nConcelhos: %s\nNatureza: %s\nEstados: %s", len(filtered), mk(byConc), mk(byNat), mk(bySta))
		postNtfyExt(ntfyURL, topic, title, body, addTag(tags, "bar_chart"), "3", "")
		lastSummaryHour = nowHour
	}
	if getenv("SUMMARY_DAILY", "1") != "0" && lastSummaryDay != nowDay && now.Hour() == 8 {
		// Daily at ~08:00
		byConc := map[string]int{}
		byNat := map[string]int{}
		bySta := map[string]int{}
		for _, f := range filtered {
			p := f.Properties
			byConc[getPropStr(p, "concelho")]++
			byNat[getPropStr(p, "natureza")]++
			bySta[getPropStr(p, "status")]++
		}
		mk := func(m map[string]int) string {
			type kv struct {
				k string
				v int
			}
			arr := []kv{}
			for k, v := range m {
				arr = append(arr, kv{k, v})
			}
			sort.Slice(arr, func(i, j int) bool { return arr[i].v > arr[j].v })
			parts := []string{}
			for i, e := range arr {
				if i >= 10 {
					break
				}
				parts = append(parts, fmt.Sprintf("%s: %d", e.k, e.v))
			}
			if len(parts) == 0 {
				return "(n/a)"
			}
			return strings.Join(parts, "; ")
		}
		title := fmt.Sprintf("Sumário diário (%s)", nowDay)
		body := fmt.Sprintf("Ativos: %d\nConcelhos: %s\nNatureza: %s\nEstados: %s", len(filtered), mk(byConc), mk(byNat), mk(bySta))
		postNtfyExt(ntfyURL, topic, title, body, addTag(tags, "calendar"), "3", "")
		lastSummaryDay = nowDay
	}

	// Save state when there were new events or TTL pruned entries
	if anyChange || pruned > 0 {
		if err := saveLastState(statePath, st, seen); err != nil {
			fmt.Fprintln(os.Stderr, "Erro a gravar estado:", err)
		}
	} else {
		debugf("Sem alterações; estado não gravado")
	}
	fmt.Printf("{\n  \"count\": %d,\n  \"timestamp\": %q\n}\n", len(filtered), now.Format(time.RFC3339))
	return anyChange, nil
}

func main() {
	pollSecStr := getenv("POLL_SECONDS", "30")
	pollSec := 30
	fmt.Sscanf(pollSecStr, "%d", &pollSec)
	stateFile := getenv("STATE_FILE", "last_ids.json")
	if !filepath.IsAbs(stateFile) {
		stateFile = filepath.Join(".", stateFile)
	}
	wanted := wantedMunicipiosFromEnv()
	fmt.Printf("Monitor a cada %ds para: %s\n", pollSec, muniLabel(wanted))

	// Teste opcional de notificação no arranque (defina NTFY_TEST=1)
	if getenv("NTFY_TEST", "") != "" {
		postNtfyExt(getenv("NTFY_URL", "https://ntfy.sh"), getenv("NTFY_TOPIC", "bombeiros-serta"), "[teste] monitor iniciado", time.Now().Format(time.RFC3339), "white_check_mark", "3", "")
	}

	// Metrics endpoint
	if getenv("METRICS_DISABLE", "") == "" {
		addr := getenv("METRICS_ADDR", ":2112")
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			if err := http.ListenAndServe(addr, mux); err != nil {
				fmt.Fprintln(os.Stderr, "metrics server error:", err)
			}
		}()
		fmt.Println("Métricas Prometheus em", getenv("METRICS_ADDR", ":2112"), "/metrics")
	}

	// Graceful shutdown on Ctrl+C / SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if pollSec <= 0 {
		_, err := runOnce(stateFile, wanted)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Erro:", err)
			os.Exit(1)
		}
		return
	}

	ticker := time.NewTicker(time.Duration(pollSec) * time.Second)
	defer ticker.Stop()
	for {
		if _, err := runOnce(stateFile, wanted); err != nil {
			fmt.Fprintln(os.Stderr, "Erro:", err)
		}
		select {
		case <-ticker.C:
			// continue loop
		case <-ctx.Done():
			fmt.Println("A terminar...")
			return
		}
	}
}
