package main

import (
	"bytes"
	"encoding/json"
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

// (Removed) ETag/Last-Modified cache vars

// Lightweight debug logger (enable with LOG_LEVEL=debug or DEBUG=1)
func debugf(format string, a ...any) {
	if strings.EqualFold(getenv("LOG_LEVEL", ""), "debug") || getenv("DEBUG", "") != "" {
		fmt.Printf("[debug] "+format+"\n", a...)
	}
}

// Metrics
var (
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
// removed unused doGetWithHeaders

func fetchActiveFeatures() ([]Feature, error) {
	// Usa apenas a nova API (inclui incêndios, acidentes e outras naturezas)
	const u = "https://api-dev.fogos.pt/v2/incidents/active?all=1"
	resp, err := doGet(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return toFeatures(data)
}

func toFeatures(body []byte) ([]Feature, error) {
	// Constrói Features a partir de objetos simples (sem GeoJSON)
	buildFromPlain := func(objs []map[string]any) []Feature {
		out := make([]Feature, 0, len(objs))
		for _, obj := range objs {
			var geom map[string]any
			// Tenta lat/lng ou latitude/longitude
			if lat, ok1 := toFloat(obj["lat"]); ok1 {
				if lng, ok2 := toFloat(obj["lng"]); ok2 {
					geom = map[string]any{
						"type":        "Point",
						"coordinates": []any{lng, lat}, // GeoJSON [lon, lat]
					}
				}
			} else if lat, ok1 := toFloat(obj["latitude"]); ok1 {
				if lng, ok2 := toFloat(obj["longitude"]); ok2 {
					geom = map[string]any{
						"type":        "Point",
						"coordinates": []any{lng, lat},
					}
				}
			}
			out = append(out, Feature{
				Type:       "Feature",
				Geometry:   geom,
				Properties: obj,
			})
		}
		return out
	}

	// 1) FeatureCollection (GeoJSON)
	var fc FeatureCollection
	if err := json.Unmarshal(body, &fc); err == nil && fc.Type != "" {
		return fc.Features, nil
	}

	// 2) Resposta embrulhada: { success?: bool, data: ... } (api-dev)
	var wrap ApiResponse
	if err := json.Unmarshal(body, &wrap); err == nil && wrap.Data != nil {
		b, _ := json.Marshal(wrap.Data)
		// 2a) data é FeatureCollection
		if err := json.Unmarshal(b, &fc); err == nil && fc.Type != "" {
			return fc.Features, nil
		}
		// 2b) data é []Feature
		var arrF []Feature
		if err := json.Unmarshal(b, &arrF); err == nil {
			// Verificar se os elementos parecem válidos (possuem propriedades/geometry/type)
			valid := false
			for _, f := range arrF {
				if f.Type != "" || f.Geometry != nil || len(f.Properties) > 0 {
					valid = true
					break
				}
			}
			if valid {
				return arrF, nil
			}
			// Caso contrário, continuar para 2c (objetos simples)
		}
		// 2c) data é []map[string]any (objetos simples)
		var arrM []map[string]any
		if err := json.Unmarshal(b, &arrM); err == nil {
			return buildFromPlain(arrM), nil
		}
	}

	// 3) Top-level []Feature
	var arr []Feature
	if err := json.Unmarshal(body, &arr); err == nil {
		return arr, nil
	}

	// 4) Top-level []map[string]any (objetos simples)
	var arrM []map[string]any
	if err := json.Unmarshal(body, &arrM); err == nil {
		return buildFromPlain(arrM), nil
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

// Novo: snapshot tipado dos meios
type Means struct {
	Man     int `json:"man"`
	Terrain int `json:"terrain"`
	Aerial  int `json:"aerial"`
	Aquatic int `json:"aquatic"`
}

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

	// Novo: carregar snapshots de meios
	if m, ok := raw["means"].(map[string]any); ok {
		for id, v := range m {
			if mv, ok := v.(map[string]any); ok {
				getInt := func(k string) int {
					if f, ok := toFloat(mv[k]); ok {
						return int(f)
					}
					return 0
				}
				lastMeansByID[id] = Means{
					Man:     getInt("man"),
					Terrain: getInt("terrain"),
					Aerial:  getInt("aerial"),
					Aquatic: getInt("aquatic"),
				}
			}
		}
	}
	// Novo: carregar extra por ID
	if m, ok := raw["extra_text"].(map[string]any); ok {
		for id, v := range m {
			if s, ok := v.(string); ok {
				lastExtraByID[id] = s
			}
		}
	}
	// Novo: carregar marcas de sumários
	if s, ok := raw["last_hourly"].(string); ok {
		lastHourlyMark = s
	}
	if s, ok := raw["last_daily"].(string); ok {
		lastSummaryDay = s
	}
	// Optional migration: legacy files may not have these keys; that's fine
	return st, seen, nil
}

func saveLastState(path string, st perMuniState, seen perMuniSeen) error {
	raw := map[string]any{
		"by":        map[string][]string{},
		"seen":      map[string]map[string]string{},
		"status":    map[string]string{},
		"first":     map[string]string{},
		"concluded": map[string]string{},
		// Novo: persistir meios/extra e marcas de sumários
		"means":       map[string]map[string]int{},
		"extra_text":  map[string]string{},
		"last_hourly": lastHourlyMark,
		"last_daily":  lastSummaryDay,
	}
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
	// Save extended maps (já existente)
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
	// Novo: persistir meios
	meansOut := raw["means"].(map[string]map[string]int)
	for id, m := range lastMeansByID {
		meansOut[id] = map[string]int{
			"man":     m.Man,
			"terrain": m.Terrain,
			"aerial":  m.Aerial,
			"aquatic": m.Aquatic,
		}
	}
	// Novo: persistir extra
	extraOut := raw["extra_text"].(map[string]string)
	for id, s := range lastExtraByID {
		extraOut[id] = s
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
		raw := getMunicipio(f.Properties)
		mun := normMunicipio(raw)
		if _, ok := wset[mun]; ok {
			out = append(out, f)
			continue
		}
		// Debug: explain why it was skipped
		if getenv("DEBUG", "") != "" || strings.EqualFold(getenv("LOG_LEVEL", ""), "debug") {
			// collect property keys for quick inspection when municipality is missing
			keys := make([]string, 0, len(f.Properties))
			for k := range f.Properties {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			if strings.TrimSpace(raw) == "" {
				debugf("skip: no concelho/municipio field; props keys=%v", keys)
			} else {
				debugf("skip: concelho %q (norm=%q) not in wanted=%v; props keys=%v", raw, mun, wantedFlat, keys)
			}
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
		// Preferir geo: no Android, se ativado
		if getenv("NTFY_CLICK_GEO", "") != "" {
			return fmt.Sprintf("geo:0,0?q=%f,%f", lat, lon)
		}
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

// Novo: adicionar várias tags de uma vez (CSV)
func addTagsCSV(base, addCSV string) string {
	addCSV = strings.TrimSpace(addCSV)
	if addCSV == "" {
		return base
	}
	for _, t := range strings.Split(addCSV, ",") {
		base = addTag(base, strings.TrimSpace(t))
	}
	return base
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
		// reduzir para prioridade default (3) se vier maior
		if strings.TrimSpace(priority) == "" {
			priority = "3"
		} else if v, err := strconv.Atoi(priority); err == nil && v > 3 {
			priority = "3"
		}
		tags = addTag(tags, "zzz")
	}

	// Common: derive actions and optional attach URL from body/click
	// Header-mode requires URL sanitization for commas/semicolons
	sanitizeActionURL := func(u string) string {
		if u == "" {
			return u
		}
		u = strings.ReplaceAll(u, ",", "%2C")
		u = strings.ReplaceAll(u, ";", "%3B")
		return u
	}
	// Build actions for both header- and JSON-mode
	actionsHeader := []string{}
	var actionsJSON []map[string]any
	addAction := func(label, u string) {
		if strings.TrimSpace(u) == "" {
			return
		}
		// Header
		actionsHeader = append(actionsHeader, fmt.Sprintf("view, %s, %s", label, sanitizeActionURL(u)))
		// JSON
		actionsJSON = append(actionsJSON, map[string]any{
			"action": "view",
			"label":  label,
			"url":    u,
			"clear":  true,
		})
	}
	if clickURL != "" {
		addAction("Abrir Mapa", clickURL)
	}
	if urlFogos := extractFogosURLFromBody(body); urlFogos != "" {
		addAction("Abrir Fogos", urlFogos)
	}
	var attachAreaURL string
	if v := extractURLAfterPrefix(body, "Área URL: "); v != "" {
		addAction("Abrir área", v)
		attachAreaURL = v
	} else if v2 := extractURLAfterPrefix(body, "Area URL: "); v2 != "" { // fallback sem acento
		addAction("Abrir area", v2)
		attachAreaURL = v2
	}

	useJSON := getenv("NTFY_JSON", "") != ""
	// Normalize tags to slice for JSON mode
	splitTags := func(csv string) []string {
		if strings.TrimSpace(csv) == "" {
			return nil
		}
		seen := map[string]struct{}{}
		out := []string{}
		for _, t := range strings.Split(csv, ",") {
			tt := strings.TrimSpace(t)
			if tt == "" {
				continue
			}
			if _, ok := seen[tt]; ok {
				continue
			}
			seen[tt] = struct{}{}
			out = append(out, tt)
		}
		return out
	}
	// Priority number for JSON mode
	prNum := 3
	if p := strings.TrimSpace(priority); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			prNum = v
		}
	}

	if useJSON {
		// JSON publishing: POST to root with topic in body
		endpoint := strings.TrimRight(ntfyURL, "/") + "/"
		payload := map[string]any{
			"topic":    topic,
			"message":  body,
			"title":    title,
			"priority": prNum,
		}
		if clickURL != "" {
			payload["click"] = clickURL
		}
		if tg := splitTags(tags); len(tg) > 0 {
			payload["tags"] = tg
		}
		if getenv("NTFY_MARKDOWN", "") != "" {
			payload["markdown"] = true
		}
		if icon := getenv("NTFY_ICON_URL", ""); icon != "" {
			payload["icon"] = icon
		}
		if email := getenv("NTFY_EMAIL", ""); email != "" {
			payload["email"] = email
		}
		if getenv("NTFY_ATTACH_AREA", "") != "" && attachAreaURL != "" {
			payload["attach"] = attachAreaURL
		}
		if len(actionsJSON) > 0 && getenv("NTFY_ACTIONS", "1") != "0" {
			payload["actions"] = actionsJSON
		}
		b, _ := json.Marshal(payload)
		req, _ := http.NewRequest("POST", endpoint, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
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
		return
	}

	// Default: header-based publishing (existing behavior)
	endpoint := strings.TrimRight(ntfyURL, "/") + "/" + topic
	// Markdown opcional
	useMarkdown := getenv("NTFY_MARKDOWN", "")
	ct := "text/plain; charset=utf-8"
	if useMarkdown != "" {
		ct = "text/markdown; charset=utf-8"
	}
	req, _ := http.NewRequest("POST", endpoint, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", ct)
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
	// Headers extra suportados pelo ntfy (via env)
	if useMarkdown != "" {
		req.Header.Set("Markdown", "yes")
	}
	if icon := getenv("NTFY_ICON_URL", ""); icon != "" {
		req.Header.Set("Icon", icon)
	}
	if email := getenv("NTFY_EMAIL", ""); email != "" {
		req.Header.Set("Email", email)
	}
	if cacheCtl := getenv("NTFY_CACHE", ""); cacheCtl != "" {
		req.Header.Set("Cache", cacheCtl) // e.g., "no"
	}
	if fb := getenv("NTFY_FIREBASE", ""); fb != "" {
		req.Header.Set("Firebase", fb) // e.g., "no"
	}
	if getenv("NTFY_ATTACH_AREA", "") != "" && attachAreaURL != "" {
		req.Header.Set("Attach", attachAreaURL)
	}
	if len(actionsHeader) > 0 && getenv("NTFY_ACTIONS", "1") != "0" {
		req.Header.Set("Actions", strings.Join(actionsHeader, "; "))
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
			if _, bad := exc[int(scF)]; bad {
				return false
			}
		}
	}
	// Extras: include/exclude por naturezaCode (ex.: 3101)
	if incCodes := parseStrSetFromEnv("INCLUDE_NATUREZA_CODE"); len(incCodes) > 0 {
		code := strings.ToLower(stripAccents(getPropStr(p, "naturezaCode")))
		if _, ok := incCodes[code]; !ok {
			return false
		}
	}
	if excCodes := parseStrSetFromEnv("EXCLUDE_NATUREZA_CODE"); len(excCodes) > 0 {
		code := strings.ToLower(stripAccents(getPropStr(p, "naturezaCode")))
		if _, ok := excCodes[code]; ok {
			return false
		}
	}
	// INCLUDE_STATUS / EXCLUDE_STATUS (por nome; substring)
	if incS := parseStrSetFromEnv("INCLUDE_STATUS"); len(incS) > 0 {
		cur := strings.ToLower(stripAccents(getPropStr(p, "status")))
		ok := false
		for want := range incS {
			if want == "" || strings.Contains(cur, want) || cur == want {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if excS := parseStrSetFromEnv("EXCLUDE_STATUS"); len(excS) > 0 {
		cur := strings.ToLower(stripAccents(getPropStr(p, "status")))
		for bad := range excS {
			if bad != "" && (strings.Contains(cur, bad) || cur == bad) {
				return false
			}
		}
	}
	// INCLUDE_NATUREZA (por nome; já existia)
	if inc := parseStrSetFromEnv("INCLUDE_NATUREZA"); len(inc) > 0 {
		nz := strings.ToLower(stripAccents(getPropStr(p, "natureza")))
		nzc := strings.ToLower(stripAccents(getPropStr(p, "naturezaCode")))
		if _, ok := inc[nz]; ok {
			return true
		}
		if _, ok := inc[nzc]; ok {
			return true
		}
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
	hf := get("heliFight")
	hc := get("heliCoord")
	pf := get("planeFight")
	// thresholds (0 disables)
	thMan, _ := strconv.Atoi(getenv("MIN_MAN", "0"))
	thTer, _ := strconv.Atoi(getenv("MIN_TERRAIN", "0"))
	thAir, _ := strconv.Atoi(getenv("MIN_AERIAL", "0"))
	thAq, _ := strconv.Atoi(getenv("MIN_AQUATIC", "0"))
	tags := baseTags
	prio := basePriority
	// ntfy: 5 = máx/urgente, 3 = default, 1 = min → elevar prioridade quando n > cur
	inc := func(n int) {
		if n <= 0 {
			return
		}
		cur := 3
		if strings.TrimSpace(prio) != "" {
			if v, err := strconv.Atoi(prio); err == nil {
				cur = v
			}
		}
		if n > cur {
			prio = strconv.Itoa(n)
		}
	}
	// Melhor mapeamento de emojis
	if thMan > 0 && man >= thMan {
		tags = addTag(tags, "busts_in_silhouette")
		inc(4)
	}
	if thTer > 0 && ter >= thTer {
		tags = addTag(tags, "deciduous_tree")
		inc(4)
	}
	if thAir > 0 && air >= thAir {
		tags = addTag(tags, "small_airplane")
		inc(5)
	}
	if thAq > 0 && aq >= thAq {
		tags = addTag(tags, "ocean")
		inc(4)
	}
	// aeronaves dedicadas
	if hf > 0 || hc > 0 {
		tags = addTag(tags, "helicopter")
		inc(5)
	}
	if pf > 0 {
		tags = addTag(tags, "airplane")
		inc(5)
	}
	// importante
	if imp := strings.ToLower(strings.TrimSpace(getPropStr(p, "important"))); imp == "true" || imp == "1" {
		tags = addTag(tags, "exclamation")
		inc(5)
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
	lastStatusByID = map[string]string{}
	firstSeenByID  = map[string]time.Time{}
	concludedAtID  = map[string]time.Time{}

	// Removido: lastSummaryHour (causava repetição quando re-inicializado)
	// lastSummaryHour int
	lastSummaryDay string

	// Novo: marca do último sumário horário enviado ("YYYY-MM-DD HH"), persistente
	lastHourlyMark string

	// Novo: último snapshot de meios/extra por ID, persistente
	lastMeansByID = map[string]Means{}
	lastExtraByID = map[string]string{}
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

	// update last-seen for current active IDs e recolher eventos
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

	// Novo: eventos de atualização de meios e extra
	type meansEvent struct {
		muniKey string
		disp    string
		id      string
		old     Means
		new     Means
		f       Feature
	}
	type extraEvent struct {
		muniKey string
		disp    string
		id      string
		old     string
		new     string
		f       Feature
	}
	meansEvents := make([]meansEvent, 0, 8)
	extraEvents := make([]extraEvent, 0, 8)

	for muniKey, feats := range perMuniNew {
		for _, f := range feats {
			id := getID(f.Properties)
			if id == "" {
				if getenv("DEBUG", "") != "" || strings.EqualFold(getenv("LOG_LEVEL", ""), "debug") {
					debugf("skip: feature without ID in muniKey=%s; props keys=%v", muniKey, func() []string {
						ks := make([]string, 0, len(f.Properties))
						for k := range f.Properties {
							ks = append(ks, k)
						}
						sort.Strings(ks)
						return ks
					}())
				}
				continue
			}
			// mark last seen
			if seen[muniKey] == nil {
				seen[muniKey] = map[string]time.Time{}
			}
			seen[muniKey][id] = now

			// Novo: ler meios atuais
			getInt := func(name string) int {
				if v, ok := toFloat(f.Properties[name]); ok {
					return int(v)
				}
				return 0
			}
			curMeans := Means{
				Man:     getInt("man"),
				Terrain: getInt("terrain"),
				Aerial:  getInt("aerial"),
				Aquatic: getInt("meios_aquaticos"),
			}
			curExtra := getPropStr(f.Properties, "extra")

			// new incident
			_, existed := st[muniKey][id]
			if !existed {
				st[muniKey][id] = struct{}{}
				when := prettyTime(f.Properties["dateTime"])
				disp := getMunicipio(f.Properties)
				if disp == "" {
					disp = muniKey
				}
				if getenv("DEBUG", "") != "" || strings.EqualFold(getenv("LOG_LEVEL", ""), "debug") {
					debugf("new: muniKey=%s id=%s disp=%s", muniKey, id, disp)
				}
				events = append(events, newEvent{muniKey: muniKey, disp: disp, id: id, when: when, f: f})
				if _, ok := firstSeenByID[id]; !ok {
					firstSeenByID[id] = now
				}
			} else {
				// Novo: detetar alterações de meios e extra (só após já existir)
				if prev, ok := lastMeansByID[id]; ok {
					if prev != curMeans {
						meansEvents = append(meansEvents, meansEvent{
							muniKey: muniKey, disp: getMunicipio(f.Properties), id: id,
							old: prev, new: curMeans, f: f,
						})
					}
				}
				if prevX, ok := lastExtraByID[id]; ok {
					if strings.TrimSpace(prevX) != strings.TrimSpace(curExtra) {
						extraEvents = append(extraEvents, extraEvent{
							muniKey: muniKey, disp: getMunicipio(f.Properties), id: id,
							old: prevX, new: curExtra, f: f,
						})
					}
				}
			}
			// Atualizar snapshots sempre no fim
			lastMeansByID[id] = curMeans
			lastExtraByID[id] = curExtra

			// Status change detection — forçar envio na primeira vez que o vemos
			curStatus := getPropStr(f.Properties, "status")
			prev := lastStatusByID[id]
			forceFirstSeenStatus := !existed
			if curStatus != "" && (curStatus != prev || forceFirstSeenStatus) {
				statusEvents = append(statusEvents, newEvent{
					muniKey: muniKey,
					disp:    getMunicipio(f.Properties),
					id:      id,
					when:    prettyTime(f.Properties["updated"]),
					f:       f,
					prev:    prev,
					cur:     curStatus,
				})
				if prev != "" && curStatus != prev {
					statusTransitions.WithLabelValues(prev, curStatus).Inc()
				}
				lastStatusByID[id] = curStatus
				if strings.EqualFold(curStatus, "Conclusão") || strings.Contains(strings.ToLower(stripAccents(curStatus)), "conclus") {
					concludedAtID[id] = now
					if t0, ok := firstSeenByID[id]; ok && now.After(t0) {
						timeToConclusion.Observe(now.Sub(t0).Seconds())
					}
				}
			}
		}
	}

	anyChange := len(events) > 0 || len(statusEvents) > 0 || len(meansEvents) > 0 || len(extraEvents) > 0

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

			// NEW: não perder transições de estado na agregação
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
				infoTags, extraLines := extraInfoTags(p)
				if len(extraLines) > 0 {
					body += "\n" + strings.Join(extraLines, "\n")
				}
				pr := priority
				s := strings.ToLower(stripAccents(curStatus))
				if strings.Contains(s, "em curso") || strings.Contains(s, "em resolucao") {
					pr = "5"
				} else if strings.Contains(s, "despacho") {
					pr = "4"
				} else if strings.Contains(s, "vigilancia") || strings.Contains(s, "conclus") {
					pr = "3"
				}
				tg, pr2 := enrichMeansTagsAndPriority(p, addTagsCSV(tags, infoTags), pr)
				if strings.Contains(s, "conclus") {
					tg = addTag(tg, "white_check_mark")
				}
				postNtfyExt(ntfyURL, topic, title, body, tg, pr2, mapsURLForFeature(ev.f, ev.disp))
			}
		} else {
			for _, ev := range events {
				p := ev.f.Properties
				status := getPropStr(p, "status", "phase", "estado")
				nature := getPropStr(p, "natureza", "type", "tipo")
				man := getPropStr(p, "man")
				ter := getPropStr(p, "terrain")
				air := getPropStr(p, "aerial")
				aq := getPropStr(p, "meios_aquaticos")
				hf := getPropStr(p, "heliFight")
				hc := getPropStr(p, "heliCoord")
				pf := getPropStr(p, "planeFight")
				title := fmt.Sprintf("Novo em %s — %s", ev.disp, nature)
				if ev.when != "" {
					title += " (" + ev.when + ")"
				}
				body := fmt.Sprintf("ID: %s\nMunicípio: %s\nEstado: %s\nMeios: man=%s, ter=%s, air=%s, aq=%s", ev.id, ev.disp, status, man, ter, air, aq)
				// aeronaves (se presentes)
				if hf != "0" || hc != "0" || pf != "0" {
					body += fmt.Sprintf("\nAeronaves: heliFight=%s, heliCoord=%s, planeFight=%s", hf, hc, pf)
				}
				// Extra
				if extra := getPropStr(p, "extra"); extra != "" {
					_, hi := parseExtraTags(extra)
					if hi != "" {
						body += "\nExtra: " + hi
					}
				}
				// Info adicional e tags
				infoTags, extraLines := extraInfoTags(p)
				if len(extraLines) > 0 {
					body += "\n" + strings.Join(extraLines, "\n")
				}
				// KML área
				if kml := getPropStr(p, "kmlVost", "kml"); kml != "" {
					if areaKm2, perKm, areaURL, saved, _ := saveKMLAndCompute(kml, getenv("SAVE_KML_DIR", ""), ev.id); saved {
						body += fmt.Sprintf("\nÁrea: %.2f km², Perímetro: %.1f km", areaKm2, perKm)
						body += "\nÁrea URL: " + areaURL
					}
				}
				body += fmt.Sprintf("\nTotal ativo no alvo: %d", len(filtered))
				clickURL := mapsURLForFeature(ev.f, ev.disp)
				if ev.id != "" {
					body += "\nFogos: https://fogos.pt/fogo/" + ev.id
				}
				// Enriquecer tags/prioridade
				tg, pr := enrichMeansTagsAndPriority(p, addTagsCSV(tags, infoTags), priority)
				// Extra tags do 'extra'
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
				hf := getPropStr(p, "heliFight")
				hc := getPropStr(p, "heliCoord")
				pf := getPropStr(p, "planeFight")
				body := fmt.Sprintf("ID: %s\nMeios: man=%s, ter=%s, air=%s, aq=%s", ev.id, man, ter, air, aq)
				if hf != "0" || hc != "0" || pf != "0" {
					body += fmt.Sprintf("\nAeronaves: heliFight=%s, heliCoord=%s, planeFight=%s", hf, hc, pf)
				}
				// Extra
				if extra := getPropStr(p, "extra"); extra != "" {
					_, hi := parseExtraTags(extra)
					if hi != "" {
						body += "\nExtra: " + hi
					}
				}
				// Info adicional
				infoTags, extraLines := extraInfoTags(p)
				if len(extraLines) > 0 {
					body += "\n" + strings.Join(extraLines, "\n")
				}
				// Fogos link
				if ev.id != "" {
					body += "\nFogos: https://fogos.pt/fogo/" + ev.id
				}
				// Ajuste de prioridade por status
				pr := priority
				s := strings.ToLower(stripAccents(curStatus))
				if strings.Contains(s, "em curso") || strings.Contains(s, "em resolucao") {
					pr = "5"
				} else if strings.Contains(s, "despacho") {
					pr = "4"
				} else if strings.Contains(s, "vigilancia") || strings.Contains(s, "conclus") {
					pr = "3"
				}
				tg, pr2 := enrichMeansTagsAndPriority(p, addTagsCSV(tags, infoTags), pr)
				prevS := strings.ToLower(stripAccents(prev))
				if (strings.Contains(prevS, "conclus") || strings.Contains(prevS, "vigil")) && (strings.Contains(s, "curso") || strings.Contains(s, "despacho")) {
					tg = addTag(tg, "repeat")
					title = "Reativado: " + title
					pr2 = "5"
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

			// Novo: enviar atualizações de meios
			if getenv("NOTIFY_MEANS_CHANGES", "1") != "0" {
				for _, ev := range meansEvents {
					parts := []string{}
					if ev.old.Man != ev.new.Man {
						parts = append(parts, fmt.Sprintf("man: %d → %d", ev.old.Man, ev.new.Man))
					}
					if ev.old.Terrain != ev.new.Terrain {
						parts = append(parts, fmt.Sprintf("ter: %d → %d", ev.old.Terrain, ev.new.Terrain))
					}
					if ev.old.Aerial != ev.new.Aerial {
						parts = append(parts, fmt.Sprintf("air: %d → %d", ev.old.Aerial, ev.new.Aerial))
					}
					if ev.old.Aquatic != ev.new.Aquatic {
						parts = append(parts, fmt.Sprintf("aq: %d → %d", ev.old.Aquatic, ev.new.Aquatic))
					}
					// incluir aeronaves se existirem nos props atuais
					p := ev.f.Properties
					hf := getPropStr(p, "heliFight")
					hc := getPropStr(p, "heliCoord")
					pf := getPropStr(p, "planeFight")
					if hf != "0" || hc != "0" || pf != "0" {
						parts = append(parts, fmt.Sprintf("aeronaves: heliFight=%s, heliCoord=%s, planeFight=%s", hf, hc, pf))
					}
					if len(parts) == 0 {
						continue
					}
					title := fmt.Sprintf("Atualização de meios — %s", ev.disp)
					body := fmt.Sprintf("ID: %s\n%s", ev.id, strings.Join(parts, ", "))
					infoTags, extraLines := extraInfoTags(p)
					if len(extraLines) > 0 {
						body += "\n" + strings.Join(extraLines, "\n")
					}
					tg, pr := enrichMeansTagsAndPriority(p, addTag(tags, infoTags), "3")
					postNtfyExt(ntfyURL, topic, title, body, tg, pr, mapsURLForFeature(ev.f, ev.disp))
				}
			}
			// Novo: enviar alterações no extra
			if getenv("NOTIFY_EXTRA_CHANGES", "1") != "0" {
				for _, ev := range extraEvents {
					// ignorar se ambos vazios
					if strings.TrimSpace(ev.old) == strings.TrimSpace(ev.new) {
						continue
					}
					title := fmt.Sprintf("Atualização — %s", ev.disp)
					body := fmt.Sprintf("ID: %s\nExtra: %s", ev.id, strings.TrimSpace(ev.new))
					// tags adicionais do 'extra' (ex.: estrada cortada)
					more, _ := parseExtraTags(ev.new)
					tg := tags
					for _, t := range more {
						tg = addTag(tg, t)
					}
					postNtfyExt(ntfyURL, topic, title, body, tg, "3", mapsURLForFeature(ev.f, ev.disp))
				}
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
	nowMin := now.Minute()

	// Corrigido: só no minuto 0 e uma vez por hora, persistente
	if getenv("SUMMARY_HOURLY", "1") != "0" {
		hourMark := now.Format("2006-01-02 15")
		if nowMin == 0 && lastHourlyMark != hourMark {
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
			lastHourlyMark = hourMark
		}
	}

	// Corrigido: diário apenas às 08:00 em ponto e uma vez por dia
	if getenv("SUMMARY_DAILY", "1") != "0" && lastSummaryDay != nowDay && nowHour == 8 && nowMin == 0 {
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

// Helpers para enriquecimento de notificações
func relUpdated(p map[string]any) string {
	// "updated": {"sec": ...}
	if m, ok := p["updated"].(map[string]any); ok && m != nil {
		if sec, ok2 := toFloat(m["sec"]); ok2 && sec > 0 {
			t := time.Unix(int64(sec), 0)
			d := time.Since(t)
			if d < 0 {
				d = -d
			}
			if d < time.Minute {
				return "agora"
			}
			return fmt.Sprintf("há %dm", int(d.Minutes()))
		}
	}
	return ""
}

func extraInfoTags(p map[string]any) (addTags string, extraLines []string) {
	// Linhas informativas
	if s := getPropStr(p, "localidade"); s != "" {
		extraLines = append(extraLines, "Localidade: "+s)
	}
	if s := getPropStr(p, "detailLocation"); s != "" {
		extraLines = append(extraLines, "Detalhe: "+s)
	}
	if s := getPropStr(p, "freguesia"); s != "" {
		extraLines = append(extraLines, "Freguesia: "+s)
	}
	if s := getPropStr(p, "dico"); s != "" {
		extraLines = append(extraLines, "DICO: "+s)
	}
	if rg := getPropStr(p, "regiao"); rg != "" || getPropStr(p, "sub_regiao") != "" {
		extraLines = append(extraLines, fmt.Sprintf("Região: %s / %s", rg, getPropStr(p, "sub_regiao")))
	}
	if ru := relUpdated(p); ru != "" {
		extraLines = append(extraLines, "Atualizado: "+ru)
	}

	// ICNF
	if m, ok := p["icnf"].(map[string]any); ok && m != nil {
		if f, ok2 := toFloat(m["altitude"]); ok2 && f > 0 {
			extraLines = append(extraLines, fmt.Sprintf("Altitude: %.0f m", f))
		}
		if b, ok2 := m["fogacho"].(bool); ok2 && b {
			addTags = addTag(addTags, "sparkles")
		}
		if s := getPropStr(m, "fontealerta"); s != "" {
			extraLines = append(extraLines, "Fonte: "+s)
			s2 := strings.ToLower(stripAccents(s))
			if strings.Contains(s2, "112") {
				addTags = addTag(addTags, "telephone")
			}
			if strings.Contains(s2, "popular") {
				addTags = addTag(addTags, "busts_in_silhouette")
			}
		}
	}

	// Aviacao
	if hf, _ := toFloat(p["heliFight"]); hf > 0 {
		addTags = addTag(addTags, "helicopter")
	}
	if hc, _ := toFloat(p["heliCoord"]); hc > 0 {
		addTags = addTag(addTags, "helicopter")
	}
	if pf, _ := toFloat(p["planeFight"]); pf > 0 {
		addTags = addTag(addTags, "airplane")
	}
	// Flag "important"
	if imp := strings.ToLower(strings.TrimSpace(getPropStr(p, "important"))); imp == "true" || imp == "1" {
		addTags = addTag(addTags, "exclamation")
	}
	return
}
