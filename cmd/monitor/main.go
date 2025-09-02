package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"unicode"
)

type Feature struct {
	Type       string                 `json:"type"`
	Geometry   map[string]any         `json:"geometry"`
	Properties map[string]any         `json:"properties"`
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
	"proencaanova":       {"proenca a nova", "proenca-anova", "proenca nova"},
	"vilavelhaderodao":   {"vila velha de rodao", "v v rodao", "vv rodao"},
	"castanheiradepera":  {"castanheira de pera", "castanheira pera"},
	"pedrogaogrande":     {"pedrogao grande", "pedrogao-grande"},
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
	if strings.Contains(v, ";") { sep = ";" }
	parts := strings.Split(v, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" { out = append(out, p) }
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
			for i, s := range alts { arr[i] = normMunicipio(s) }
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

func doGet(url string) (*http.Response, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil { return nil, err }
	req.Header = defaultHeaders()
	client := &http.Client{ Timeout: 20 * time.Second }
	resp, err := client.Do(req)
	if err != nil { return nil, err }
	if resp.StatusCode >= 400 { return nil, fmt.Errorf("http %d", resp.StatusCode) }
	return resp, nil
}

func fetchActiveFeatures() ([]Feature, error) {
	base := strings.TrimSpace(getenv("FOGOS_URL", "https://api.fogos.pt/v2/incidents/active?geojson=true"))
	fallbacks := strings.FieldsFunc(strings.TrimSpace(os.Getenv("FOGOS_FALLBACK_URLS")), func(r rune) bool { return r == ',' || r == ' ' || r == ';' })
	urls := append([]string{base}, fallbacks...)
	var lastErr error
	for _, u := range urls {
		resp, err := doGet(u)
		if err != nil { lastErr = err; continue }
		data, err := io.ReadAll(resp.Body); resp.Body.Close()
		if err != nil { lastErr = err; continue }
		features, err := toFeatures(data)
		if err != nil { lastErr = err; continue }
		return features, nil
	}
	if lastErr == nil { lastErr = errors.New("no endpoints tried") }
	return nil, lastErr
}

func toFeatures(body []byte) ([]Feature, error) {
	// Try several shapes: FeatureCollection, {data: FeatureCollection}, array
	var fc FeatureCollection
	if err := json.Unmarshal(body, &fc); err == nil && len(fc.Features) > 0 {
		return fc.Features, nil
	}
	var wrap ApiResponse
	if err := json.Unmarshal(body, &wrap); err == nil && wrap.Data != nil {
		b, _ := json.Marshal(wrap.Data)
		if err := json.Unmarshal(b, &fc); err == nil && len(fc.Features) > 0 { return fc.Features, nil }
		var arr []Feature
		if err := json.Unmarshal(b, &arr); err == nil && len(arr) > 0 { return arr, nil }
	}
	var arr []Feature
	if err := json.Unmarshal(body, &arr); err == nil && len(arr) > 0 {
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
				if t != "" { return t }
			case float64:
				if t != 0 { return fmt.Sprintf("%.0f", t) }
			}
		}
	}
	return ""
}

func getMunicipio(p map[string]any) string {
	for _, k := range []string{"concelho", "municipio", "county"} {
		if v, ok := p[k].(string); ok && strings.TrimSpace(v) != "" { return v }
	}
	return ""
}

type perMuniState map[string]map[string]struct{}

func loadLastState(path string) (perMuniState, error) {
	b, err := os.ReadFile(path)
	if err != nil { return perMuniState{}, err }
	var raw map[string]map[string][]string
	if err := json.Unmarshal(b, &raw); err != nil { return perMuniState{}, err }
	st := perMuniState{}
	if m, ok := raw["by"]; ok {
		for muni, ids := range m {
			set := map[string]struct{}{}
			for _, id := range ids { set[id] = struct{}{} }
			st[muni] = set
		}
	}
	return st, nil
}

func saveLastState(path string, st perMuniState) error {
	raw := map[string]map[string][]string{"by": {}}
	for muni, set := range st {
		ids := make([]string, 0, len(set))
		for id := range set { ids = append(ids, id) }
		raw["by"][muni] = ids
	}
	b, _ := json.MarshalIndent(raw, "", "  ")
	if err := os.WriteFile(path, b, 0644); err != nil { return err }
	return nil
}

func filterByMunicipios(features []Feature, wantedFlat []string) []Feature {
	wset := map[string]struct{}{}
	for _, w := range wantedFlat { wset[w] = struct{}{} }
	out := make([]Feature, 0, len(features))
	for _, f := range features {
		mun := normMunicipio(getMunicipio(f.Properties))
		if _, ok := wset[mun]; ok {
			out = append(out, f)
		}
	}
	return out
}

func muniLabel(names []string) string {
	if len(names) == 0 { return "" }
	if len(names) == 1 { return names[0] }
	return strings.Join(names[:len(names)-1], ", ") + " e " + names[len(names)-1]
}

func prettyTime(val any) string {
	if s, ok := val.(string); ok {
		if t, err := time.Parse(time.RFC3339, s); err == nil { return t.Local().Format("02-01 15:04") }
	}
	return ""
}

func postNtfy(ntfyURL, topic, title, body, tags, priority string) {
	if strings.TrimSpace(topic) == "" { return }
	endpoint := strings.TrimRight(ntfyURL, "/") + "/" + topic
	req, _ := http.NewRequest("POST", endpoint, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Title", title)
	if tags != "" { req.Header.Set("Tags", tags) }
	if priority != "" { req.Header.Set("Priority", priority) } else { req.Header.Set("Priority", "3") }
	resp, err := http.DefaultClient.Do(req)
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

func runOnce(statePath string, wantedNames []string) (changed bool, err error) {
	features, err := fetchActiveFeatures()
	if err != nil { return false, err }
	wantedSet, wantedFlat := makeWantedSet(wantedNames)
	filtered := filterByMunicipios(features, wantedFlat)

	// load state
	st, _ := loadLastState(statePath)
	if st == nil { st = perMuniState{} }

	// compute new IDs per muni
	now := time.Now()
	ntfyURL := getenv("NTFY_URL", "https://ntfy.sh")
	topic := "bombeiros-serta" // tópico fixo

	perMuniNew := map[string][]Feature{}
	for _, f := range filtered {
		mun := normMunicipio(getMunicipio(f.Properties))
		// map syns to canonical key if needed
		canon := mun
		for k, alts := range wantedSet {
			for _, a := range alts { if a == mun { canon = k; break } }
		}
		perMuniNew[canon] = append(perMuniNew[canon], f)
	}

	// init existing
	for k := range wantedSet { if _, ok := st[k]; !ok { st[k] = map[string]struct{}{} } }

	// detect new ids and notify
	anyChange := false
	for muniKey, feats := range perMuniNew {
		for _, f := range feats {
			id := getID(f.Properties)
			if id == "" { continue }
			if _, ok := st[muniKey][id]; !ok {
				st[muniKey][id] = struct{}{}
				anyChange = true
				when := prettyTime(f.Properties["dateTime"])
				disp := getMunicipio(f.Properties)
				if disp == "" { disp = muniKey }
				title := fmt.Sprintf("Novo incidente em %s", disp)
				if when != "" { title += " (" + when + ")" }
				body := fmt.Sprintf("ID: %s\nMunicípio: %s\nTotal ativo no alvo: %d", id, disp, len(filtered))
				postNtfy(ntfyURL, topic, title, body, "fire,rotating_light", "5")
			}
		}
	}

	// Optionally send a summary on first run without state
	_ = saveLastState(statePath, st)
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
		postNtfy(getenv("NTFY_URL", "https://ntfy.sh"), "bombeiros-serta", "[teste] monitor iniciado", time.Now().Format(time.RFC3339), "white_check_mark", "3")
	}

	if pollSec <= 0 {
		_, err := runOnce(stateFile, wanted)
		if err != nil { fmt.Fprintln(os.Stderr, "Erro:", err); os.Exit(1) }
		return
	}

	ticker := time.NewTicker(time.Duration(pollSec) * time.Second)
	defer ticker.Stop()
	for {
		if _, err := runOnce(stateFile, wanted); err != nil {
			fmt.Fprintln(os.Stderr, "Erro:", err)
		}
		<-ticker.C
	}
}
