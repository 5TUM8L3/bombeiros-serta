// Fetch active incidents GeoJSON from Fogos.pt and filter by concelho (default "Sertã")
// Bun runtime (fetch, URL, etc.)
import { readFile, writeFile } from "node:fs/promises";

// Torna o endpoint configurável e permite fallbacks por env
const FOGOS_URL = (
  process.env.FOGOS_URL ||
  "https://api.fogos.pt/v2/incidents/active?geojson=true"
).trim();
const FOGOS_FALLBACKS = (process.env.FOGOS_FALLBACK_URLS || "")
  .split(/[,\s]+/)
  .map((s) => s.trim())
  .filter(Boolean);

// Headers mais "amigáveis" a CDNs/WAFs para evitar 403
const DEFAULT_HEADERS: Record<string, string> = {
  Accept: "application/json",
  "User-Agent": "David-Bombeiros/0.2 (Bun)",
  "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
  Referer: "https://fogos.pt/",
  Origin: "https://fogos.pt",
  "Cache-Control": "no-cache",
};

const DEFAULT_MUNICIPIOS = [
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
];
// Variantes toleradas para equivalência
const MUNICIPIO_SYNONYMS: Record<string, string[]> = {
  proencaanova: ["proenca a nova", "proenca-anova", "proenca nova"],
  vilavelhaderodao: ["vila velha de rodao", "v v rodao", "vv rodao"],
  castanheiradepera: ["castanheira de pera", "castanheira pera"],
  pedrogaogrande: ["pedrogao grande", "pedrogao-grande"],
};
const MUNICIPIOS: string[] = (
  process.env.MUNICIPIOS ||
  process.env.MUNICIPIO ||
  DEFAULT_MUNICIPIOS.join(",")
)
  .split(/[;,]/)
  .map((s) => s.trim())
  .filter(Boolean);
const POLL_SECONDS = Number.parseInt(process.env.POLL_SECONDS || "30", 10) || 0; // default 30s; set 0 to run once
const NTFY_URL = process.env.NTFY_URL || "https://ntfy.sh";
const STATE_FILE = process.env.STATE_FILE || "last_ids.json"; // mantém IDs entre reinícios

function norm(s: string) {
  return s
    .normalize("NFD")
    .replace(/\p{Diacritic}+/gu, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "") // remove espaços, hífens e pontuação
    .trim();
}

function getId(p: any): string {
  return String(
    p?.id ??
      p?.identifier ??
      p?.fire ??
      p?.code ??
      Math.random().toString(36).slice(2)
  );
}

function prettyWhen(val: any): string {
  if (val == null) return "";
  if (typeof val === "string") {
    const d = new Date(val);
    if (!Number.isNaN(d.getTime())) return d.toLocaleString();
    return val;
  }
  if (typeof val === "number") {
    const ms = val > 1e12 ? val : val * 1000; // heurística: s vs ms
    const d = new Date(ms);
    if (!Number.isNaN(d.getTime())) return d.toLocaleString();
    return String(val);
  }
  if (typeof val === "object") {
    const cand =
      (val as any).hora ??
      (val as any).hora_alerta ??
      (val as any).updated ??
      (val as any).date ??
      (val as any).time ??
      (val as any).datetime ??
      (val as any).ts ??
      (val as any).at;
    if (cand != null) return prettyWhen(cand);
    return "";
  }
  return String(val);
}

// Substitui a função por uma versão com headers, fallbacks e normalização
async function fetchActiveFeatures() {
  // Candidatos: URL principal, variáveis de ambiente e alguns formatos alternativos comuns
  const candidates = [
    FOGOS_URL,
    ...FOGOS_FALLBACKS,
    "https://api.fogos.pt/v2/incidents/active?format=geojson",
    "https://api.fogos.pt/v2/incidents/active?geojson=1",
    "https://api.fogos.pt/v2/incidents/active",
  ].filter(Boolean);

  let lastErr: unknown;

  for (const url of candidates) {
    try {
      const res = await fetch(url, { headers: DEFAULT_HEADERS });
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        lastErr = new Error(
          `HTTP ${res.status} ${res.statusText}${
            text ? ` - ${text.slice(0, 140)}` : ""
          }`
        );
        console.warn(`Falha ao obter ${url}:`, (lastErr as Error).message);
        continue;
      }
      const data = await res.json().catch((e) => {
        throw new Error(`Falha ao parsear JSON de ${url}: ${e}`);
      });

      const features = toFeatures(data);
      if (Array.isArray(features)) {
        return features;
      }
      lastErr = new Error("Resposta sem features reconhecíveis");
      console.warn(`Forma inesperada em ${url}`);
    } catch (e) {
      lastErr = e;
      console.warn(`Erro ao obter ${url}:`, e);
    }
  }

  throw (
    lastErr ||
    new Error("Falha ao obter dados de todos os endpoints candidatos")
  );
}

// Normaliza várias formas de resposta para um array de Features
function toFeatures(data: any): any[] {
  if (!data) return [];
  if (data.type === "FeatureCollection" && Array.isArray(data.features)) {
    return data.features;
  }
  if (Array.isArray(data)) {
    return data.map((p: any) => {
      const lat = Number(
        p?.latitude ?? p?.lat ?? p?.y ?? p?.coords?.lat ?? p?.location?.lat
      );
      const lon = Number(
        p?.longitude ??
          p?.lng ??
          p?.lon ??
          p?.x ??
          p?.coords?.lng ??
          p?.location?.lng
      );
      const hasGeom = Number.isFinite(lat) && Number.isFinite(lon);
      return {
        type: "Feature",
        geometry: hasGeom ? { type: "Point", coordinates: [lon, lat] } : null,
        properties: p,
      };
    });
  }
  return [];
}

function filterByMunicipios(features: any[], wanted: string[]) {
  const set = new Set<string>();
  for (const w of wanted) {
    const key = norm(w);
    set.add(key);
    const syns = MUNICIPIO_SYNONYMS[key] || [];
    for (const s of syns) set.add(norm(s));
  }
  if ((process.env.DEBUG_MUNI || "").toLowerCase() === "1") {
    const allConcelhos = new Set<string>();
    for (const f of features) {
      const p = f?.properties || {};
      const c = p.concelho || p.municipio || p.county || "";
      if (typeof c === "string") allConcelhos.add(`${c} -> ${norm(c)}`);
    }
    console.log(
      JSON.stringify(
        {
          debugWanted: [...set].sort(),
          debugConcelhos: [...allConcelhos].sort().slice(0, 50),
        },
        null,
        2
      )
    );
  }
  const filtered = features.filter((f: any) => {
    const concelho =
      f?.properties?.concelho ??
      f?.properties?.municipio ??
      f?.properties?.county ??
      f?.properties?.Concelho ??
      f?.properties?.Municipio ??
      f?.properties?.Municipality;
    return typeof concelho === "string" && set.has(norm(concelho));
  });
  return filtered;
}

type PerMuniState = Map<string, Set<string>>; // key: municipio normalizado -> IDs

async function loadLastState(): Promise<PerMuniState | undefined> {
  try {
    const txt = await readFile(STATE_FILE, "utf8");
    const data = JSON.parse(txt);
    const map: PerMuniState = new Map();
    if (Array.isArray(data)) {
      // backward-compat: antiga lista combinada; aplica como baseline a todos
      const set = new Set(data.map((x: any) => String(x)));
      for (const m of MUNICIPIOS) map.set(norm(m), new Set(set));
      return map;
    }
    if (
      data &&
      typeof data === "object" &&
      (data as any).by &&
      typeof (data as any).by === "object"
    ) {
      for (const [k, arr] of Object.entries<any>((data as any).by)) {
        if (Array.isArray(arr)) map.set(String(k), new Set(arr.map(String)));
      }
      return map;
    }
  } catch (_e) {
    // ignore if file not found/invalid
  }
  return undefined;
}

async function saveLastState(state: PerMuniState): Promise<void> {
  try {
    const by: Record<string, string[]> = {};
    for (const [k, v] of state.entries()) by[k] = [...v];
    await writeFile(STATE_FILE, JSON.stringify({ by }), "utf8");
  } catch (e) {
    console.warn("Falha ao gravar estado (last_ids):", e);
  }
}

async function postNtfy({
  title,
  body,
  tags = "",
  priority = "3",
}: {
  title: string;
  body: string;
  tags?: string;
  priority?: string;
}) {
  const topic = process.env.NTFY_TOPIC;
  if (!topic) return;
  try {
    const res = await fetch(
      `${NTFY_URL.replace(/\/$/, "")}/${encodeURIComponent(topic)}`,
      {
        method: "POST",
        headers: {
          Title: title,
          Tags: tags,
          Priority: priority,
        },
        body,
      }
    );
    if (res.ok) {
      console.log("Notificação enviada para ntfy.sh");
    } else {
      const text = await res.text().catch(() => "");
      console.warn(
        `Falha ao enviar notificação ntfy: HTTP ${res.status} ${
          res.statusText
        }${text ? ` - ${text}` : ""}`
      );
    }
  } catch (e) {
    console.warn("Falha ao enviar notificação ntfy:", e);
  }
}

function muniLabel(names: string[]): string {
  if (names.length <= 5) return names.join(", ");
  return `${names.length} municípios alvo`;
}

async function notifyNtfy(features: any[], municipios: string[]) {
  if (!process.env.NTFY_TOPIC || features.length === 0) return;
  const scope = municipios.length === 1 ? municipios[0] : muniLabel(municipios);
  const title = `Ocorrências ativas em ${scope}: ${features.length}`;
  const body = features
    .map((f: any) => {
      const p = f.properties || {};
      const when = prettyWhen(p.hora_alerta ?? p.hora ?? p.updated ?? p.date);
      const concelho = p.concelho || p.municipio || p.county || "";
      const freguesia = p.freguesia || p.local || "";
      const lugar = [concelho, freguesia].filter(Boolean).join(" / ");
      return `#${getId(p)} ${p.natureza ?? p.type ?? "Incêndio"} - ${lugar} ${
        when ? `(${when})` : ""
      }`;
    })
    .join("\n");
  await postNtfy({ title, body, tags: "fire,rotating_light", priority: "5" });
}

async function notifyMunicipio(
  name: string,
  features: any[],
  newOnly: Set<string> | null = null
) {
  if (!process.env.NTFY_TOPIC || features.length === 0) return;
  const all = features;
  const newItems = newOnly
    ? all.filter((f) => newOnly.has(getId(f.properties)))
    : all;
  if (newOnly && newItems.length === 0) return;
  const title = newOnly
    ? `Novas ocorrências em ${name}: ${newItems.length} (total: ${all.length})`
    : `Ocorrências ativas em ${name}: ${all.length}`;
  const body = newItems
    .map((f: any) => {
      const p = f.properties || {};
      const when = prettyWhen(p.hora_alerta ?? p.hora ?? p.updated ?? p.date);
      const concelho = p.concelho || p.municipio || p.county || "";
      const freguesia = p.freguesia || p.local || "";
      const lugar = [concelho, freguesia].filter(Boolean).join(" / ");
      return `#${getId(p)} ${p.natureza ?? p.type ?? "Incêndio"} - ${lugar} ${
        when ? `(${when})` : ""
      }`;
    })
    .join("\n");
  await postNtfy({ title, body, tags: "fire,rotating_light", priority: "5" });
}

async function notifyStartup() {
  if (!process.env.NTFY_TOPIC) return;
  const mode = POLL_SECONDS > 0 ? `polling ${POLL_SECONDS}s` : "execução única";
  const title = `Monitor Fogos iniciado (${muniLabel(MUNICIPIOS)})`;
  const body = `Modo: ${mode}\nHora: ${new Date().toLocaleString()}`;
  await postNtfy({ title, body, tags: "fire,rocket", priority: "3" });
}

// Reutiliza fetchActiveFeatures para evitar segunda chamada potencialmente bloqueada
async function notifyAllActiveNationwide() {
  if (!process.env.NTFY_TOPIC) return;
  try {
    const features = await fetchActiveFeatures();
    const title = `Ocorrências ativas em Portugal: ${features.length}`;
    const body = features
      .map((f: any) => {
        const p = f?.properties || {};
        const when = prettyWhen(p.hora_alerta ?? p.hora ?? p.updated ?? p.date);
        const concelho = p.concelho || p.municipio || p.county || "";
        const freguesia = p.freguesia || p.local || "";
        const lugar = [concelho, freguesia].filter(Boolean).join(" / ");
        return `#${getId(p)} ${p.natureza ?? p.type ?? "Incêndio"} - ${lugar}${
          when ? ` (${when})` : ""
        }`;
      })
      .join("\n");
    await postNtfy({ title, body, tags: "fire,world_map", priority: "4" });
  } catch (e) {
    console.warn("Falha ao obter/avisar ocorrências nacionais:", e);
  }
}

async function runOnce(lastState?: PerMuniState) {
  const all: any[] = await fetchActiveFeatures();
  const features: any[] = filterByMunicipios(all, MUNICIPIOS);
  const grouped: Record<string, any[]> = {};
  for (const f of features) {
    const p = f.properties || {};
    const c = p.concelho || p.municipio || p.county || "";
    const key = norm(String(c));
    (grouped[key] ||= []).push(f);
  }
  const byCounts: Record<string, number> = Object.fromEntries(
    Object.entries(grouped).map(([k, v]) => [k, v.length])
  );
  console.log(
    JSON.stringify({ count: features.length, byMunicipio: byCounts }, null, 2)
  );

  const nextState: PerMuniState = new Map(
    lastState ? [...lastState.entries()] : []
  );
  for (const name of MUNICIPIOS) {
    const key = norm(name);
    const list = grouped[key] || [];
    const ids = new Set<string>(list.map((f: any) => getId(f.properties)));
    const prev = nextState.get(key);
    if (!prev) {
      // primeira vez: notificar o estado atual do município (snapshot)
      if (list.length > 0) {
        const displayName = list[0]?.properties?.concelho || name;
        await notifyMunicipio(String(displayName), list, null);
      }
      nextState.set(key, new Set(ids));
      continue;
    }
    const newIds = new Set<string>([...ids].filter((id) => !prev.has(id)));
    if (newIds.size > 0) {
      const displayName = list[0]?.properties?.concelho || name;
      await notifyMunicipio(String(displayName), list, newIds);
    }
    nextState.set(key, ids);
  }
  return nextState;
}

// Reintroduz a função principal e a invocação para evitar terminar imediatamente
async function main() {
  try {
    // Test-only mode para validar entrega de ntfy rapidamente
    const ntfyTest = (process.env.NTFY_TEST || "").toLowerCase();
    if (ntfyTest === "1" || ntfyTest === "true") {
      await postNtfy({
        title: `Teste de alerta (${muniLabel(MUNICIPIOS)})`,
        body: `Esta é uma mensagem de teste enviada de E:\\David-Bombeiros às ${new Date().toLocaleString()}`,
        tags: "fire,rotating_light",
        priority: "5",
      });
      return;
    }

    // Notificações de arranque
    await notifyStartup();
    await notifyAllActiveNationwide();

    if (POLL_SECONDS > 0) {
      console.log(
        `A monitorizar ${muniLabel(
          MUNICIPIOS
        )} a cada ${POLL_SECONDS}s... (CTRL+C para sair)`
      );
      let lastState: PerMuniState | undefined = await loadLastState();
      while (true) {
        try {
          lastState = await runOnce(lastState);
          if (lastState) await saveLastState(lastState);
        } catch (err) {
          console.error("Erro na execução: ", err);
        }
        await new Promise((r) => setTimeout(r, POLL_SECONDS * 1000));
      }
    } else {
      const prev = await loadLastState();
      const state = await runOnce(prev);
      if (state) await saveLastState(state);
    }
  } catch (err) {
    console.error("Erro:", err);
    process.exitCode = 1;
  }
}

main();
