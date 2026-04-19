<<<<<<< HEAD
require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const axios = require("axios");
const twilio = require("twilio");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { google } = require("googleapis");
const { DateTime } = require("luxon");
const OpenAI = require("openai");

const app = express();
app.disable("x-powered-by");
app.set("trust proxy", 1);

app.use(express.urlencoded({ extended: false }));
app.use(express.json({ limit: "300kb" }));

// ======================================================
// CONFIG
// ======================================================
const PORT = Number(process.env.PORT || 3000);
const NODE_ENV = process.env.NODE_ENV || "development";
const APP_BASE_URL = process.env.APP_BASE_URL || "";
const TZ = process.env.APP_TIMEZONE || "Europe/Paris";

const ADMIN_USER = process.env.ADMIN_USER || "";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "";

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const CONTACTS_FILE = path.join(DATA_DIR, "contacts.json");

const SESSION_TTL_MINUTES = Number(process.env.SESSION_TTL_MINUTES || 30);
const MAX_CONTACTS = Number(process.env.MAX_CONTACTS || 5000);

const TWILIO_VALIDATE_SIGNATURE =
  String(process.env.TWILIO_VALIDATE_SIGNATURE || "true").toLowerCase() !==
  "false";

const OPENAI_INTENT_MODEL = process.env.OPENAI_INTENT_MODEL || "gpt-4o-mini";
const OPENAI_CITY_MODEL = process.env.OPENAI_CITY_MODEL || "gpt-4o-mini";
const OPENAI_REF_MODEL = process.env.OPENAI_REF_MODEL || "gpt-4o-mini";

const APIMO_API_TOKEN = String(process.env.APIMO_API_TOKEN || "").trim();
const APIMO_PROVIDER_ID = String(process.env.APIMO_PROVIDER_ID || "").trim();
const APIMO_AGENCY_ID = String(process.env.APIMO_AGENCY_ID || "").trim();
const APIMO_BASE_URL = process.env.APIMO_BASE_URL || "https://api.apimo.pro";
const APIMO_CACHE_TTL_MS = Number(process.env.APIMO_CACHE_TTL_MS || 60000);

const OPENWEATHER_API_KEY = process.env.OPENWEATHER_API_KEY || "";
const WEATHER_CACHE_TTL_MS = Number(process.env.WEATHER_CACHE_TTL_MS || 600000);

const SWIXIM_BASE_URL = process.env.SWIXIM_BASE_URL || "https://www.swixim.com";
const SWIXIM_AGENCY_URL =
  process.env.SWIXIM_AGENCY_URL ||
  "https://www.swixim.com/fr/agences/details/35/swixim-strasbourg";

const GOOGLE_CALENDAR_ID = process.env.GOOGLE_CALENDAR_ID || "primary";
const APPOINTMENT_DURATION_MINUTES = Number(
  process.env.APPOINTMENT_DURATION_MINUTES || 30
);

const REQUIRED_PROD_SECRETS = [
  "ADMIN_USER",
  "ADMIN_PASSWORD",
  "TWILIO_ACCOUNT_SID",
  "TWILIO_AUTH_TOKEN",
  "GOOGLE_CLIENT_EMAIL",
  "GOOGLE_PRIVATE_KEY",
  "APIMO_API_TOKEN",
  "APIMO_PROVIDER_ID",
  "APIMO_AGENCY_ID",
];

const KNOWN_CITIES = [
  "strasbourg",
  "erstein",
  "bischheim",
  "schiltigheim",
  "illkirch graffenstaden",
  "illkirch-graffenstaden",
  "lingolsheim",
  "hoerdt",
  "guemar",
  "wolfisheim",
  "cernay",
  "mulhouse",
  "saverne",
  "haguenau",
  "selestat",
  "molsheim",
  "obernai",
  "colmar",
  "benfeld",
  "geispolsheim",
  "ostwald",
  "brumath",
  "vendenheim",
  "la wantzenau",
  "entzheim",
  "kehl",
];

// ======================================================
// SECURITY
// ======================================================
app.use(
  helmet({
    contentSecurityPolicy: false,
  })
);

app.use(
  rateLimit({
    windowMs: 15 * 60 * 1000,
    max: NODE_ENV === "production" ? 300 : 5000,
    standardHeaders: true,
    legacyHeaders: false,
  })
);

app.use(
  "/admin",
  rateLimit({
    windowMs: 15 * 60 * 1000,
    max: NODE_ENV === "production" ? 100 : 1000,
    standardHeaders: true,
    legacyHeaders: false,
  })
);

// ======================================================
// CLIENTS
// ======================================================
const openai =
  process.env.OPENAI_API_KEY && String(process.env.OPENAI_API_KEY).trim()
    ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY })
    : null;

const apimo = axios.create({
  baseURL: APIMO_BASE_URL,
  auth: {
    username: APIMO_PROVIDER_ID,
    password: APIMO_API_TOKEN,
  },
  timeout: 10000,
  headers: {
    Accept: "application/json",
  },
  validateStatus: () => true,
});

// ======================================================
// LOGGING
// ======================================================
function businessLog(event, payload = {}) {
  console.log(
    `[${new Date().toISOString()}] ${event}`,
    JSON.stringify(payload, null, 2)
  );
}

// ======================================================
// STORAGE
// ======================================================
function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function safeWriteJson(file, data) {
  const tmp = `${file}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2), "utf-8");
  fs.renameSync(tmp, file);
}

function initStorage() {
  ensureDir(DATA_DIR);
  if (!fs.existsSync(CONTACTS_FILE)) {
    safeWriteJson(CONTACTS_FILE, []);
  }
}

function readJsonFile(file, fallback) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf-8"));
  } catch {
    return fallback;
  }
}

function writeJsonFile(file, data) {
  safeWriteJson(file, data);
}

function readContacts() {
  return readJsonFile(CONTACTS_FILE, []);
}

function writeContacts(items) {
  const next = Array.isArray(items) ? items.slice(0, MAX_CONTACTS) : [];
  writeJsonFile(CONTACTS_FILE, next);
}

function saveContactEntry(entry) {
  const items = readContacts();
  items.unshift(entry);
  writeContacts(items);
}

function upsertContactEntryByPredicate(predicate, createOrUpdate) {
  const items = readContacts();
  const index = items.findIndex(predicate);

  if (index === -1) {
    const created = createOrUpdate(null);
    items.unshift(created);
    writeContacts(items);
    return created;
  }

  items[index] = createOrUpdate(items[index]);
  writeContacts(items);
  return items[index];
}

function updateContactEntry(id, updater) {
  const items = readContacts();
  const index = items.findIndex((item) => item.id === id);
  if (index === -1) return null;
  items[index] = updater(items[index]);
  writeContacts(items);
  return items[index];
}

function deleteContactEntry(id) {
  const items = readContacts();
  const next = items.filter((item) => item.id !== id);
  if (next.length === items.length) return false;
  writeContacts(next);
  return true;
}

initStorage();

// ======================================================
// SESSIONS
// ======================================================
const sessions = new Map();

function getCallSid(req) {
  return req.body?.CallSid || "unknown";
}

function getNowParis() {
  return DateTime.now().setZone(TZ);
}

function cleanupExpiredSessions() {
  const now = Date.now();
  const ttlMs = SESSION_TTL_MINUTES * 60 * 1000;

  for (const [callSid, session] of sessions.entries()) {
    const touchedAt = new Date(
      session.touchedAt || session.createdAt || 0
    ).getTime();
    if (!touchedAt || now - touchedAt > ttlMs) {
      sessions.delete(callSid);
    }
  }
}

function getSession(callSid) {
  cleanupExpiredSessions();

  if (!sessions.has(callSid)) {
    sessions.set(callSid, {
      flow: null,
      unclearCount: 0,
      createdAt: new Date().toISOString(),
      touchedAt: new Date().toISOString(),
      data: {
        name: "",
        phone: "",
        city: "",
        refDigits: "",
        message: "",
        messageMode: "",
        dateText: "",
        timeText: "",
        calendarEventId: "",
        calendarHtmlLink: "",
        pendingVoiceMessageId: "",
      },
    });
  }

  const session = sessions.get(callSid);
  session.touchedAt = new Date().toISOString();
  return session;
}

function resetSession(callSid) {
  sessions.delete(callSid);
}

setInterval(cleanupExpiredSessions, 5 * 60 * 1000).unref();

// ======================================================
// CACHES
// ======================================================
const propertiesCache = {
  data: [],
  fetchedAt: 0,
};

function isApimoCacheFresh() {
  return (
    Array.isArray(propertiesCache.data) &&
    propertiesCache.data.length > 0 &&
    Date.now() - propertiesCache.fetchedAt < APIMO_CACHE_TTL_MS
  );
}

const weatherCache = new Map();

function getWeatherCacheKey(city = "") {
  return normalize(city);
}

function getCachedWeather(city = "") {
  const key = getWeatherCacheKey(city);
  const item = weatherCache.get(key);
  if (!item) return null;

  if (Date.now() - item.fetchedAt > WEATHER_CACHE_TTL_MS) {
    weatherCache.delete(key);
    return null;
  }

  return item.value;
}

function setCachedWeather(city = "", value = "") {
  const key = getWeatherCacheKey(city);
  weatherCache.set(key, {
    value,
    fetchedAt: Date.now(),
  });
}

// ======================================================
// UTILS
// ======================================================
function vr() {
  return new twilio.twiml.VoiceResponse();
}

function normalize(text = "") {
  return String(text)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^\w\s+/:.\-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanText(text = "") {
  return String(text).replace(/\s+/g, " ").trim();
}

function onlyDigits(text = "") {
  return String(text).replace(/\D/g, "");
}

function normalizeFrenchPhone(raw = "") {
  const digits = onlyDigits(raw);

  if (!digits) return "";
  if (digits.startsWith("00")) return `+${digits.slice(2)}`;
  if (digits.startsWith("33") && digits.length >= 11 && digits.length <= 15) {
    return `+${digits}`;
  }
  if (digits.startsWith("0") && digits.length === 10) return `+33${digits.slice(1)}`;
  if (digits.length === 9) return `+33${digits}`;
  if (digits.length >= 10 && digits.length <= 15) return `+${digits}`;

  return "";
}

function looksLikePhone(raw = "") {
  const normalizedPhone = normalizeFrenchPhone(raw);
  const digits = onlyDigits(normalizedPhone);
  return digits.length >= 11 && digits.length <= 15;
}

function normalizeWaRecipientPhone(raw = "") {
  const phone = normalizeFrenchPhone(raw);
  return onlyDigits(phone);
}

function isSafeId(value = "") {
  return /^[a-zA-Z0-9_-]{1,100}$/.test(String(value));
}

function sayFr(node, text) {
  node.say({ language: "fr-FR" }, text);
}

function gatherSpeech(response, action, prompt, hints = "") {
  const gather = response.gather({
    input: "speech dtmf",
    action,
    method: "POST",
    language: "fr-FR",
    speechTimeout: "auto",
    timeout: 6,
    hints,
  });

  sayFr(gather, prompt);
  return gather;
}

function hangupWithMessage(response, text) {
  sayFr(response, text);
  response.hangup();
}

function escapeHtml(value = "") {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatLocalDateTime(isoString = "") {
  if (!isoString) return "";
  const dt = DateTime.fromISO(isoString, { zone: "utc" }).setZone(TZ);
  if (!dt.isValid) return isoString;
  return dt.setLocale("fr").toLocaleString(DateTime.DATETIME_SHORT);
}

function spellDigitsForSpeech(digits = "") {
  return String(digits).split("").join(" ");
}

function monthNameToNumber(name) {
  const months = {
    janvier: 1,
    fevrier: 2,
    mars: 3,
    avril: 4,
    mai: 5,
    juin: 6,
    juillet: 7,
    aout: 8,
    septembre: 9,
    octobre: 10,
    novembre: 11,
    decembre: 12,
  };
  return months[name] || null;
}

function safeEqualString(a = "", b = "") {
  const aBuf = Buffer.from(String(a));
  const bBuf = Buffer.from(String(b));
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

function getRequestAbsoluteUrl(req) {
  if (APP_BASE_URL) {
    const base = APP_BASE_URL.replace(/\/$/, "");
    return `${base}${req.originalUrl}`;
  }

  const proto = req.headers["x-forwarded-proto"] || req.protocol || "http";
  const host = req.headers["x-forwarded-host"] || req.get("host");
  return `${proto}://${host}${req.originalUrl}`;
}

function validateTwilioWebhook(req) {
  if (!TWILIO_VALIDATE_SIGNATURE) return true;

  const authToken = process.env.TWILIO_AUTH_TOKEN;
  const signature = req.headers["x-twilio-signature"];

  if (!authToken || !signature) return false;

  const url = getRequestAbsoluteUrl(req);

  try {
    return twilio.validateRequest(authToken, signature, url, req.body || {});
  } catch (error) {
    console.error("Erreur validation Twilio signature", error);
    return false;
  }
}

function twilioOnly(handler) {
  return (req, res, next) => {
    if (!validateTwilioWebhook(req)) {
      return res.status(403).send("Forbidden");
    }
    return handler(req, res, next);
  };
}

function requireEnvInProduction() {
  if (NODE_ENV !== "production") return;

  const missing = REQUIRED_PROD_SECRETS.filter((key) => !process.env[key]);
  if (missing.length) {
    throw new Error(
      `Variables d'environnement manquantes en production: ${missing.join(", ")}`
    );
  }
}

function checkEnvAtStartup() {
  const missing = [];
  if (!APIMO_API_TOKEN) missing.push("APIMO_API_TOKEN");
  if (!APIMO_PROVIDER_ID) missing.push("APIMO_PROVIDER_ID");
  if (!APIMO_AGENCY_ID) missing.push("APIMO_AGENCY_ID");

  if (missing.length) {
    throw new Error(`Variables manquantes: ${missing.join(", ")}`);
  }
}

function checkBasicAuth(req) {
  if (!ADMIN_USER || !ADMIN_PASSWORD) {
    return { ok: false, status: 503, message: "Admin non configuré" };
  }

  const auth = req.headers.authorization;
  if (!auth || !auth.startsWith("Basic ")) {
    return { ok: false, status: 401, message: "Authentification requise" };
  }

  const base64 = auth.split(" ")[1];
  const decoded = Buffer.from(base64, "base64").toString("utf-8");
  const separatorIndex = decoded.indexOf(":");

  if (separatorIndex === -1) {
    return { ok: false, status: 401, message: "Accès refusé" };
  }

  const username = decoded.slice(0, separatorIndex);
  const password = decoded.slice(separatorIndex + 1);

  if (!safeEqualString(username, ADMIN_USER) || !safeEqualString(password, ADMIN_PASSWORD)) {
    return { ok: false, status: 401, message: "Accès refusé" };
  }

  return { ok: true };
}

// ======================================================
// FRENCH NUMBER PARSING
// ======================================================
const NUMBER_WORDS = {
  zero: 0,
  un: 1,
  une: 1,
  deux: 2,
  trois: 3,
  quatre: 4,
  cinq: 5,
  six: 6,
  sept: 7,
  huit: 8,
  neuf: 9,
  dix: 10,
  onze: 11,
  douze: 12,
  treize: 13,
  quatorze: 14,
  quinze: 15,
  seize: 16,
  dixsept: 17,
  dixhuit: 18,
  dixneuf: 19,
  vingt: 20,
  trente: 30,
  quarante: 40,
  cinquante: 50,
};

function parseFrenchNumberWords(text = "") {
  const cleaned = normalize(text)
    .replace(/vingt et un/g, "vingt un")
    .replace(/trente et un/g, "trente un")
    .replace(/quarante et un/g, "quarante un")
    .replace(/cinquante et un/g, "cinquante un");

  const parts = cleaned.split(/[\s\-]+/).filter(Boolean);
  let total = 0;
  let current = 0;
  let used = false;

  for (const part of parts) {
    if (part === "et") continue;

    if (part === "cent" || part === "cents") {
      current = (current || 1) * 100;
      used = true;
      continue;
    }

    if (part === "mille") {
      total += (current || 1) * 1000;
      current = 0;
      used = true;
      continue;
    }

    const compact = part.replace(/[\s\-]/g, "");
    if (compact in NUMBER_WORDS) {
      current += NUMBER_WORDS[compact];
      used = true;
      continue;
    }

    if (/^\d+$/.test(part)) {
      current += Number(part);
      used = true;
      continue;
    }

    return { ok: false };
  }

  if (!used) return { ok: false };
  return { ok: true, value: total + current };
}

// ======================================================
// AI HELPERS
// ======================================================
function detectIntentByRules(text = "") {
  const t = normalize(text);

  if (
    t.includes("fiche") ||
    t.includes("descriptive") ||
    t.includes("descriptif") ||
    t.includes("description du bien")
  ) {
    return "fiche";
  }

  if (
    t.includes("message") ||
    t.includes("laisser un message") ||
    t.includes("message vocal") ||
    t.includes("conseiller")
  ) {
    return "message";
  }

  if (
    t.includes("rendez vous") ||
    t.includes("rendezvous") ||
    t.includes("rendez") ||
    t.includes("rdv")
  ) {
    return "rdv";
  }

  return null;
}

async function detectIntentWithAI(text = "") {
  if (!openai || !text.trim()) return null;

  try {
    const response = await openai.responses.create({
      model: OPENAI_INTENT_MODEL,
      store: false,
      max_output_tokens: 20,
      input: [
        {
          role: "system",
          content: [
            {
              type: "input_text",
              text:
                "Classe l'intention utilisateur en un seul mot parmi: fiche, message, rdv, unknown. Réponds uniquement par un de ces quatre mots.",
            },
          ],
        },
        {
          role: "user",
          content: [{ type: "input_text", text }],
        },
      ],
    });

    const out = (response.output_text || "").trim().toLowerCase();
    if (["fiche", "message", "rdv"].includes(out)) return out;
    return null;
  } catch (error) {
    console.error("detectIntentWithAI error", error.message || error);
    return null;
  }
}

async function detectIntent(text = "") {
  const byRules = detectIntentByRules(text);
  if (byRules) return byRules;
  return await detectIntentWithAI(text);
}

function normalizeCityDeterministic(raw = "") {
  const text = normalize(raw);
  if (!text) return "";

  const exact = KNOWN_CITIES.find((city) => city === text);
  if (exact) return exact;

  const partial = KNOWN_CITIES.find(
    (city) => city.includes(text) || text.includes(city)
  );
  if (partial) return partial;

  return raw.trim();
}

function toTitleCaseFr(input = "") {
  return String(input)
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(" ");
}

async function normalizeCity(raw = "") {
  const byRules = normalizeCityDeterministic(raw);
  if (normalize(byRules) !== normalize(raw) || KNOWN_CITIES.includes(normalize(byRules))) {
    return toTitleCaseFr(byRules);
  }

  if (!openai || !raw.trim()) return raw.trim();

  try {
    const response = await openai.responses.create({
      model: OPENAI_CITY_MODEL,
      store: false,
      max_output_tokens: 20,
      input: [
        {
          role: "system",
          content: [
            {
              type: "input_text",
              text:
                "Corrige un nom de ville française dicté à la voix. Réponds uniquement avec le nom de la ville, sans phrase.",
            },
          ],
        },
        {
          role: "user",
          content: [{ type: "input_text", text: raw }],
        },
      ],
    });

    const aiCity = (response.output_text || raw).trim();
    const finalCity = normalizeCityDeterministic(aiCity);
    return toTitleCaseFr(finalCity || aiCity);
  } catch (error) {
    console.error("normalizeCity error", error.message || error);
    return raw.trim();
  }
}

function parseRefDigitsByRules(raw = "") {
  const text = normalize(raw);
  if (!text) return { ok: false };

  let result = text;

  for (const [word, digit] of Object.entries({
    zero: "0",
    un: "1",
    une: "1",
    deux: "2",
    trois: "3",
    quatre: "4",
    cinq: "5",
    six: "6",
    sept: "7",
    huit: "8",
    neuf: "9",
  })) {
    result = result.replace(new RegExp(`\\b${word}\\b`, "g"), digit);
  }

  const digits = onlyDigits(result);
  if (!digits || digits.length < 3) return { ok: false };

  return { ok: true, digits };
}

async function parseRefDigitsWithAI(raw = "") {
  if (!openai || !raw.trim()) return { ok: false };

  try {
    const response = await openai.responses.create({
      model: OPENAI_REF_MODEL,
      store: false,
      max_output_tokens: 20,
      input: [
        {
          role: "system",
          content: [
            {
              type: "input_text",
              text:
                "Extrais uniquement les chiffres d'une référence immobilière dictée oralement. Réponds uniquement avec les chiffres. Si impossible, réponds NONE.",
            },
          ],
        },
        {
          role: "user",
          content: [{ type: "input_text", text: raw }],
        },
      ],
    });

    const out = (response.output_text || "").trim();
    if (out.toUpperCase() === "NONE") return { ok: false };

    const digits = onlyDigits(out);
    if (!digits || digits.length < 3) return { ok: false };

    return { ok: true, digits };
  } catch (error) {
    console.error("parseRefDigitsWithAI error", error.message || error);
    return { ok: false };
  }
}

async function parseRefDigits(raw = "") {
  const byRules = parseRefDigitsByRules(raw);
  if (byRules.ok) return byRules;
  return await parseRefDigitsWithAI(raw);
}

// ======================================================
// WEATHER HELPERS
// ======================================================
function buildWeatherJoke(city, temp, description = "") {
  const d = normalize(description);

  if (d.includes("pluie") || d.includes("averse")) {
    return `À ${city}, il pleut aujourd hui. Bon, au moins c est parfait pour tester l isolation sans supplément.`;
  }

  if (d.includes("orage")) {
    return `À ${city}, il y a de l orage. Ambiance dramatique idéale, mais on préfère quand même les visites sans effets spéciaux.`;
  }

  if (d.includes("neige")) {
    return `À ${city}, il neige. Très joli, et excellent test grandeur nature pour le chauffage.`;
  }

  if (temp >= 28) {
    return `À ${city}, il fait ${temp} degrés. Disons que la visite permet aussi de vérifier la climatisation.`;
  }

  if (temp <= 3) {
    return `À ${city}, il fait ${temp} degrés. Une bonne occasion de juger le chauffage dès l entrée.`;
  }

  if (d.includes("soleil") || d.includes("degage") || d.includes("ciel clair")) {
    return `À ${city}, il fait ${temp} degrés avec du soleil. Franchement, la météo fait déjà la moitié de la visite.`;
  }

  return `À ${city}, il fait ${temp} degrés avec ${description}. Météo neutre, donc le bien devra séduire tout seul.`;
}

async function getWeatherComment(city = "") {
  try {
    if (!OPENWEATHER_API_KEY || !city) return "";

    const cached = getCachedWeather(city);
    if (cached) return cached;

    const response = await axios.get(
      "https://api.openweathermap.org/data/2.5/weather",
      {
        timeout: 5000,
        params: {
          q: `${city},FR`,
          units: "metric",
          lang: "fr",
          appid: OPENWEATHER_API_KEY,
        },
        validateStatus: () => true,
      }
    );

    if (response.status >= 400 || !response.data?.main) {
      return "";
    }

    const temp = Math.round(Number(response.data.main.temp));
    const description = String(
      response.data.weather?.[0]?.description || ""
    ).trim();

    if (!Number.isFinite(temp) || !description) {
      return "";
    }

    const sentence = buildWeatherJoke(city, temp, description);
    setCachedWeather(city, sentence);
    return sentence;
  } catch (error) {
    console.error("Weather error", error.message || error);
    return "";
  }
}

// ======================================================
// APIMO HELPERS
// ======================================================
async function fetchPropertiesFromApimo() {
  const url = `/agencies/${APIMO_AGENCY_ID}/properties`;

  console.log("APIMO token loaded:", !!APIMO_API_TOKEN);
  console.log("APIMO provider:", APIMO_PROVIDER_ID);
  console.log("APIMO agency:", APIMO_AGENCY_ID);
  console.log("APIMO base url:", APIMO_BASE_URL);
  console.log("APIMO URL:", `${APIMO_BASE_URL}${url}`);

  const response = await apimo.get(url);

  console.log("APIMO STATUS:", response.status);

  if (response.status >= 400) {
    throw new Error(
      `APIMO HTTP ${response.status}: ${JSON.stringify(response.data || {})}`
    );
  }

  const data = response.data;

  if (Array.isArray(data)) return data;
  if (Array.isArray(data?.properties)) return data.properties;
  if (Array.isArray(data?.items)) return data.items;

  return [];
}

async function fetchProperties({ forceRefresh = false } = {}) {
  if (!forceRefresh && isApimoCacheFresh()) {
    return propertiesCache.data;
  }

  const properties = await fetchPropertiesFromApimo();
  propertiesCache.data = properties;
  propertiesCache.fetchedAt = Date.now();

  businessLog("APIMO_PROPERTIES_REFRESHED", {
    count: properties.length,
    fetchedAt: new Date(propertiesCache.fetchedAt).toISOString(),
  });

  return properties;
}

function extractPropertyId(property) {
  return String(
    property?.id ?? property?.property_id ?? property?.propertyId ?? ""
  );
}

function extractPropertyTitle(property) {
  return cleanText(
    property?.title ||
      property?.reference ||
      property?.category ||
      property?.type ||
      "Bien immobilier"
  );
}

function extractPropertyCity(property) {
  const city =
    property?.city?.name ||
    property?.city?.label ||
    property?.city?.value ||
    property?.address?.city?.name ||
    property?.address?.city?.label ||
    property?.address?.city ||
    property?.location?.city?.name ||
    property?.location?.city?.label ||
    property?.location?.city ||
    property?.city;

  return typeof city === "string" ? cleanText(city) : "";
}

function extractPropertySurface(property) {
  const surface =
    property?.surface ??
    property?.area ??
    property?.living_area ??
    property?.land_area;
  const n = Number(surface);
  return Number.isFinite(n) ? `${n} m²` : "";
}

function extractPropertyPrice(property) {
  const n = Number(
    property?.price ?? property?.amount ?? property?.sale_price
  );
  return Number.isFinite(n)
    ? `${n.toLocaleString("fr-FR")} €`
    : "Prix sur demande";
}

function extractPropertyReference(property) {
  return cleanText(
    property?.reference || property?.ref || property?.property_reference || ""
  );
}

function extractPropertyUrl(property) {
  const url =
    property?.url ||
    property?.public_url ||
    property?.share_url ||
    property?.link ||
    "";

  return typeof url === "string" ? cleanText(url) : "";
}

function sanitizeProperty(property) {
  return {
    id: extractPropertyId(property),
    title: extractPropertyTitle(property),
    city: extractPropertyCity(property),
    reference: extractPropertyReference(property),
    price: extractPropertyPrice(property),
    surface: extractPropertySurface(property),
    url: extractPropertyUrl(property),
    raw: property,
  };
}

function formatPropertyMessage(property) {
  const p = sanitizeProperty(property);
  const lines = [
    `🏡 ${p.title}`,
    p.city ? `📍 ${p.city}` : "",
    p.reference ? `🔖 Réf : ${p.reference}` : "",
    "",
    `💰 ${p.price}`,
    p.surface ? `📐 ${p.surface}` : "",
    "",
    p.url ? `👉 ${p.url}` : `👉 ${SWIXIM_AGENCY_URL || SWIXIM_BASE_URL}`,
  ].filter(Boolean);

  return lines.join("\n").trim();
}

function generateWhatsAppPrefilledLink(toPhone, message) {
  const recipient = normalizeWaRecipientPhone(toPhone);
  if (!recipient) return "";

  const encoded = encodeURIComponent(message);
  return `https://wa.me/${recipient}?text=${encoded}`;
}

function buildPropertyPayload(property) {
  const safe = sanitizeProperty(property);
  const message = formatPropertyMessage(property);

  return {
    property: safe,
    message,
  };
}

function findPropertyById(properties, id) {
  return properties.find((property) => extractPropertyId(property) === String(id));
}

function parseDateToTimestamp(value) {
  const ts = Date.parse(value);
  return Number.isFinite(ts) ? ts : 0;
}

function findLatestProperty(properties) {
  if (!Array.isArray(properties) || !properties.length) return null;

  return [...properties].sort((a, b) => {
    const aTs = Math.max(
      parseDateToTimestamp(a?.updated_at),
      parseDateToTimestamp(a?.created_at),
      parseDateToTimestamp(a?.updatedAt),
      parseDateToTimestamp(a?.createdAt)
    );
    const bTs = Math.max(
      parseDateToTimestamp(b?.updated_at),
      parseDateToTimestamp(b?.created_at),
      parseDateToTimestamp(b?.updatedAt),
      parseDateToTimestamp(b?.createdAt)
    );
    return bTs - aTs;
  })[0];
}

function findPropertyFromApimoList({ properties, city, refDigits }) {
  const wantedCity = normalize(city);

  const candidates = properties
    .map((item) => {
      const safe = sanitizeProperty(item);
      let score = 0;

      const refDigitsItem = onlyDigits(safe.reference);
      const idDigits = onlyDigits(safe.id);

      if (!refDigits) return null;

      if (refDigitsItem === refDigits) score += 100;
      else if (refDigitsItem.startsWith(refDigits)) score += 80;
      else if (idDigits === refDigits) score += 120;
      else if (idDigits.startsWith(refDigits)) score += 90;

      const cityNorm = normalize(safe.city);
      if (wantedCity) {
        if (cityNorm === wantedCity) score += 80;
        else if (cityNorm.includes(wantedCity)) score += 45;
        else if (wantedCity.includes(cityNorm)) score += 25;
      }

      return { item, safe, score };
    })
    .filter(Boolean)
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score);

  businessLog("APIMO_MATCH_DEBUG", {
    city,
    refDigits,
    candidateCount: candidates.length,
    top: candidates.slice(0, 5).map((x) => ({
      score: x.score,
      id: x.safe.id,
      city: x.safe.city,
      reference: x.safe.reference,
      title: x.safe.title,
    })),
  });

  if (!candidates.length) return null;
  if (candidates[0].score < 80) return null;
  return candidates[0].item;
}

// ======================================================
// GOOGLE CALENDAR
// ======================================================
function getGoogleAuth() {
  const clientEmail = process.env.GOOGLE_CLIENT_EMAIL;
  const privateKey = process.env.GOOGLE_PRIVATE_KEY;

  if (!clientEmail || !privateKey) {
    throw new Error("GOOGLE_CLIENT_EMAIL ou GOOGLE_PRIVATE_KEY manquant");
  }

  return new google.auth.JWT({
    email: clientEmail,
    key: privateKey.replace(/\\n/g, "\n"),
    scopes: ["https://www.googleapis.com/auth/calendar"],
  });
}

async function getCalendarClient() {
  const auth = getGoogleAuth();
  return google.calendar({ version: "v3", auth });
}

function parseFrenchDate(raw = "") {
  const text = normalize(raw);
  const now = getNowParis();

  function toIso(year, month, day) {
    const dt = DateTime.fromObject({ year, month, day }, { zone: TZ });
    if (!dt.isValid) return { ok: false };
    return {
      ok: true,
      year,
      month,
      day,
      isoDate: dt.toISODate(),
    };
  }

  function addDays(baseDate, days) {
    const d = baseDate.plus({ days });
    return toIso(d.year, d.month, d.day);
  }

  function nextWeekday(targetDay) {
    let d = now.startOf("day");
    while (d.weekday % 7 !== targetDay) {
      d = d.plus({ days: 1 });
    }
    if (d <= now.startOf("day")) d = d.plus({ days: 7 });
    return toIso(d.year, d.month, d.day);
  }

  if (text.includes("aujourd hui")) return toIso(now.year, now.month, now.day);
  if (text.includes("apres demain")) return addDays(now, 2);
  if (text.includes("demain")) return addDays(now, 1);

  const weekdays = {
    dimanche: 0,
    lundi: 1,
    mardi: 2,
    mercredi: 3,
    jeudi: 4,
    vendredi: 5,
    samedi: 6,
  };

  for (const [name, dayNumber] of Object.entries(weekdays)) {
    if (text.includes(name)) {
      return nextWeekday(dayNumber);
    }
  }

  let match = text.match(/\b(\d{1,2})[\/\-\s](\d{1,2})[\/\-\s](\d{4})\b/);
  if (match) {
    return toIso(Number(match[3]), Number(match[2]), Number(match[1]));
  }

  match = text.match(
    /\b(\d{1,2})\s+(janvier|fevrier|mars|avril|mai|juin|juillet|aout|septembre|octobre|novembre|decembre)\s+(\d{4})\b/
  );
  if (match) {
    const month = monthNameToNumber(match[2]);
    return toIso(Number(match[3]), month, Number(match[1]));
  }

  match = text.match(
    /\b(\d{1,2})\s+(janvier|fevrier|mars|avril|mai|juin|juillet|aout|septembre|octobre|novembre|decembre)\b/
  );
  if (match) {
    const day = Number(match[1]);
    const month = monthNameToNumber(match[2]);
    let year = now.year;

    const candidate = DateTime.fromObject({ year, month, day }, { zone: TZ });
    if (!candidate.isValid) return { ok: false };
    if (candidate.startOf("day") < now.startOf("day")) year += 1;
    return toIso(year, month, day);
  }

  match = text.match(
    /\b([a-z\s\-]+)\s+(janvier|fevrier|mars|avril|mai|juin|juillet|aout|septembre|octobre|novembre|decembre)\s+([a-z\s\-\d]+)\b/
  );
  if (match) {
    const dayParsed = parseFrenchNumberWords(match[1]);
    const month = monthNameToNumber(match[2]);
    const yearParsed = parseFrenchNumberWords(match[3]);
    if (dayParsed.ok && yearParsed.ok && month) {
      return toIso(yearParsed.value, month, dayParsed.value);
    }
  }

  match = text.match(
    /\b([a-z\s\-]+)\s+(janvier|fevrier|mars|avril|mai|juin|juillet|aout|septembre|octobre|novembre|decembre)\b/
  );
  if (match) {
    const dayParsed = parseFrenchNumberWords(match[1]);
    const month = monthNameToNumber(match[2]);
    if (dayParsed.ok && month) {
      let year = now.year;
      const candidate = DateTime.fromObject(
        { year, month, day: dayParsed.value },
        { zone: TZ }
      );
      if (!candidate.isValid) return { ok: false };
      if (candidate.startOf("day") < now.startOf("day")) year += 1;
      return toIso(year, month, dayParsed.value);
    }
  }

  return { ok: false };
}

function parseFrenchTime(raw = "") {
  const text = normalize(raw);

  let match = text.match(/\b(\d{1,2})[:h](\d{2})\b/);
  if (match) {
    const hour = Number(match[1]);
    const minute = Number(match[2]);
    if (hour >= 0 && hour <= 23 && minute >= 0 && minute <= 59) {
      return {
        ok: true,
        hour,
        minute,
        hhmm: `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`,
      };
    }
  }

  match = text.match(/\b(\d{1,2})(?:\s*(?:heures|heure|h))\s*(\d{1,2})?\b/);
  if (match) {
    const hour = Number(match[1]);
    const minute = match[2] ? Number(match[2]) : 0;
    if (hour >= 0 && hour <= 23 && minute >= 0 && minute <= 59) {
      return {
        ok: true,
        hour,
        minute,
        hhmm: `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`,
      };
    }
  }

  match = text.match(/^\d{1,2}$/);
  if (match) {
    const hour = Number(text);
    if (hour >= 0 && hour <= 23) {
      return {
        ok: true,
        hour,
        minute: 0,
        hhmm: `${String(hour).padStart(2, "0")}:00`,
      };
    }
  }

  const wordHourMinuteMatch = text.match(
    /^([a-z\s\-]+?)(?:\s+heure(?:s)?\s*|\s+h\s*|\s+)([a-z\s\-]+)?$/
  );

  if (wordHourMinuteMatch) {
    const hourParsed = parseFrenchNumberWords(wordHourMinuteMatch[1] || "");
    const minuteParsed = wordHourMinuteMatch[2]
      ? parseFrenchNumberWords(wordHourMinuteMatch[2])
      : { ok: true, value: 0 };

    if (
      hourParsed.ok &&
      minuteParsed.ok &&
      hourParsed.value >= 0 &&
      hourParsed.value <= 23 &&
      minuteParsed.value >= 0 &&
      minuteParsed.value <= 59
    ) {
      return {
        ok: true,
        hour: hourParsed.value,
        minute: minuteParsed.value,
        hhmm: `${String(hourParsed.value).padStart(2, "0")}:${String(minuteParsed.value).padStart(2, "0")}`,
      };
    }
  }

  return { ok: false };
}

function isFutureSlot(isoDate, hhmm) {
  const dt = DateTime.fromISO(`${isoDate}T${hhmm}`, { zone: TZ });
  if (!dt.isValid) return false;
  return dt.toMillis() > getNowParis().toMillis();
}

async function isCalendarSlotAvailable({ isoDate, hhmm, durationMinutes }) {
  const calendar = await getCalendarClient();
  const start = DateTime.fromISO(`${isoDate}T${hhmm}`, { zone: TZ });
  const end = start.plus({ minutes: durationMinutes });

  const response = await calendar.events.list({
    calendarId: GOOGLE_CALENDAR_ID,
    timeMin: start.toUTC().toISO(),
    timeMax: end.toUTC().toISO(),
    singleEvents: true,
    orderBy: "startTime",
  });

  return (response.data.items || []).length === 0;
}

async function findAlternativeSlotsForDay({
  isoDate,
  durationMinutes,
  excludeHhmm,
  businessHoursStart = 9,
  businessHoursEnd = 18,
  slotStepMinutes = 30,
  maxSuggestions = 3,
}) {
  const results = [];

  for (
    let minutes = businessHoursStart * 60;
    minutes <= businessHoursEnd * 60 - durationMinutes;
    minutes += slotStepMinutes
  ) {
    const hour = Math.floor(minutes / 60);
    const minute = minutes % 60;
    const hhmm = `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;

    if (hhmm === excludeHhmm) continue;
    if (!isFutureSlot(isoDate, hhmm)) continue;

    const available = await isCalendarSlotAvailable({
      isoDate,
      hhmm,
      durationMinutes,
    });

    if (available) {
      results.push(hhmm);
      if (results.length >= maxSuggestions) break;
    }
  }

  return results;
}

async function createCalendarEvent({ name, phone, isoDate, hhmm }) {
  const calendar = await getCalendarClient();
  const start = DateTime.fromISO(`${isoDate}T${hhmm}`, { zone: TZ });
  const end = start.plus({ minutes: APPOINTMENT_DURATION_MINUTES });

  const result = await calendar.events.insert({
    calendarId: GOOGLE_CALENDAR_ID,
    requestBody: {
      summary: `RDV téléphonique - ${name}`,
      description: `Demande reçue via agent vocal Swixim\nNom: ${name}\nTéléphone: ${phone}`,
      start: {
        dateTime: start.toISO(),
        timeZone: TZ,
      },
      end: {
        dateTime: end.toISO(),
        timeZone: TZ,
      },
    },
  });

  return result.data;
}

// ======================================================
// ADMIN HTML
// ======================================================
function renderAdminPage(items) {
  const rows = items
    .map((item) => {
      const typeLabel =
        item.type === "voice_message"
          ? "Message vocal"
          : item.type === "spoken_message"
            ? "Message dicté"
            : item.type === "brochure_request"
              ? "Demande de fiche"
              : item.type === "appointment_request"
                ? "Rendez-vous"
                : item.type || "-";

      const statusLabel = item.isRead ? "Lu" : "Non lu";

      const messageCell = item.message ? escapeHtml(item.message) : "-";

      const audioCell = item.recordingUrl
        ? `
          <audio controls preload="none" style="max-width:220px;">
            <source src="/admin/audio/${encodeURIComponent(item.id)}" type="audio/mpeg" />
            Votre navigateur ne supporte pas l'audio.
          </audio>
        `
        : "-";

      const appointmentCell =
        item.appointment?.date || item.appointment?.time
          ? `${escapeHtml(item.appointment?.date || "")} ${escapeHtml(item.appointment?.time || "")}`.trim()
          : "-";

      const calendarCell = item.appointment?.calendarHtmlLink
        ? `<a href="${escapeHtml(item.appointment.calendarHtmlLink)}" target="_blank" rel="noreferrer">Voir</a>`
        : "-";

      const brochureRef = item.brochure?.ref ? escapeHtml(item.brochure.ref) : "-";
      const brochureCity = item.brochure?.city ? escapeHtml(item.brochure.city) : "-";
      const brochureLink = item.brochure?.link
        ? `<a href="${escapeHtml(item.brochure.link)}" target="_blank" rel="noreferrer">Ouvrir</a>`
        : "-";

      const waLink = item.brochure?.whatsappPrefilledLink
        ? `<a href="${escapeHtml(item.brochure.whatsappPrefilledLink)}" target="_blank" rel="noreferrer">Ouvrir</a>`
        : "-";

      return `
        <tr>
          <td>${escapeHtml(formatLocalDateTime(item.createdAt))}</td>
          <td>${escapeHtml(typeLabel)}</td>
          <td>${escapeHtml(item.callerName || "-")}</td>
          <td>${escapeHtml(item.callerPhone || "-")}</td>
          <td>${brochureCity}</td>
          <td>${brochureRef}</td>
          <td>${brochureLink}</td>
          <td>${waLink}</td>
          <td>${messageCell}</td>
          <td>${audioCell}</td>
          <td>${escapeHtml(appointmentCell)}</td>
          <td>${calendarCell}</td>
          <td>${escapeHtml(statusLabel)}</td>
          <td>
            <form method="POST" action="/admin/mark-read" style="display:inline-block; margin:0 4px 4px 0;">
              <input type="hidden" name="id" value="${escapeHtml(item.id)}" />
              <button type="submit">${item.isRead ? "Marquer non lu" : "Marquer lu"}</button>
            </form>
            <form method="POST" action="/admin/delete" style="display:inline-block; margin:0 4px 4px 0;" onsubmit="return confirm('Supprimer cette entrée ?');">
              <input type="hidden" name="id" value="${escapeHtml(item.id)}" />
              <button type="submit">Supprimer</button>
            </form>
          </td>
        </tr>
      `;
    })
    .join("");

  return `
    <!DOCTYPE html>
    <html lang="fr">
      <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Admin Swixim</title>
        <style>
          * { box-sizing: border-box; }
          body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 24px;
            background: #f4f6f8;
            color: #1f2937;
          }
          h1 { margin-top: 0; }
          .topbar {
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap:16px;
            margin-bottom:20px;
            flex-wrap:wrap;
          }
          .card {
            background:white;
            border-radius:12px;
            padding:16px;
            box-shadow:0 2px 10px rgba(0,0,0,0.06);
            margin-bottom:20px;
          }
          .meta { color:#6b7280; font-size:14px; }
          .table-wrap { overflow-x:auto; }
          table { width:100%; border-collapse:collapse; background:white; }
          th, td {
            padding:12px;
            border-bottom:1px solid #e5e7eb;
            text-align:left;
            vertical-align:top;
            font-size:14px;
          }
          th { background:#f9fafb; position:sticky; top:0; }
          button {
            border:none;
            background:#111827;
            color:white;
            padding:8px 12px;
            border-radius:8px;
            cursor:pointer;
          }
          button:hover { opacity:0.92; }
          a { color:#2563eb; text-decoration:none; }
          a:hover { text-decoration:underline; }
          audio { width:220px; }
        </style>
      </head>
      <body>
        <div class="topbar">
          <div>
            <h1>Administration Swixim</h1>
            <div class="meta">${items.length} entrée(s)</div>
          </div>
          <div class="meta"><a href="/admin/json">Version JSON</a></div>
        </div>

        <div class="card">
          <strong>Types gérés :</strong> messages vocaux, messages dictés, demandes de fiche, rendez-vous.
        </div>

        <div class="card table-wrap">
          <table>
            <thead>
              <tr>
                <th>Date</th>
                <th>Type</th>
                <th>Nom</th>
                <th>Téléphone</th>
                <th>Ville</th>
                <th>Réf bien</th>
                <th>Fiche</th>
                <th>WA lien</th>
                <th>Message</th>
                <th>Audio</th>
                <th>RDV</th>
                <th>Agenda</th>
                <th>Statut</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              ${rows || `<tr><td colspan="14">Aucune donnée.</td></tr>`}
            </tbody>
          </table>
        </div>
      </body>
    </html>
  `;
}

// ======================================================
// TWILIO AUDIO FETCH
// ======================================================
async function fetchTwilioRecordingMp3(recordingUrl) {
  const accountSid = process.env.TWILIO_ACCOUNT_SID;
  const authToken = process.env.TWILIO_AUTH_TOKEN;

  if (!accountSid || !authToken) {
    throw new Error("TWILIO_ACCOUNT_SID ou TWILIO_AUTH_TOKEN manquant");
  }

  const url = `${recordingUrl}.mp3`;
  const response = await axios.get(url, {
    responseType: "arraybuffer",
    headers: {
      Authorization:
        "Basic " + Buffer.from(`${accountSid}:${authToken}`).toString("base64"),
    },
    validateStatus: () => true,
  });

  if (response.status >= 400) {
    throw new Error(`Erreur Twilio audio: ${response.status}`);
  }

  return Buffer.from(response.data);
}

// ======================================================
// ROOT / HEALTH
// ======================================================
app.get("/", (_req, res) => {
  res.json({
    ok: true,
    service: "Swixim unified server",
    features: [
      "twilio_voice",
      "google_calendar",
      "apimo",
      "brochure_request",
      "whatsapp_prefilled_link",
      "voice_message",
      "spoken_message",
      "admin",
      "weather_comment",
    ],
  });
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    env: NODE_ENV,
    timezone: TZ,
    sessions: sessions.size,
    apimo: {
      providerId: APIMO_PROVIDER_ID,
      agencyId: APIMO_AGENCY_ID,
      cacheCount: propertiesCache.data.length,
      cacheFresh: isApimoCacheFresh(),
      cacheFetchedAt: propertiesCache.fetchedAt
        ? new Date(propertiesCache.fetchedAt).toISOString()
        : null,
    },
    weather: {
      enabled: Boolean(OPENWEATHER_API_KEY),
      cacheSize: weatherCache.size,
    },
  });
});

// ======================================================
// APIMO API
// ======================================================
app.get("/properties/raw", async (_req, res, next) => {
  try {
    const properties = await fetchProperties();
    res.json(properties);
  } catch (error) {
    next(error);
  }
});

app.get("/properties", async (_req, res, next) => {
  try {
    const properties = await fetchProperties();
    res.json({
      count: properties.length,
      items: properties.map(sanitizeProperty),
    });
  } catch (error) {
    next(error);
  }
});

app.post("/properties/refresh", async (_req, res, next) => {
  try {
    const properties = await fetchProperties({ forceRefresh: true });
    res.json({
      ok: true,
      count: properties.length,
      refreshedAt: new Date(propertiesCache.fetchedAt).toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

app.get("/property/:id", async (req, res, next) => {
  try {
    if (!isSafeId(req.params.id)) {
      return res.status(400).json({ error: "Identifiant invalide" });
    }

    const properties = await fetchProperties();
    const property = findPropertyById(properties, req.params.id);

    if (!property) {
      return res.status(404).json({ error: "Bien introuvable" });
    }

    res.json(buildPropertyPayload(property));
  } catch (error) {
    next(error);
  }
});

app.get("/property/:id/whatsapp-link", async (req, res, next) => {
  try {
    if (!isSafeId(req.params.id)) {
      return res.status(400).json({ error: "Identifiant invalide" });
    }

    const phone = normalizeFrenchPhone(req.query.phone || "");
    if (!phone) {
      return res.status(400).json({ error: "Numéro invalide" });
    }

    const properties = await fetchProperties();
    const property = findPropertyById(properties, req.params.id);

    if (!property) {
      return res.status(404).json({ error: "Bien introuvable" });
    }

    const payload = buildPropertyPayload(property);
    const whatsappPrefilledLink = generateWhatsAppPrefilledLink(
      phone,
      payload.message
    );

    res.json({
      property: payload.property,
      whatsapp_prefilled_link: whatsappPrefilledLink,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/property/:id/open-whatsapp", async (req, res, next) => {
  try {
    if (!isSafeId(req.params.id)) {
      return res.status(400).send("Identifiant invalide");
    }

    const phone = normalizeFrenchPhone(req.query.phone || "");
    if (!phone) {
      return res.status(400).send("Numéro invalide");
    }

    const properties = await fetchProperties();
    const property = findPropertyById(properties, req.params.id);

    if (!property) {
      return res.status(404).send("Bien introuvable");
    }

    const payload = buildPropertyPayload(property);
    const whatsappPrefilledLink = generateWhatsAppPrefilledLink(
      phone,
      payload.message
    );

    return res.redirect(whatsappPrefilledLink);
  } catch (error) {
    next(error);
  }
});

app.get("/properties/latest", async (_req, res, next) => {
  try {
    const properties = await fetchProperties();
    const latest = findLatestProperty(properties);

    if (!latest) {
      return res.status(404).json({ error: "Aucun bien trouvé" });
    }

    res.json(buildPropertyPayload(latest));
  } catch (error) {
    next(error);
  }
});

// ======================================================
// ADMIN
// ======================================================
app.get("/admin/json", (req, res) => {
  const auth = checkBasicAuth(req);
  if (!auth.ok) {
    res.set("WWW-Authenticate", 'Basic realm="Admin"');
    return res.status(auth.status).send(auth.message);
  }

  res.json({
    count: readContacts().length,
    items: readContacts(),
  });
});

app.get("/admin", (req, res) => {
  const auth = checkBasicAuth(req);
  if (!auth.ok) {
    res.set("WWW-Authenticate", 'Basic realm="Admin"');
    return res.status(auth.status).send(auth.message);
  }

  res.type("html").send(renderAdminPage(readContacts()));
});

app.get("/admin/audio/:id", async (req, res) => {
  const auth = checkBasicAuth(req);
  if (!auth.ok) {
    res.set("WWW-Authenticate", 'Basic realm="Admin"');
    return res.status(auth.status).send(auth.message);
  }

  const id = String(req.params?.id || "");
  const item = readContacts().find((entry) => entry.id === id);

  if (!item) return res.status(404).send("Enregistrement introuvable");
  if (!item.recordingUrl) return res.status(404).send("Aucun audio disponible");

  try {
    const audioBuffer = await fetchTwilioRecordingMp3(item.recordingUrl);
    res.set("Content-Type", "audio/mpeg");
    res.set("Content-Length", String(audioBuffer.length));
    res.set("Cache-Control", "no-store");
    res.send(audioBuffer);
  } catch (error) {
    console.error("Erreur lecture audio Twilio", error);
    res.status(500).send("Impossible de lire l'audio");
  }
});

app.post("/admin/mark-read", (req, res) => {
  const auth = checkBasicAuth(req);
  if (!auth.ok) {
    res.set("WWW-Authenticate", 'Basic realm="Admin"');
    return res.status(auth.status).send(auth.message);
  }

  const id = String(req.body?.id || "");
  if (id) {
    updateContactEntry(id, (item) => ({
      ...item,
      isRead: !item.isRead,
      updatedAt: new Date().toISOString(),
    }));
  }

  res.redirect("/admin");
});

app.post("/admin/delete", (req, res) => {
  const auth = checkBasicAuth(req);
  if (!auth.ok) {
    res.set("WWW-Authenticate", 'Basic realm="Admin"');
    return res.status(auth.status).send(auth.message);
  }

  const id = String(req.body?.id || "");
  if (id) deleteContactEntry(id);

  res.redirect("/admin");
});

// ======================================================
// TWILIO VOICE ENTRY
// ======================================================
app.post(
  "/",
  twilioOnly((_req, res) => {
    const response = vr();
    response.redirect({ method: "POST" }, "/voice");
    res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/voice",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    getSession(callSid);

    businessLog("CALL_STARTED", {
      callSid,
      startedAt: new Date().toISOString(),
    });

    const response = vr();
    sayFr(
      response,
      "Bonjour et bienvenue chez Swixim. Je suis Léa, votre agent digitale."
    );
    response.pause({ length: 1 });
    response.redirect({ method: "POST" }, "/menu");

    res.type("text/xml").send(response.toString());
  })
);

// ======================================================
// MENU / INTENT
// ======================================================
app.post(
  "/menu",
  twilioOnly((_req, res) => {
    const response = vr();

    gatherSpeech(
      response,
      "/intent",
      "Je peux vous aider pour trois besoins. Prendre un rendez vous téléphonique, laisser un message au conseiller, ou recevoir une fiche descriptive du bien. Dites par exemple rendez vous, message, ou fiche descriptive.",
      "rendez vous telephonique, rendez vous, rdv, message au conseiller, fiche descriptive, descriptif, description"
    );

    response.redirect({ method: "POST" }, "/menu-retry");
    res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/menu-retry",
  twilioOnly((_req, res) => {
    const response = vr();

    gatherSpeech(
      response,
      "/intent",
      "Je n ai pas bien entendu. Dites rendez vous téléphonique, message au conseiller, ou fiche descriptive.",
      "rendez vous telephonique, rdv, message au conseiller, fiche descriptive"
    );

    response.redirect({ method: "POST" }, "/goodbye");
    res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/intent",
  twilioOnly(async (req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);

    const speechResult = String(req.body?.SpeechResult || "").trim();
    const text = normalize(speechResult);
    const response = vr();

    businessLog("INTENT_INPUT", {
      callSid,
      speechResult,
      normalizedText: text,
      confidence: req.body?.Confidence,
    });

    const intent = await detectIntent(text);

    if (intent === "fiche") {
      session.flow = "fiche";
      session.unclearCount = 0;

      gatherSpeech(
        response,
        "/fiche-city",
        "Très bien. Pour quelle ville souhaitez vous la fiche ?",
        "strasbourg, mulhouse, guemar, hoerdt, wolfisheim, cernay, erstein, bischheim"
      );
      response.redirect({ method: "POST" }, "/fiche-city-retry");
      return res.type("text/xml").send(response.toString());
    }

    if (intent === "message") {
      session.flow = "message";
      session.unclearCount = 0;

      gatherSpeech(
        response,
        "/msg-type",
        "D accord. Souhaitez vous laisser un message vocal, ou dicter un message ? Dites message vocal, ou message dicté.",
        "message vocal, vocal, message dicté, dicter"
      );
      response.redirect({ method: "POST" }, "/msg-type-retry");
      return res.type("text/xml").send(response.toString());
    }

    if (intent === "rdv") {
      session.flow = "rdv";
      session.unclearCount = 0;

      gatherSpeech(
        response,
        "/rdv-name",
        "Très bien. Quel est votre nom et prénom ?",
        "nom, prénom"
      );
      response.redirect({ method: "POST" }, "/rdv-name-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.unclearCount += 1;

    if (session.unclearCount >= 2) {
      hangupWithMessage(
        response,
        "Je n ai pas bien compris votre besoin. Merci de rappeler. Au revoir."
      );
      resetSession(callSid);
      return res.type("text/xml").send(response.toString());
    }

    gatherSpeech(
      response,
      "/intent",
      "Je n ai pas compris. Dites rendez vous téléphonique, message au conseiller, ou fiche descriptive.",
      "rendez vous telephonique, rdv, message au conseiller, fiche descriptive"
    );
    response.redirect({ method: "POST" }, "/goodbye");
    return res.type("text/xml").send(response.toString());
  })
);

// ======================================================
// FICHE DESCRIPTIVE
// ======================================================
app.post(
  "/fiche-city",
  twilioOnly(async (req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!raw) {
      gatherSpeech(
        response,
        "/fiche-city",
        "Je n ai pas bien entendu la ville. Répétez la ville souhaitée."
      );
      response.redirect({ method: "POST" }, "/fiche-city-retry");
      return res.type("text/xml").send(response.toString());
    }

    const city = await normalizeCity(raw);
    session.data.city = city;

    businessLog("BROCHURE_CITY_PARSED", { callSid, raw, city });

    gatherSpeech(
      response,
      "/fiche-ref",
      `Très bien. Pour ${city}, dites maintenant la référence du bien. Vous pouvez dire les 4 premiers chiffres, ou toute la référence.`,
      "zero, un, deux, trois, quatre, cinq, six, sept, huit, neuf"
    );
    response.redirect({ method: "POST" }, "/fiche-ref-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/fiche-city-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(
      response,
      "Je n ai pas reçu de ville valide. Merci de votre appel. Au revoir."
    );
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/fiche-ref",
  twilioOnly(async (req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    const parsed = await parseRefDigits(raw);

    if (!parsed.ok) {
      gatherSpeech(
        response,
        "/fiche-ref",
        "Je n ai pas bien compris la référence. Répétez les chiffres de la référence."
      );
      response.redirect({ method: "POST" }, "/fiche-ref-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.refDigits = parsed.digits;

    businessLog("BROCHURE_REF_PARSED", {
      callSid,
      raw,
      refDigits: parsed.digits,
    });

    gatherSpeech(
      response,
      "/fiche-phone",
      `Très bien. J ai noté la ville ${session.data.city} et la référence ${spellDigitsForSpeech(parsed.digits)}. Quel numéro de téléphone souhaitez vous utiliser pour recevoir la fiche sur WhatsApp ?`
    );
    response.redirect({ method: "POST" }, "/fiche-phone-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/fiche-ref-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(
      response,
      "Je n ai pas reçu de référence valide. Merci de votre appel. Au revoir."
    );
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/fiche-phone",
  twilioOnly(async (req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!looksLikePhone(raw)) {
      gatherSpeech(
        response,
        "/fiche-phone",
        "Je n ai pas bien compris le numéro. Répétez uniquement votre numéro de téléphone."
      );
      response.redirect({ method: "POST" }, "/fiche-phone-retry");
      return res.type("text/xml").send(response.toString());
    }

    const phone = normalizeFrenchPhone(raw);
    session.data.phone = phone;

    const entry = {
      id: crypto.randomUUID(),
      type: "brochure_request",
      callSid,
      callerName: "",
      callerPhone: phone,
      message: "",
      recordingSid: "",
      recordingUrl: "",
      recordingStatus: "",
      recordingDuration: "",
      brochure: {
        city: session.data.city,
        ref: session.data.refDigits,
        link: "",
        whatsappPrefilledLink: "",
        title: "",
      },
      appointment: {
        date: "",
        time: "",
        calendarEventId: "",
        calendarHtmlLink: "",
      },
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      isRead: false,
      assignedTo: "conseiller",
      source: "twilio_voice",
    };

    saveContactEntry(entry);

    try {
      sayFr(response, "Parfait, je retrouve la fiche. Un instant.");

      const weatherComment = await getWeatherComment(session.data.city);
      if (weatherComment) {
        response.pause({ length: 1 });
        sayFr(response, weatherComment);
      }

      const properties = await fetchProperties();
      const property = findPropertyFromApimoList({
        properties,
        city: session.data.city,
        refDigits: session.data.refDigits,
      });

      if (!property) {
        throw new Error(
          `Aucun bien trouvé pour ${session.data.city} / ${session.data.refDigits}`
        );
      }

      const payload = buildPropertyPayload(property);
      const whatsappLink = generateWhatsAppPrefilledLink(phone, payload.message);

      updateContactEntry(entry.id, (item) => ({
        ...item,
        brochure: {
          ...item.brochure,
          link: payload.property.url || SWIXIM_AGENCY_URL,
          whatsappPrefilledLink: whatsappLink,
          title: payload.property.title,
        },
        updatedAt: new Date().toISOString(),
      }));

      businessLog("BROCHURE_READY", {
        callSid,
        city: session.data.city,
        refDigits: session.data.refDigits,
        phone,
        property: payload.property,
        whatsappLink,
      });

      sayFr(
        response,
        "C est bon. Votre demande de fiche a été enregistrée. Le lien WhatsApp est prêt pour ce numéro. Merci de votre appel. Au revoir."
      );
      response.hangup();

      resetSession(callSid);
      return res.type("text/xml").send(response.toString());
    } catch (error) {
      console.error("Erreur brochure", error);

      updateContactEntry(entry.id, (item) => ({
        ...item,
        updatedAt: new Date().toISOString(),
        message: `Erreur fiche: ${error?.message || "Erreur inconnue"}`,
      }));

      sayFr(
        response,
        "Votre demande de fiche descriptive a bien été enregistrée, mais je n ai pas pu finaliser la recherche automatiquement. Un conseiller vous recontactera rapidement. Merci de votre appel. Au revoir."
      );
      response.hangup();

      resetSession(callSid);
      return res.type("text/xml").send(response.toString());
    }
  })
);

app.post(
  "/fiche-phone-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(
      response,
      "Je n ai pas reçu de numéro. Merci de votre appel. Au revoir."
    );
    return res.type("text/xml").send(response.toString());
  })
);

// ======================================================
// RDV TELEPHONIQUE
// ======================================================
app.post(
  "/rdv-name",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!raw) {
      gatherSpeech(
        response,
        "/rdv-name",
        "Je n ai pas bien entendu. Répétez votre nom et prénom."
      );
      response.redirect({ method: "POST" }, "/rdv-name-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.name = raw;

    gatherSpeech(response, "/rdv-phone", "Merci. Quel est votre numéro de téléphone ?");
    response.redirect({ method: "POST" }, "/rdv-phone-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/rdv-name-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre nom. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/rdv-phone",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!looksLikePhone(raw)) {
      gatherSpeech(
        response,
        "/rdv-phone",
        "Je n ai pas bien compris le numéro. Répétez uniquement votre numéro de téléphone."
      );
      response.redirect({ method: "POST" }, "/rdv-phone-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.phone = normalizeFrenchPhone(raw);

    gatherSpeech(
      response,
      "/rdv-date",
      "Très bien. Quelle date souhaitez vous ? Dites par exemple vingt huit mars deux mille vingt six, ou demain."
    );
    response.redirect({ method: "POST" }, "/rdv-date-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/rdv-phone-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre numéro. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/rdv-date",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    const parsedDate = parseFrenchDate(raw);

    if (!parsedDate.ok) {
      gatherSpeech(
        response,
        "/rdv-date",
        "Je n ai pas bien compris la date. Répétez par exemple vingt huit mars deux mille vingt six, ou demain."
      );
      response.redirect({ method: "POST" }, "/rdv-date-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.dateText = parsedDate.isoDate;

    gatherSpeech(
      response,
      "/rdv-time",
      "Merci. Quelle heure souhaitez vous ? Dites par exemple quatorze heures, quatorze heures trente, ou quatorze trente."
    );
    response.redirect({ method: "POST" }, "/rdv-time-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/rdv-date-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu de date valide. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/rdv-time",
  twilioOnly(async (req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    const parsedTime = parseFrenchTime(raw);

    if (!parsedTime.ok) {
      gatherSpeech(
        response,
        "/rdv-time",
        "Je n ai pas bien compris l heure. Répétez par exemple quatorze heures, quatorze heures trente, ou quatorze trente."
      );
      response.redirect({ method: "POST" }, "/rdv-time-retry");
      return res.type("text/xml").send(response.toString());
    }

    if (!isFutureSlot(session.data.dateText, parsedTime.hhmm)) {
      gatherSpeech(
        response,
        "/rdv-date",
        "La date ou l heure demandée est déjà passée. Merci de redonner une nouvelle date de rendez vous."
      );
      return res.type("text/xml").send(response.toString());
    }

    session.data.timeText = parsedTime.hhmm;

    try {
      const available = await isCalendarSlotAvailable({
        isoDate: session.data.dateText,
        hhmm: session.data.timeText,
        durationMinutes: APPOINTMENT_DURATION_MINUTES,
      });

      if (!available) {
        const alternatives = await findAlternativeSlotsForDay({
          isoDate: session.data.dateText,
          durationMinutes: APPOINTMENT_DURATION_MINUTES,
          excludeHhmm: parsedTime.hhmm,
          maxSuggestions: 3,
        });

        if (alternatives.length > 0) {
          gatherSpeech(
            response,
            "/rdv-time",
            `Ce créneau est déjà pris. Je peux vous proposer ${alternatives.join(", ")}. Merci de dire l heure choisie pour cette même date.`
          );
          response.redirect({ method: "POST" }, "/rdv-time-retry");
          return res.type("text/xml").send(response.toString());
        }

        gatherSpeech(
          response,
          "/rdv-time",
          "Ce créneau est déjà pris. Je n ai pas trouvé d autre disponibilité proche pour cette date. Merci de proposer une autre heure."
        );
        response.redirect({ method: "POST" }, "/rdv-time-retry");
        return res.type("text/xml").send(response.toString());
      }

      const event = await createCalendarEvent({
        name: session.data.name,
        phone: session.data.phone,
        isoDate: session.data.dateText,
        hhmm: session.data.timeText,
      });

      session.data.calendarEventId = event.id || "";
      session.data.calendarHtmlLink = event.htmlLink || "";

      saveContactEntry({
        id: crypto.randomUUID(),
        type: "appointment_request",
        callSid,
        callerName: session.data.name,
        callerPhone: session.data.phone,
        message: "",
        recordingSid: "",
        recordingUrl: "",
        recordingStatus: "",
        recordingDuration: "",
        brochure: {
          city: "",
          ref: "",
          link: "",
          whatsappPrefilledLink: "",
          title: "",
        },
        appointment: {
          date: session.data.dateText,
          time: session.data.timeText,
          calendarEventId: session.data.calendarEventId,
          calendarHtmlLink: session.data.calendarHtmlLink,
        },
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        isRead: false,
        assignedTo: "conseiller",
        source: "twilio_voice",
      });

      hangupWithMessage(
        response,
        "Parfait. Votre rendez vous téléphonique a bien été enregistré dans l agenda. Merci de votre appel. Au revoir."
      );
      resetSession(callSid);
      return res.type("text/xml").send(response.toString());
    } catch (error) {
      console.error("Erreur création Google Calendar", error);

      hangupWithMessage(
        response,
        "Votre demande a bien été prise en compte, mais je rencontre un problème pour l enregistrer dans l agenda. Un conseiller vous recontactera rapidement. Merci de votre appel. Au revoir."
      );
      resetSession(callSid);
      return res.type("text/xml").send(response.toString());
    }
  })
);

app.post(
  "/rdv-time-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu d heure valide. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

// ======================================================
// MESSAGE CHOICE
// ======================================================
app.post(
  "/msg-type",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);

    const raw = String(req.body?.SpeechResult || "").trim();
    const text = normalize(raw);
    const response = vr();

    if (text.includes("vocal")) {
      session.data.messageMode = "voice";

      gatherSpeech(response, "/msg-voice-name", "Très bien. Quel est votre nom et prénom ?");
      response.redirect({ method: "POST" }, "/msg-voice-name-retry");
      return res.type("text/xml").send(response.toString());
    }

    if (
      text.includes("dicte") ||
      text.includes("dicter") ||
      text.includes("dictee") ||
      text.includes("message")
    ) {
      session.data.messageMode = "spoken";

      gatherSpeech(response, "/msg-name", "D accord. Quel est votre nom et prénom ?");
      response.redirect({ method: "POST" }, "/msg-name-retry");
      return res.type("text/xml").send(response.toString());
    }

    gatherSpeech(
      response,
      "/msg-type",
      "Je n ai pas bien compris. Dites message vocal, ou message dicté.",
      "message vocal, vocal, message dicté, dicter"
    );
    response.redirect({ method: "POST" }, "/msg-type-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-type-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre choix. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

// ======================================================
// MESSAGE VOCAL
// ======================================================
app.post(
  "/msg-voice-name",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!raw) {
      gatherSpeech(
        response,
        "/msg-voice-name",
        "Je n ai pas bien entendu. Répétez votre nom et prénom."
      );
      response.redirect({ method: "POST" }, "/msg-voice-name-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.name = raw;

    gatherSpeech(response, "/msg-voice-phone", "Merci. Quel est votre numéro de téléphone ?");
    response.redirect({ method: "POST" }, "/msg-voice-phone-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-voice-name-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre nom. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-voice-phone",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!looksLikePhone(raw)) {
      gatherSpeech(
        response,
        "/msg-voice-phone",
        "Je n ai pas bien compris le numéro. Répétez uniquement votre numéro de téléphone."
      );
      response.redirect({ method: "POST" }, "/msg-voice-phone-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.phone = normalizeFrenchPhone(raw);

    const pendingId = crypto.randomUUID();
    session.data.pendingVoiceMessageId = pendingId;

    saveContactEntry({
      id: pendingId,
      type: "voice_message",
      callSid,
      callerName: session.data.name,
      callerPhone: session.data.phone,
      message: "",
      recordingSid: "",
      recordingUrl: "",
      recordingStatus: "pending",
      recordingDuration: "",
      brochure: {
        city: "",
        ref: "",
        link: "",
        whatsappPrefilledLink: "",
        title: "",
      },
      appointment: {
        date: "",
        time: "",
        calendarEventId: "",
        calendarHtmlLink: "",
      },
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      isRead: false,
      assignedTo: "conseiller",
      source: "twilio_voice",
    });

    sayFr(
      response,
      "Très bien. Après le bip, vous pouvez laisser votre message vocal. Quand vous avez terminé, raccrochez ou appuyez sur la touche dièse."
    );

    response.record({
      action: "/msg-voice-done",
      method: "POST",
      maxLength: 120,
      finishOnKey: "#",
      playBeep: true,
      trim: "trim-silence",
      timeout: 5,
      recordingStatusCallback: "/recording-status",
      recordingStatusCallbackMethod: "POST",
    });

    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-voice-phone-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre numéro. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-voice-done",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const response = vr();

    businessLog("VOICE_MESSAGE_FINISHED", {
      callSid,
      recordingSid: req.body?.RecordingSid || null,
      recordingUrl: req.body?.RecordingUrl || null,
      recordingDuration: req.body?.RecordingDuration || null,
      finishedAt: new Date().toISOString(),
    });

    hangupWithMessage(
      response,
      "Parfait. Votre message vocal a bien été enregistré. Merci de votre appel. Au revoir."
    );

    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/recording-status",
  twilioOnly((req, res) => {
    const callSid = req.body?.CallSid || "unknown";
    const session = sessions.get(callSid);
    const recordingSid = String(req.body?.RecordingSid || "");
    const recordingUrl = String(req.body?.RecordingUrl || "");
    const pendingId = session?.data?.pendingVoiceMessageId;

    if (pendingId) {
      updateContactEntry(pendingId, (item) => ({
        ...item,
        recordingSid,
        recordingUrl,
        recordingStatus: req.body?.RecordingStatus || item.recordingStatus || "",
        recordingDuration: req.body?.RecordingDuration || item.recordingDuration || "",
        updatedAt: new Date().toISOString(),
      }));
    } else {
      upsertContactEntryByPredicate(
        (item) => item.recordingSid && item.recordingSid === recordingSid,
        (existing) => ({
          id: existing?.id || crypto.randomUUID(),
          type: "voice_message",
          callSid,
          callerName: existing?.callerName || "",
          callerPhone: existing?.callerPhone || "",
          message: "",
          recordingSid,
          recordingUrl,
          recordingStatus:
            req.body?.RecordingStatus || existing?.recordingStatus || "",
          recordingDuration:
            req.body?.RecordingDuration || existing?.recordingDuration || "",
          brochure: existing?.brochure || {
            city: "",
            ref: "",
            link: "",
            whatsappPrefilledLink: "",
            title: "",
          },
          appointment: existing?.appointment || {
            date: "",
            time: "",
            calendarEventId: "",
            calendarHtmlLink: "",
          },
          createdAt: existing?.createdAt || new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          isRead: existing?.isRead || false,
          assignedTo: "conseiller",
          source: "twilio_voice",
        })
      );
    }

    resetSession(callSid);
    return res.status(200).send("ok");
  })
);

// ======================================================
// MESSAGE DICTE
// ======================================================
app.post(
  "/msg-name",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!raw) {
      gatherSpeech(
        response,
        "/msg-name",
        "Je n ai pas bien entendu. Répétez votre nom et prénom."
      );
      response.redirect({ method: "POST" }, "/msg-name-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.name = raw;

    gatherSpeech(response, "/msg-phone", "Quel est votre numéro de téléphone ?");
    response.redirect({ method: "POST" }, "/msg-phone-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-name-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre nom. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-phone",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!looksLikePhone(raw)) {
      gatherSpeech(
        response,
        "/msg-phone",
        "Je n ai pas bien compris le numéro. Répétez uniquement votre numéro de téléphone."
      );
      response.redirect({ method: "POST" }, "/msg-phone-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.phone = normalizeFrenchPhone(raw);

    gatherSpeech(response, "/msg-body", "Quel message souhaitez vous laisser ?");
    response.redirect({ method: "POST" }, "/msg-body-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-phone-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre numéro. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-body",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!raw) {
      gatherSpeech(
        response,
        "/msg-body",
        "Je n ai pas bien entendu. Répétez votre message, s il vous plaît."
      );
      response.redirect({ method: "POST" }, "/msg-body-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.message = raw;

    saveContactEntry({
      id: crypto.randomUUID(),
      type: "spoken_message",
      callSid,
      callerName: session.data.name,
      callerPhone: session.data.phone,
      message: session.data.message,
      recordingSid: "",
      recordingUrl: "",
      recordingStatus: "",
      recordingDuration: "",
      brochure: {
        city: "",
        ref: "",
        link: "",
        whatsappPrefilledLink: "",
        title: "",
      },
      appointment: {
        date: "",
        time: "",
        calendarEventId: "",
        calendarHtmlLink: "",
      },
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      isRead: false,
      assignedTo: "conseiller",
      source: "twilio_voice",
    });

    hangupWithMessage(
      response,
      "Parfait. Votre message pour le conseiller a bien été enregistré. Merci de votre appel. Au revoir."
    );
    resetSession(callSid);
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-body-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre message. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

// ======================================================
// GOODBYE
// ======================================================
app.post(
  "/goodbye",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Merci de votre appel. Au revoir.");
    res.type("text/xml").send(response.toString());
  })
);

// ======================================================
// ERROR HANDLING
// ======================================================
app.use((req, res) => {
  res.status(404).json({
    error: "Route introuvable",
  });
});

app.use((error, _req, res, _next) => {
  console.error("UNHANDLED_ERROR", error);
  res.status(500).json({
    error: "Internal Server Error",
    message:
      NODE_ENV === "development"
        ? String(error.message || error)
        : "Une erreur est survenue",
  });
});

process.on("unhandledRejection", (reason) => {
  console.error("UNHANDLED_REJECTION:", reason);
});

process.on("uncaughtException", (error) => {
  console.error("UNCAUGHT_EXCEPTION:", error);
});

// ======================================================
// START
// ======================================================
requireEnvInProduction();
checkEnvAtStartup();

console.log("APIMO token loaded:", !!APIMO_API_TOKEN);
console.log("APIMO provider:", APIMO_PROVIDER_ID);
console.log("APIMO agency:", APIMO_AGENCY_ID);
console.log("APIMO base url:", APIMO_BASE_URL);
console.log("OPENWEATHER enabled:", Boolean(OPENWEATHER_API_KEY));

app.listen(PORT, "0.0.0.0", () => {
  businessLog("SERVER_STARTED", {
    port: PORT,
    env: NODE_ENV,
    timezone: TZ,
    appBaseUrl: APP_BASE_URL,
    apimoCacheTtlMs: APIMO_CACHE_TTL_MS,
    weatherCacheTtlMs: WEATHER_CACHE_TTL_MS,
    appointmentDurationMinutes: APPOINTMENT_DURATION_MINUTES,
    twilioValidateSignature: TWILIO_VALIDATE_SIGNATURE,
  });
});
=======
require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const axios = require("axios");
const twilio = require("twilio");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { google } = require("googleapis");
const { DateTime } = require("luxon");
const OpenAI = require("openai");
const { Pool } = require("pg");

const app = express();
app.disable("x-powered-by");
app.set("trust proxy", 1);

app.use(express.urlencoded({ extended: false }));
app.use(express.json({ limit: "300kb" }));

// ======================================================
// CONFIG
// ======================================================
const PORT = Number(process.env.PORT || 3000);
const NODE_ENV = process.env.NODE_ENV || "development";
const APP_BASE_URL = process.env.APP_BASE_URL || "";
const TZ = process.env.APP_TIMEZONE || "Europe/Paris";

const ADMIN_USER = process.env.ADMIN_USER || "";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "";

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const CONTACTS_FILE = path.join(DATA_DIR, "contacts.json");

const SESSION_TTL_MINUTES = Number(process.env.SESSION_TTL_MINUTES || 30);
const MAX_CONTACTS = Number(process.env.MAX_CONTACTS || 5000);

const TWILIO_VALIDATE_SIGNATURE =
  String(process.env.TWILIO_VALIDATE_SIGNATURE || "true").toLowerCase() !==
  "false";

const OPENAI_INTENT_MODEL = process.env.OPENAI_INTENT_MODEL || "gpt-4o-mini";
const OPENAI_CITY_MODEL = process.env.OPENAI_CITY_MODEL || "gpt-4o-mini";
const OPENAI_REF_MODEL = process.env.OPENAI_REF_MODEL || "gpt-4o-mini";

const APIMO_API_TOKEN = String(process.env.APIMO_API_TOKEN || "").trim();
const APIMO_PROVIDER_ID = String(process.env.APIMO_PROVIDER_ID || "").trim();
const APIMO_AGENCY_ID = String(process.env.APIMO_AGENCY_ID || "").trim();
const APIMO_BASE_URL = process.env.APIMO_BASE_URL || "https://api.apimo.pro";
const APIMO_CACHE_TTL_MS = Number(process.env.APIMO_CACHE_TTL_MS || 60000);

const OPENWEATHER_API_KEY = process.env.OPENWEATHER_API_KEY || "";
const WEATHER_CACHE_TTL_MS = Number(process.env.WEATHER_CACHE_TTL_MS || 600000);

const SWIXIM_BASE_URL = process.env.SWIXIM_BASE_URL || "https://www.swixim.com";
const SWIXIM_AGENCY_URL =
  process.env.SWIXIM_AGENCY_URL ||
  "https://www.swixim.com/fr/agences/details/35/swixim-strasbourg";

const GOOGLE_CALENDAR_ID = process.env.GOOGLE_CALENDAR_ID || "primary";
const APPOINTMENT_DURATION_MINUTES = Number(
  process.env.APPOINTMENT_DURATION_MINUTES || 30
);
const DATABASE_URL = String(process.env.DATABASE_URL || "").trim();

const REQUIRED_PROD_SECRETS = [$1,
  "DATABASE_URL",
];

const KNOWN_CITIES = [
  "strasbourg",
  "erstein",
  "bischheim",
  "schiltigheim",
  "illkirch graffenstaden",
  "illkirch-graffenstaden",
  "lingolsheim",
  "hoerdt",
  "guemar",
  "wolfisheim",
  "cernay",
  "mulhouse",
  "saverne",
  "haguenau",
  "selestat",
  "molsheim",
  "obernai",
  "colmar",
  "benfeld",
  "geispolsheim",
  "ostwald",
  "brumath",
  "vendenheim",
  "la wantzenau",
  "entzheim",
  "kehl",
];

// ======================================================
// SECURITY
// ======================================================
app.use(
  helmet({
    contentSecurityPolicy: false,
  })
);

app.use(
  rateLimit({
    windowMs: 15 * 60 * 1000,
    max: NODE_ENV === "production" ? 300 : 5000,
    standardHeaders: true,
    legacyHeaders: false,
  })
);

app.use(
  "/admin",
  rateLimit({
    windowMs: 15 * 60 * 1000,
    max: NODE_ENV === "production" ? 100 : 1000,
    standardHeaders: true,
    legacyHeaders: false,
  })
);

// ======================================================
// CLIENTS
// ======================================================
const openai =
  process.env.OPENAI_API_KEY && String(process.env.OPENAI_API_KEY).trim()
    ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY })
    : null;

const apimo = axios.create({
  baseURL: APIMO_BASE_URL,
  auth: {
    username: APIMO_PROVIDER_ID,
    password: APIMO_API_TOKEN,
  },
  timeout: 10000,
  headers: {
    Accept: "application/json",
  },
  validateStatus: () => true,
});

// ======================================================
// LOGGING
// ======================================================
function businessLog(event, payload = {}) {
  console.log(
    `[${new Date().toISOString()}] ${event}`,
    JSON.stringify(payload, null, 2)
  );
}

// ======================================================
// STORAGE / DATABASE
// ======================================================
const pgPool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl:
        NODE_ENV === "production"
          ? { rejectUnauthorized: false }
          : false,
    })
  : null;

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function safeWriteJson(file, data) {
  const tmp = `${file}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2), "utf-8");
  fs.renameSync(tmp, file);
}

function initStorage() {
  if (pgPool) return;
  ensureDir(DATA_DIR);
  if (!fs.existsSync(CONTACTS_FILE)) {
    safeWriteJson(CONTACTS_FILE, []);
  }
}

async function initDatabase() {
  if (!pgPool) return;

  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS contacts (
      id TEXT PRIMARY KEY,
      type TEXT NOT NULL DEFAULT '',
      call_sid TEXT NOT NULL DEFAULT '',
      caller_name TEXT NOT NULL DEFAULT '',
      caller_phone TEXT NOT NULL DEFAULT '',
      message TEXT NOT NULL DEFAULT '',
      recording_sid TEXT NOT NULL DEFAULT '',
      recording_url TEXT NOT NULL DEFAULT '',
      recording_status TEXT NOT NULL DEFAULT '',
      recording_duration TEXT NOT NULL DEFAULT '',
      brochure JSONB NOT NULL DEFAULT '{}'::jsonb,
      appointment JSONB NOT NULL DEFAULT '{}'::jsonb,
      is_read BOOLEAN NOT NULL DEFAULT false,
      assigned_to TEXT NOT NULL DEFAULT '',
      source TEXT NOT NULL DEFAULT '',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
}

function readJsonFile(file, fallback) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf-8"));
  } catch {
    return fallback;
  }
}

function writeJsonFile(file, data) {
  safeWriteJson(file, data);
}

function normalizeStoredContact(item = {}) {
  return {
    id: String(item.id || ""),
    type: String(item.type || ""),
    callSid: String(item.callSid || item.call_sid || ""),
    callerName: String(item.callerName || item.caller_name || ""),
    callerPhone: String(item.callerPhone || item.caller_phone || ""),
    message: String(item.message || ""),
    recordingSid: String(item.recordingSid || item.recording_sid || ""),
    recordingUrl: String(item.recordingUrl || item.recording_url || ""),
    recordingStatus: String(item.recordingStatus || item.recording_status || ""),
    recordingDuration: String(item.recordingDuration || item.recording_duration || ""),
    brochure: item.brochure || {},
    appointment: item.appointment || {},
    createdAt: item.createdAt || item.created_at || new Date().toISOString(),
    updatedAt: item.updatedAt || item.updated_at || new Date().toISOString(),
    isRead: Boolean(item.isRead ?? item.is_read ?? false),
    assignedTo: String(item.assignedTo || item.assigned_to || ""),
    source: String(item.source || ""),
  };
}

async function readContacts() {
  if (!pgPool) {
    return readJsonFile(CONTACTS_FILE, []).map(normalizeStoredContact);
  }

  const result = await pgPool.query(
    `SELECT * FROM contacts ORDER BY created_at DESC LIMIT $1`,
    [MAX_CONTACTS]
  );

  return result.rows.map(normalizeStoredContact);
}

async function writeContacts(items) {
  if (!pgPool) {
    const next = Array.isArray(items) ? items.slice(0, MAX_CONTACTS) : [];
    writeJsonFile(CONTACTS_FILE, next);
    return;
  }

  throw new Error("writeContacts n'est pas supporté en mode Postgres");
}

async function saveContactEntry(entry) {
  const item = normalizeStoredContact(entry);

  if (!pgPool) {
    const items = await readContacts();
    items.unshift(item);
    await writeContacts(items);
    return item;
  }

  await pgPool.query(
    `
      INSERT INTO contacts (
        id, type, call_sid, caller_name, caller_phone, message,
        recording_sid, recording_url, recording_status, recording_duration,
        brochure, appointment, is_read, assigned_to, source, created_at, updated_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,
        $7,$8,$9,$10,
        $11::jsonb,$12::jsonb,$13,$14,$15,$16,$17
      )
      ON CONFLICT (id) DO UPDATE SET
        type = EXCLUDED.type,
        call_sid = EXCLUDED.call_sid,
        caller_name = EXCLUDED.caller_name,
        caller_phone = EXCLUDED.caller_phone,
        message = EXCLUDED.message,
        recording_sid = EXCLUDED.recording_sid,
        recording_url = EXCLUDED.recording_url,
        recording_status = EXCLUDED.recording_status,
        recording_duration = EXCLUDED.recording_duration,
        brochure = EXCLUDED.brochure,
        appointment = EXCLUDED.appointment,
        is_read = EXCLUDED.is_read,
        assigned_to = EXCLUDED.assigned_to,
        source = EXCLUDED.source,
        updated_at = EXCLUDED.updated_at
    `,
    [
      item.id,
      item.type,
      item.callSid,
      item.callerName,
      item.callerPhone,
      item.message,
      item.recordingSid,
      item.recordingUrl,
      item.recordingStatus,
      item.recordingDuration,
      JSON.stringify(item.brochure || {}),
      JSON.stringify(item.appointment || {}),
      item.isRead,
      item.assignedTo,
      item.source,
      item.createdAt,
      item.updatedAt,
    ]
  );

  return item;
}

async function upsertContactEntryByPredicate(predicate, createOrUpdate) {
  const items = await readContacts();
  const existing = items.find(predicate) || null;
  const nextItem = normalizeStoredContact(createOrUpdate(existing));
  await saveContactEntry(nextItem);
  return nextItem;
}

async function updateContactEntry(id, updater) {
  const items = await readContacts();
  const existing = items.find((item) => item.id === id);
  if (!existing) return null;
  const nextItem = normalizeStoredContact(updater(existing));
  await saveContactEntry(nextItem);
  return nextItem;
}

async function deleteContactEntry(id) {
  if (!pgPool) {
    const items = await readContacts();
    const next = items.filter((item) => item.id !== id);
    if (next.length === items.length) return false;
    await writeContacts(next);
    return true;
  }

  const result = await pgPool.query(`DELETE FROM contacts WHERE id = $1`, [id]);
  return result.rowCount > 0;
}

initStorage();

// ======================================================
// SESSIONS
// ======================================================
const sessions = new Map();

function getCallSid(req) {
  return req.body?.CallSid || "unknown";
}

function getNowParis() {
  return DateTime.now().setZone(TZ);
}

function cleanupExpiredSessions() {
  const now = Date.now();
  const ttlMs = SESSION_TTL_MINUTES * 60 * 1000;

  for (const [callSid, session] of sessions.entries()) {
    const touchedAt = new Date(
      session.touchedAt || session.createdAt || 0
    ).getTime();
    if (!touchedAt || now - touchedAt > ttlMs) {
      sessions.delete(callSid);
    }
  }
}

function getSession(callSid) {
  cleanupExpiredSessions();

  if (!sessions.has(callSid)) {
    sessions.set(callSid, {
      flow: null,
      unclearCount: 0,
      createdAt: new Date().toISOString(),
      touchedAt: new Date().toISOString(),
      data: {
        name: "",
        phone: "",
        city: "",
        refDigits: "",
        message: "",
        messageMode: "",
        dateText: "",
        timeText: "",
        calendarEventId: "",
        calendarHtmlLink: "",
        pendingVoiceMessageId: "",
      },
    });
  }

  const session = sessions.get(callSid);
  session.touchedAt = new Date().toISOString();
  return session;
}

function resetSession(callSid) {
  sessions.delete(callSid);
}

setInterval(cleanupExpiredSessions, 5 * 60 * 1000).unref();

// ======================================================
// CACHES
// ======================================================
const propertiesCache = {
  data: [],
  fetchedAt: 0,
};

function isApimoCacheFresh() {
  return (
    Array.isArray(propertiesCache.data) &&
    propertiesCache.data.length > 0 &&
    Date.now() - propertiesCache.fetchedAt < APIMO_CACHE_TTL_MS
  );
}

const weatherCache = new Map();

function getWeatherCacheKey(city = "") {
  return normalize(city);
}

function getCachedWeather(city = "") {
  const key = getWeatherCacheKey(city);
  const item = weatherCache.get(key);
  if (!item) return null;

  if (Date.now() - item.fetchedAt > WEATHER_CACHE_TTL_MS) {
    weatherCache.delete(key);
    return null;
  }

  return item.value;
}

function setCachedWeather(city = "", value = "") {
  const key = getWeatherCacheKey(city);
  weatherCache.set(key, {
    value,
    fetchedAt: Date.now(),
  });
}

// ======================================================
// UTILS
// ======================================================
function vr() {
  return new twilio.twiml.VoiceResponse();
}

function normalize(text = "") {
  return String(text)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^\w\s+/:.\-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanText(text = "") {
  return String(text).replace(/\s+/g, " ").trim();
}

function onlyDigits(text = "") {
  return String(text).replace(/\D/g, "");
}

function normalizeFrenchPhone(raw = "") {
  const digits = onlyDigits(raw);

  if (!digits) return "";
  if (digits.startsWith("00")) return `+${digits.slice(2)}`;
  if (digits.startsWith("33") && digits.length >= 11 && digits.length <= 15) {
    return `+${digits}`;
  }
  if (digits.startsWith("0") && digits.length === 10) return `+33${digits.slice(1)}`;
  if (digits.length === 9) return `+33${digits}`;
  if (digits.length >= 10 && digits.length <= 15) return `+${digits}`;

  return "";
}

function looksLikePhone(raw = "") {
  const normalizedPhone = normalizeFrenchPhone(raw);
  const digits = onlyDigits(normalizedPhone);
  return digits.length >= 11 && digits.length <= 15;
}

function normalizeWaRecipientPhone(raw = "") {
  const phone = normalizeFrenchPhone(raw);
  return onlyDigits(phone);
}

function isSafeId(value = "") {
  return /^[a-zA-Z0-9_-]{1,100}$/.test(String(value));
}

function sayFr(node, text) {
  node.say({ language: "fr-FR" }, text);
}

function gatherSpeech(response, action, prompt, hints = "") {
  const gather = response.gather({
    input: "speech dtmf",
    action,
    method: "POST",
    language: "fr-FR",
    speechTimeout: "auto",
    timeout: 6,
    hints,
  });

  sayFr(gather, prompt);
  return gather;
}

function hangupWithMessage(response, text) {
  sayFr(response, text);
  response.hangup();
}

function escapeHtml(value = "") {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatLocalDateTime(isoString = "") {
  if (!isoString) return "";
  const dt = DateTime.fromISO(isoString, { zone: "utc" }).setZone(TZ);
  if (!dt.isValid) return isoString;
  return dt.setLocale("fr").toLocaleString(DateTime.DATETIME_SHORT);
}

function spellDigitsForSpeech(digits = "") {
  return String(digits).split("").join(" ");
}

function monthNameToNumber(name) {
  const months = {
    janvier: 1,
    fevrier: 2,
    mars: 3,
    avril: 4,
    mai: 5,
    juin: 6,
    juillet: 7,
    aout: 8,
    septembre: 9,
    octobre: 10,
    novembre: 11,
    decembre: 12,
  };
  return months[name] || null;
}

function safeEqualString(a = "", b = "") {
  const aBuf = Buffer.from(String(a));
  const bBuf = Buffer.from(String(b));
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

function getRequestAbsoluteUrl(req) {
  if (APP_BASE_URL) {
    const base = APP_BASE_URL.replace(/\/$/, "");
    return `${base}${req.originalUrl}`;
  }

  const proto = req.headers["x-forwarded-proto"] || req.protocol || "http";
  const host = req.headers["x-forwarded-host"] || req.get("host");
  return `${proto}://${host}${req.originalUrl}`;
}

function validateTwilioWebhook(req) {
  if (!TWILIO_VALIDATE_SIGNATURE) return true;

  const authToken = process.env.TWILIO_AUTH_TOKEN;
  const signature = req.headers["x-twilio-signature"];

  if (!authToken || !signature) return false;

  const url = getRequestAbsoluteUrl(req);

  try {
    return twilio.validateRequest(authToken, signature, url, req.body || {});
  } catch (error) {
    console.error("Erreur validation Twilio signature", error);
    return false;
  }
}

function twilioOnly(handler) {
  return (req, res, next) => {
    if (!validateTwilioWebhook(req)) {
      return res.status(403).send("Forbidden");
    }
    return handler(req, res, next);
  };
}

function requireEnvInProduction() {
  if (NODE_ENV !== "production") return;

  const missing = REQUIRED_PROD_SECRETS.filter((key) => !process.env[key]);
  if (missing.length) {
    throw new Error(
      `Variables d'environnement manquantes en production: ${missing.join(", ")}`
    );
  }
}

function checkEnvAtStartup() {
  const missing = [];
  if (!APIMO_API_TOKEN) missing.push("APIMO_API_TOKEN");
  if (!APIMO_PROVIDER_ID) missing.push("APIMO_PROVIDER_ID");
  if (!APIMO_AGENCY_ID) missing.push("APIMO_AGENCY_ID");

  if (missing.length) {
    throw new Error(`Variables manquantes: ${missing.join(", ")}`);
  }
}

function checkBasicAuth(req) {
  if (!ADMIN_USER || !ADMIN_PASSWORD) {
    return { ok: false, status: 503, message: "Admin non configuré" };
  }

  const auth = req.headers.authorization;
  if (!auth || !auth.startsWith("Basic ")) {
    return { ok: false, status: 401, message: "Authentification requise" };
  }

  const base64 = auth.split(" ")[1];
  const decoded = Buffer.from(base64, "base64").toString("utf-8");
  const separatorIndex = decoded.indexOf(":");

  if (separatorIndex === -1) {
    return { ok: false, status: 401, message: "Accès refusé" };
  }

  const username = decoded.slice(0, separatorIndex);
  const password = decoded.slice(separatorIndex + 1);

  if (!safeEqualString(username, ADMIN_USER) || !safeEqualString(password, ADMIN_PASSWORD)) {
    return { ok: false, status: 401, message: "Accès refusé" };
  }

  return { ok: true };
}

// ======================================================
// FRENCH NUMBER PARSING
// ======================================================
const NUMBER_WORDS = {
  zero: 0,
  un: 1,
  une: 1,
  deux: 2,
  trois: 3,
  quatre: 4,
  cinq: 5,
  six: 6,
  sept: 7,
  huit: 8,
  neuf: 9,
  dix: 10,
  onze: 11,
  douze: 12,
  treize: 13,
  quatorze: 14,
  quinze: 15,
  seize: 16,
  dixsept: 17,
  dixhuit: 18,
  dixneuf: 19,
  vingt: 20,
  trente: 30,
  quarante: 40,
  cinquante: 50,
};

function parseFrenchNumberWords(text = "") {
  const cleaned = normalize(text)
    .replace(/vingt et un/g, "vingt un")
    .replace(/trente et un/g, "trente un")
    .replace(/quarante et un/g, "quarante un")
    .replace(/cinquante et un/g, "cinquante un");

  const parts = cleaned.split(/[\s\-]+/).filter(Boolean);
  let total = 0;
  let current = 0;
  let used = false;

  for (const part of parts) {
    if (part === "et") continue;

    if (part === "cent" || part === "cents") {
      current = (current || 1) * 100;
      used = true;
      continue;
    }

    if (part === "mille") {
      total += (current || 1) * 1000;
      current = 0;
      used = true;
      continue;
    }

    const compact = part.replace(/[\s\-]/g, "");
    if (compact in NUMBER_WORDS) {
      current += NUMBER_WORDS[compact];
      used = true;
      continue;
    }

    if (/^\d+$/.test(part)) {
      current += Number(part);
      used = true;
      continue;
    }

    return { ok: false };
  }

  if (!used) return { ok: false };
  return { ok: true, value: total + current };
}

// ======================================================
// AI HELPERS
// ======================================================
function detectIntentByRules(text = "") {
  const t = normalize(text);

  if (
    t.includes("fiche") ||
    t.includes("descriptive") ||
    t.includes("descriptif") ||
    t.includes("description du bien")
  ) {
    return "fiche";
  }

  if (
    t.includes("message") ||
    t.includes("laisser un message") ||
    t.includes("message vocal") ||
    t.includes("conseiller")
  ) {
    return "message";
  }

  if (
    t.includes("rendez vous") ||
    t.includes("rendezvous") ||
    t.includes("rendez") ||
    t.includes("rdv")
  ) {
    return "rdv";
  }

  return null;
}

async function detectIntentWithAI(text = "") {
  if (!openai || !text.trim()) return null;

  try {
    const response = await openai.responses.create({
      model: OPENAI_INTENT_MODEL,
      store: false,
      max_output_tokens: 20,
      input: [
        {
          role: "system",
          content: [
            {
              type: "input_text",
              text:
                "Classe l'intention utilisateur en un seul mot parmi: fiche, message, rdv, unknown. Réponds uniquement par un de ces quatre mots.",
            },
          ],
        },
        {
          role: "user",
          content: [{ type: "input_text", text }],
        },
      ],
    });

    const out = (response.output_text || "").trim().toLowerCase();
    if (["fiche", "message", "rdv"].includes(out)) return out;
    return null;
  } catch (error) {
    console.error("detectIntentWithAI error", error.message || error);
    return null;
  }
}

async function detectIntent(text = "") {
  const byRules = detectIntentByRules(text);
  if (byRules) return byRules;
  return await detectIntentWithAI(text);
}

function normalizeCityDeterministic(raw = "") {
  const text = normalize(raw);
  if (!text) return "";

  const exact = KNOWN_CITIES.find((city) => city === text);
  if (exact) return exact;

  const partial = KNOWN_CITIES.find(
    (city) => city.includes(text) || text.includes(city)
  );
  if (partial) return partial;

  return raw.trim();
}

function toTitleCaseFr(input = "") {
  return String(input)
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(" ");
}

async function normalizeCity(raw = "") {
  const byRules = normalizeCityDeterministic(raw);
  if (normalize(byRules) !== normalize(raw) || KNOWN_CITIES.includes(normalize(byRules))) {
    return toTitleCaseFr(byRules);
  }

  if (!openai || !raw.trim()) return raw.trim();

  try {
    const response = await openai.responses.create({
      model: OPENAI_CITY_MODEL,
      store: false,
      max_output_tokens: 20,
      input: [
        {
          role: "system",
          content: [
            {
              type: "input_text",
              text:
                "Corrige un nom de ville française dicté à la voix. Réponds uniquement avec le nom de la ville, sans phrase.",
            },
          ],
        },
        {
          role: "user",
          content: [{ type: "input_text", text: raw }],
        },
      ],
    });

    const aiCity = (response.output_text || raw).trim();
    const finalCity = normalizeCityDeterministic(aiCity);
    return toTitleCaseFr(finalCity || aiCity);
  } catch (error) {
    console.error("normalizeCity error", error.message || error);
    return raw.trim();
  }
}

function parseRefDigitsByRules(raw = "") {
  const text = normalize(raw);
  if (!text) return { ok: false };

  let result = text;

  for (const [word, digit] of Object.entries({
    zero: "0",
    un: "1",
    une: "1",
    deux: "2",
    trois: "3",
    quatre: "4",
    cinq: "5",
    six: "6",
    sept: "7",
    huit: "8",
    neuf: "9",
  })) {
    result = result.replace(new RegExp(`\\b${word}\\b`, "g"), digit);
  }

  const digits = onlyDigits(result);
  if (!digits || digits.length < 3) return { ok: false };

  return { ok: true, digits };
}

async function parseRefDigitsWithAI(raw = "") {
  if (!openai || !raw.trim()) return { ok: false };

  try {
    const response = await openai.responses.create({
      model: OPENAI_REF_MODEL,
      store: false,
      max_output_tokens: 20,
      input: [
        {
          role: "system",
          content: [
            {
              type: "input_text",
              text:
                "Extrais uniquement les chiffres d'une référence immobilière dictée oralement. Réponds uniquement avec les chiffres. Si impossible, réponds NONE.",
            },
          ],
        },
        {
          role: "user",
          content: [{ type: "input_text", text: raw }],
        },
      ],
    });

    const out = (response.output_text || "").trim();
    if (out.toUpperCase() === "NONE") return { ok: false };

    const digits = onlyDigits(out);
    if (!digits || digits.length < 3) return { ok: false };

    return { ok: true, digits };
  } catch (error) {
    console.error("parseRefDigitsWithAI error", error.message || error);
    return { ok: false };
  }
}

async function parseRefDigits(raw = "") {
  const byRules = parseRefDigitsByRules(raw);
  if (byRules.ok) return byRules;
  return await parseRefDigitsWithAI(raw);
}

// ======================================================
// WEATHER HELPERS
// ======================================================
function buildWeatherJoke(city, temp, description = "") {
  const d = normalize(description);

  if (d.includes("pluie") || d.includes("averse")) {
    return `À ${city}, il pleut aujourd hui. Bon, au moins c est parfait pour tester l isolation sans supplément.`;
  }

  if (d.includes("orage")) {
    return `À ${city}, il y a de l orage. Ambiance dramatique idéale, mais on préfère quand même les visites sans effets spéciaux.`;
  }

  if (d.includes("neige")) {
    return `À ${city}, il neige. Très joli, et excellent test grandeur nature pour le chauffage.`;
  }

  if (temp >= 28) {
    return `À ${city}, il fait ${temp} degrés. Disons que la visite permet aussi de vérifier la climatisation.`;
  }

  if (temp <= 3) {
    return `À ${city}, il fait ${temp} degrés. Une bonne occasion de juger le chauffage dès l entrée.`;
  }

  if (d.includes("soleil") || d.includes("degage") || d.includes("ciel clair")) {
    return `À ${city}, il fait ${temp} degrés avec du soleil. Franchement, la météo fait déjà la moitié de la visite.`;
  }

  return `À ${city}, il fait ${temp} degrés avec ${description}. Météo neutre, donc le bien devra séduire tout seul.`;
}

async function getWeatherComment(city = "") {
  try {
    if (!OPENWEATHER_API_KEY || !city) return "";

    const cached = getCachedWeather(city);
    if (cached) return cached;

    const response = await axios.get(
      "https://api.openweathermap.org/data/2.5/weather",
      {
        timeout: 5000,
        params: {
          q: `${city},FR`,
          units: "metric",
          lang: "fr",
          appid: OPENWEATHER_API_KEY,
        },
        validateStatus: () => true,
      }
    );

    if (response.status >= 400 || !response.data?.main) {
      return "";
    }

    const temp = Math.round(Number(response.data.main.temp));
    const description = String(
      response.data.weather?.[0]?.description || ""
    ).trim();

    if (!Number.isFinite(temp) || !description) {
      return "";
    }

    const sentence = buildWeatherJoke(city, temp, description);
    setCachedWeather(city, sentence);
    return sentence;
  } catch (error) {
    console.error("Weather error", error.message || error);
    return "";
  }
}

// ======================================================
// APIMO HELPERS
// ======================================================
async function fetchPropertiesFromApimo() {
  const url = `/agencies/${APIMO_AGENCY_ID}/properties`;

  console.log("APIMO token loaded:", !!APIMO_API_TOKEN);
  console.log("APIMO provider:", APIMO_PROVIDER_ID);
  console.log("APIMO agency:", APIMO_AGENCY_ID);
  console.log("APIMO base url:", APIMO_BASE_URL);
  console.log("APIMO URL:", `${APIMO_BASE_URL}${url}`);

  const response = await apimo.get(url);

  console.log("APIMO STATUS:", response.status);

  if (response.status >= 400) {
    throw new Error(
      `APIMO HTTP ${response.status}: ${JSON.stringify(response.data || {})}`
    );
  }

  const data = response.data;

  if (Array.isArray(data)) return data;
  if (Array.isArray(data?.properties)) return data.properties;
  if (Array.isArray(data?.items)) return data.items;

  return [];
}

async function fetchProperties({ forceRefresh = false } = {}) {
  if (!forceRefresh && isApimoCacheFresh()) {
    return propertiesCache.data;
  }

  const properties = await fetchPropertiesFromApimo();
  propertiesCache.data = properties;
  propertiesCache.fetchedAt = Date.now();

  businessLog("APIMO_PROPERTIES_REFRESHED", {
    count: properties.length,
    fetchedAt: new Date(propertiesCache.fetchedAt).toISOString(),
  });

  return properties;
}

function extractPropertyId(property) {
  return String(
    property?.id ?? property?.property_id ?? property?.propertyId ?? ""
  );
}

function extractPropertyTitle(property) {
  return cleanText(
    property?.title ||
      property?.reference ||
      property?.category ||
      property?.type ||
      "Bien immobilier"
  );
}

function extractPropertyCity(property) {
  const city =
    property?.city?.name ||
    property?.city?.label ||
    property?.city?.value ||
    property?.address?.city?.name ||
    property?.address?.city?.label ||
    property?.address?.city ||
    property?.location?.city?.name ||
    property?.location?.city?.label ||
    property?.location?.city ||
    property?.city;

  return typeof city === "string" ? cleanText(city) : "";
}

function extractPropertySurface(property) {
  const surface =
    property?.surface ??
    property?.area ??
    property?.living_area ??
    property?.land_area;
  const n = Number(surface);
  return Number.isFinite(n) ? `${n} m²` : "";
}

function extractPropertyPrice(property) {
  const n = Number(
    property?.price ?? property?.amount ?? property?.sale_price
  );
  return Number.isFinite(n)
    ? `${n.toLocaleString("fr-FR")} €`
    : "Prix sur demande";
}

function extractPropertyReference(property) {
  return cleanText(
    property?.reference || property?.ref || property?.property_reference || ""
  );
}

function extractPropertyUrl(property) {
  const url =
    property?.url ||
    property?.public_url ||
    property?.share_url ||
    property?.link ||
    "";

  return typeof url === "string" ? cleanText(url) : "";
}

function sanitizeProperty(property) {
  return {
    id: extractPropertyId(property),
    title: extractPropertyTitle(property),
    city: extractPropertyCity(property),
    reference: extractPropertyReference(property),
    price: extractPropertyPrice(property),
    surface: extractPropertySurface(property),
    url: extractPropertyUrl(property),
    raw: property,
  };
}

function formatPropertyMessage(property) {
  const p = sanitizeProperty(property);
  const lines = [
    `🏡 ${p.title}`,
    p.city ? `📍 ${p.city}` : "",
    p.reference ? `🔖 Réf : ${p.reference}` : "",
    "",
    `💰 ${p.price}`,
    p.surface ? `📐 ${p.surface}` : "",
    "",
    p.url ? `👉 ${p.url}` : `👉 ${SWIXIM_AGENCY_URL || SWIXIM_BASE_URL}`,
  ].filter(Boolean);

  return lines.join("\n").trim();
}

function generateWhatsAppPrefilledLink(toPhone, message) {
  const recipient = normalizeWaRecipientPhone(toPhone);
  if (!recipient) return "";

  const encoded = encodeURIComponent(message);
  return `https://wa.me/${recipient}?text=${encoded}`;
}

function buildPropertyPayload(property) {
  const safe = sanitizeProperty(property);
  const message = formatPropertyMessage(property);

  return {
    property: safe,
    message,
  };
}

function findPropertyById(properties, id) {
  return properties.find((property) => extractPropertyId(property) === String(id));
}

function parseDateToTimestamp(value) {
  const ts = Date.parse(value);
  return Number.isFinite(ts) ? ts : 0;
}

function findLatestProperty(properties) {
  if (!Array.isArray(properties) || !properties.length) return null;

  return [...properties].sort((a, b) => {
    const aTs = Math.max(
      parseDateToTimestamp(a?.updated_at),
      parseDateToTimestamp(a?.created_at),
      parseDateToTimestamp(a?.updatedAt),
      parseDateToTimestamp(a?.createdAt)
    );
    const bTs = Math.max(
      parseDateToTimestamp(b?.updated_at),
      parseDateToTimestamp(b?.created_at),
      parseDateToTimestamp(b?.updatedAt),
      parseDateToTimestamp(b?.createdAt)
    );
    return bTs - aTs;
  })[0];
}

function findPropertyFromApimoList({ properties, city, refDigits }) {
  const wantedCity = normalize(city);

  const candidates = properties
    .map((item) => {
      const safe = sanitizeProperty(item);
      let score = 0;

      const refDigitsItem = onlyDigits(safe.reference);
      const idDigits = onlyDigits(safe.id);

      if (!refDigits) return null;

      if (refDigitsItem === refDigits) score += 100;
      else if (refDigitsItem.startsWith(refDigits)) score += 80;
      else if (idDigits === refDigits) score += 120;
      else if (idDigits.startsWith(refDigits)) score += 90;

      const cityNorm = normalize(safe.city);
      if (wantedCity) {
        if (cityNorm === wantedCity) score += 80;
        else if (cityNorm.includes(wantedCity)) score += 45;
        else if (wantedCity.includes(cityNorm)) score += 25;
      }

      return { item, safe, score };
    })
    .filter(Boolean)
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score);

  businessLog("APIMO_MATCH_DEBUG", {
    city,
    refDigits,
    candidateCount: candidates.length,
    top: candidates.slice(0, 5).map((x) => ({
      score: x.score,
      id: x.safe.id,
      city: x.safe.city,
      reference: x.safe.reference,
      title: x.safe.title,
    })),
  });

  if (!candidates.length) return null;
  if (candidates[0].score < 80) return null;
  return candidates[0].item;
}

// ======================================================
// GOOGLE CALENDAR
// ======================================================
function getGoogleAuth() {
  const clientEmail = process.env.GOOGLE_CLIENT_EMAIL;
  const privateKey = process.env.GOOGLE_PRIVATE_KEY;

  if (!clientEmail || !privateKey) {
    throw new Error("GOOGLE_CLIENT_EMAIL ou GOOGLE_PRIVATE_KEY manquant");
  }

  return new google.auth.JWT({
    email: clientEmail,
    key: privateKey.replace(/\\n/g, "\n"),
    scopes: ["https://www.googleapis.com/auth/calendar"],
  });
}

async function getCalendarClient() {
  const auth = getGoogleAuth();
  return google.calendar({ version: "v3", auth });
}

function parseFrenchDate(raw = "") {
  const text = normalize(raw);
  const now = getNowParis();

  function toIso(year, month, day) {
    const dt = DateTime.fromObject({ year, month, day }, { zone: TZ });
    if (!dt.isValid) return { ok: false };
    return {
      ok: true,
      year,
      month,
      day,
      isoDate: dt.toISODate(),
    };
  }

  function addDays(baseDate, days) {
    const d = baseDate.plus({ days });
    return toIso(d.year, d.month, d.day);
  }

  function nextWeekday(targetDay) {
    let d = now.startOf("day");
    while (d.weekday % 7 !== targetDay) {
      d = d.plus({ days: 1 });
    }
    if (d <= now.startOf("day")) d = d.plus({ days: 7 });
    return toIso(d.year, d.month, d.day);
  }

  if (text.includes("aujourd hui")) return toIso(now.year, now.month, now.day);
  if (text.includes("apres demain")) return addDays(now, 2);
  if (text.includes("demain")) return addDays(now, 1);

  const weekdays = {
    dimanche: 0,
    lundi: 1,
    mardi: 2,
    mercredi: 3,
    jeudi: 4,
    vendredi: 5,
    samedi: 6,
  };

  for (const [name, dayNumber] of Object.entries(weekdays)) {
    if (text.includes(name)) {
      return nextWeekday(dayNumber);
    }
  }

  let match = text.match(/\b(\d{1,2})[\/\-\s](\d{1,2})[\/\-\s](\d{4})\b/);
  if (match) {
    return toIso(Number(match[3]), Number(match[2]), Number(match[1]));
  }

  match = text.match(
    /\b(\d{1,2})\s+(janvier|fevrier|mars|avril|mai|juin|juillet|aout|septembre|octobre|novembre|decembre)\s+(\d{4})\b/
  );
  if (match) {
    const month = monthNameToNumber(match[2]);
    return toIso(Number(match[3]), month, Number(match[1]));
  }

  match = text.match(
    /\b(\d{1,2})\s+(janvier|fevrier|mars|avril|mai|juin|juillet|aout|septembre|octobre|novembre|decembre)\b/
  );
  if (match) {
    const day = Number(match[1]);
    const month = monthNameToNumber(match[2]);
    let year = now.year;

    const candidate = DateTime.fromObject({ year, month, day }, { zone: TZ });
    if (!candidate.isValid) return { ok: false };
    if (candidate.startOf("day") < now.startOf("day")) year += 1;
    return toIso(year, month, day);
  }

  match = text.match(
    /\b([a-z\s\-]+)\s+(janvier|fevrier|mars|avril|mai|juin|juillet|aout|septembre|octobre|novembre|decembre)\s+([a-z\s\-\d]+)\b/
  );
  if (match) {
    const dayParsed = parseFrenchNumberWords(match[1]);
    const month = monthNameToNumber(match[2]);
    const yearParsed = parseFrenchNumberWords(match[3]);
    if (dayParsed.ok && yearParsed.ok && month) {
      return toIso(yearParsed.value, month, dayParsed.value);
    }
  }

  match = text.match(
    /\b([a-z\s\-]+)\s+(janvier|fevrier|mars|avril|mai|juin|juillet|aout|septembre|octobre|novembre|decembre)\b/
  );
  if (match) {
    const dayParsed = parseFrenchNumberWords(match[1]);
    const month = monthNameToNumber(match[2]);
    if (dayParsed.ok && month) {
      let year = now.year;
      const candidate = DateTime.fromObject(
        { year, month, day: dayParsed.value },
        { zone: TZ }
      );
      if (!candidate.isValid) return { ok: false };
      if (candidate.startOf("day") < now.startOf("day")) year += 1;
      return toIso(year, month, dayParsed.value);
    }
  }

  return { ok: false };
}

function parseFrenchTime(raw = "") {
  const text = normalize(raw);

  let match = text.match(/\b(\d{1,2})[:h](\d{2})\b/);
  if (match) {
    const hour = Number(match[1]);
    const minute = Number(match[2]);
    if (hour >= 0 && hour <= 23 && minute >= 0 && minute <= 59) {
      return {
        ok: true,
        hour,
        minute,
        hhmm: `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`,
      };
    }
  }

  match = text.match(/\b(\d{1,2})(?:\s*(?:heures|heure|h))\s*(\d{1,2})?\b/);
  if (match) {
    const hour = Number(match[1]);
    const minute = match[2] ? Number(match[2]) : 0;
    if (hour >= 0 && hour <= 23 && minute >= 0 && minute <= 59) {
      return {
        ok: true,
        hour,
        minute,
        hhmm: `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`,
      };
    }
  }

  match = text.match(/^\d{1,2}$/);
  if (match) {
    const hour = Number(text);
    if (hour >= 0 && hour <= 23) {
      return {
        ok: true,
        hour,
        minute: 0,
        hhmm: `${String(hour).padStart(2, "0")}:00`,
      };
    }
  }

  const wordHourMinuteMatch = text.match(
    /^([a-z\s\-]+?)(?:\s+heure(?:s)?\s*|\s+h\s*|\s+)([a-z\s\-]+)?$/
  );

  if (wordHourMinuteMatch) {
    const hourParsed = parseFrenchNumberWords(wordHourMinuteMatch[1] || "");
    const minuteParsed = wordHourMinuteMatch[2]
      ? parseFrenchNumberWords(wordHourMinuteMatch[2])
      : { ok: true, value: 0 };

    if (
      hourParsed.ok &&
      minuteParsed.ok &&
      hourParsed.value >= 0 &&
      hourParsed.value <= 23 &&
      minuteParsed.value >= 0 &&
      minuteParsed.value <= 59
    ) {
      return {
        ok: true,
        hour: hourParsed.value,
        minute: minuteParsed.value,
        hhmm: `${String(hourParsed.value).padStart(2, "0")}:${String(minuteParsed.value).padStart(2, "0")}`,
      };
    }
  }

  return { ok: false };
}

function isFutureSlot(isoDate, hhmm) {
  const dt = DateTime.fromISO(`${isoDate}T${hhmm}`, { zone: TZ });
  if (!dt.isValid) return false;
  return dt.toMillis() > getNowParis().toMillis();
}

async function isCalendarSlotAvailable({ isoDate, hhmm, durationMinutes }) {
  const calendar = await getCalendarClient();
  const start = DateTime.fromISO(`${isoDate}T${hhmm}`, { zone: TZ });
  const end = start.plus({ minutes: durationMinutes });

  const response = await calendar.events.list({
    calendarId: GOOGLE_CALENDAR_ID,
    timeMin: start.toUTC().toISO(),
    timeMax: end.toUTC().toISO(),
    singleEvents: true,
    orderBy: "startTime",
  });

  return (response.data.items || []).length === 0;
}

async function findAlternativeSlotsForDay({
  isoDate,
  durationMinutes,
  excludeHhmm,
  businessHoursStart = 9,
  businessHoursEnd = 18,
  slotStepMinutes = 30,
  maxSuggestions = 3,
}) {
  const results = [];

  for (
    let minutes = businessHoursStart * 60;
    minutes <= businessHoursEnd * 60 - durationMinutes;
    minutes += slotStepMinutes
  ) {
    const hour = Math.floor(minutes / 60);
    const minute = minutes % 60;
    const hhmm = `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;

    if (hhmm === excludeHhmm) continue;
    if (!isFutureSlot(isoDate, hhmm)) continue;

    const available = await isCalendarSlotAvailable({
      isoDate,
      hhmm,
      durationMinutes,
    });

    if (available) {
      results.push(hhmm);
      if (results.length >= maxSuggestions) break;
    }
  }

  return results;
}

async function createCalendarEvent({ name, phone, isoDate, hhmm }) {
  const calendar = await getCalendarClient();
  const start = DateTime.fromISO(`${isoDate}T${hhmm}`, { zone: TZ });
  const end = start.plus({ minutes: APPOINTMENT_DURATION_MINUTES });

  const result = await calendar.events.insert({
    calendarId: GOOGLE_CALENDAR_ID,
    requestBody: {
      summary: `RDV téléphonique - ${name}`,
      description: `Demande reçue via agent vocal Swixim\nNom: ${name}\nTéléphone: ${phone}`,
      start: {
        dateTime: start.toISO(),
        timeZone: TZ,
      },
      end: {
        dateTime: end.toISO(),
        timeZone: TZ,
      },
    },
  });

  return result.data;
}

// ======================================================
// ADMIN HTML
// ======================================================
function renderAdminPage(items) {
  const rows = items
    .map((item) => {
      const typeLabel =
        item.type === "voice_message"
          ? "Message vocal"
          : item.type === "spoken_message"
            ? "Message dicté"
            : item.type === "brochure_request"
              ? "Demande de fiche"
              : item.type === "appointment_request"
                ? "Rendez-vous"
                : item.type || "-";

      const statusLabel = item.isRead ? "Lu" : "Non lu";

      const messageCell = item.message ? escapeHtml(item.message) : "-";

      const audioCell = item.recordingUrl
        ? `
          <audio controls preload="none" style="max-width:220px;">
            <source src="/admin/audio/${encodeURIComponent(item.id)}" type="audio/mpeg" />
            Votre navigateur ne supporte pas l'audio.
          </audio>
        `
        : "-";

      const appointmentCell =
        item.appointment?.date || item.appointment?.time
          ? `${escapeHtml(item.appointment?.date || "")} ${escapeHtml(item.appointment?.time || "")}`.trim()
          : "-";

      const calendarCell = item.appointment?.calendarHtmlLink
        ? `<a href="${escapeHtml(item.appointment.calendarHtmlLink)}" target="_blank" rel="noreferrer">Voir</a>`
        : "-";

      const brochureRef = item.brochure?.ref ? escapeHtml(item.brochure.ref) : "-";
      const brochureCity = item.brochure?.city ? escapeHtml(item.brochure.city) : "-";
      const brochureLink = item.brochure?.link
        ? `<a href="${escapeHtml(item.brochure.link)}" target="_blank" rel="noreferrer">Ouvrir</a>`
        : "-";

      const waLink = item.brochure?.whatsappPrefilledLink
        ? `<a href="${escapeHtml(item.brochure.whatsappPrefilledLink)}" target="_blank" rel="noreferrer">Ouvrir</a>`
        : "-";

      return `
        <tr>
          <td>${escapeHtml(formatLocalDateTime(item.createdAt))}</td>
          <td>${escapeHtml(typeLabel)}</td>
          <td>${escapeHtml(item.callerName || "-")}</td>
          <td>${escapeHtml(item.callerPhone || "-")}</td>
          <td>${brochureCity}</td>
          <td>${brochureRef}</td>
          <td>${brochureLink}</td>
          <td>${waLink}</td>
          <td>${messageCell}</td>
          <td>${audioCell}</td>
          <td>${escapeHtml(appointmentCell)}</td>
          <td>${calendarCell}</td>
          <td>${escapeHtml(statusLabel)}</td>
          <td>
            <form method="POST" action="/admin/mark-read" style="display:inline-block; margin:0 4px 4px 0;">
              <input type="hidden" name="id" value="${escapeHtml(item.id)}" />
              <button type="submit">${item.isRead ? "Marquer non lu" : "Marquer lu"}</button>
            </form>
            <form method="POST" action="/admin/delete" style="display:inline-block; margin:0 4px 4px 0;" onsubmit="return confirm('Supprimer cette entrée ?');">
              <input type="hidden" name="id" value="${escapeHtml(item.id)}" />
              <button type="submit">Supprimer</button>
            </form>
          </td>
        </tr>
      `;
    })
    .join("");

  return `
    <!DOCTYPE html>
    <html lang="fr">
      <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Admin Swixim</title>
        <style>
          * { box-sizing: border-box; }
          body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 24px;
            background: #f4f6f8;
            color: #1f2937;
          }
          h1 { margin-top: 0; }
          .topbar {
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap:16px;
            margin-bottom:20px;
            flex-wrap:wrap;
          }
          .card {
            background:white;
            border-radius:12px;
            padding:16px;
            box-shadow:0 2px 10px rgba(0,0,0,0.06);
            margin-bottom:20px;
          }
          .meta { color:#6b7280; font-size:14px; }
          .table-wrap { overflow-x:auto; }
          table { width:100%; border-collapse:collapse; background:white; }
          th, td {
            padding:12px;
            border-bottom:1px solid #e5e7eb;
            text-align:left;
            vertical-align:top;
            font-size:14px;
          }
          th { background:#f9fafb; position:sticky; top:0; }
          button {
            border:none;
            background:#111827;
            color:white;
            padding:8px 12px;
            border-radius:8px;
            cursor:pointer;
          }
          button:hover { opacity:0.92; }
          a { color:#2563eb; text-decoration:none; }
          a:hover { text-decoration:underline; }
          audio { width:220px; }
        </style>
      </head>
      <body>
        <div class="topbar">
          <div>
            <h1>Administration Swixim</h1>
            <div class="meta">${items.length} entrée(s)</div>
          </div>
          <div class="meta"><a href="/admin/json">Version JSON</a></div>
        </div>

        <div class="card">
          <strong>Types gérés :</strong> messages vocaux, messages dictés, demandes de fiche, rendez-vous.
        </div>

        <div class="card table-wrap">
          <table>
            <thead>
              <tr>
                <th>Date</th>
                <th>Type</th>
                <th>Nom</th>
                <th>Téléphone</th>
                <th>Ville</th>
                <th>Réf bien</th>
                <th>Fiche</th>
                <th>WA lien</th>
                <th>Message</th>
                <th>Audio</th>
                <th>RDV</th>
                <th>Agenda</th>
                <th>Statut</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              ${rows || `<tr><td colspan="14">Aucune donnée.</td></tr>`}
            </tbody>
          </table>
        </div>
      </body>
    </html>
  `;
}

// ======================================================
// TWILIO AUDIO FETCH
// ======================================================
async function fetchTwilioRecordingMp3(recordingUrl) {
  const accountSid = process.env.TWILIO_ACCOUNT_SID;
  const authToken = process.env.TWILIO_AUTH_TOKEN;

  if (!accountSid || !authToken) {
    throw new Error("TWILIO_ACCOUNT_SID ou TWILIO_AUTH_TOKEN manquant");
  }

  const url = `${recordingUrl}.mp3`;
  const response = await axios.get(url, {
    responseType: "arraybuffer",
    headers: {
      Authorization:
        "Basic " + Buffer.from(`${accountSid}:${authToken}`).toString("base64"),
    },
    validateStatus: () => true,
  });

  if (response.status >= 400) {
    throw new Error(`Erreur Twilio audio: ${response.status}`);
  }

  return Buffer.from(response.data);
}

// ======================================================
// ROOT / HEALTH
// ======================================================
app.get("/", (_req, res) => {
  res.json({
    ok: true,
    service: "Swixim unified server",
    features: [
      "twilio_voice",
      "google_calendar",
      "apimo",
      "brochure_request",
      "whatsapp_prefilled_link",
      "voice_message",
      "spoken_message",
      "admin",
      "weather_comment",
    ],
  });
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    env: NODE_ENV,
    timezone: TZ,
    sessions: sessions.size,
    apimo: {
      providerId: APIMO_PROVIDER_ID,
      agencyId: APIMO_AGENCY_ID,
      cacheCount: propertiesCache.data.length,
      cacheFresh: isApimoCacheFresh(),
      cacheFetchedAt: propertiesCache.fetchedAt
        ? new Date(propertiesCache.fetchedAt).toISOString()
        : null,
    },
    weather: {
      enabled: Boolean(OPENWEATHER_API_KEY),
      cacheSize: weatherCache.size,
    },
  });
});

// ======================================================
// APIMO API
// ======================================================
app.get("/properties/raw", async (_req, res, next) => {
  try {
    const properties = await fetchProperties();
    res.json(properties);
  } catch (error) {
    next(error);
  }
});

app.get("/properties", async (_req, res, next) => {
  try {
    const properties = await fetchProperties();
    res.json({
      count: properties.length,
      items: properties.map(sanitizeProperty),
    });
  } catch (error) {
    next(error);
  }
});

app.post("/properties/refresh", async (_req, res, next) => {
  try {
    const properties = await fetchProperties({ forceRefresh: true });
    res.json({
      ok: true,
      count: properties.length,
      refreshedAt: new Date(propertiesCache.fetchedAt).toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

app.get("/property/:id", async (req, res, next) => {
  try {
    if (!isSafeId(req.params.id)) {
      return res.status(400).json({ error: "Identifiant invalide" });
    }

    const properties = await fetchProperties();
    const property = findPropertyById(properties, req.params.id);

    if (!property) {
      return res.status(404).json({ error: "Bien introuvable" });
    }

    res.json(buildPropertyPayload(property));
  } catch (error) {
    next(error);
  }
});

app.get("/property/:id/whatsapp-link", async (req, res, next) => {
  try {
    if (!isSafeId(req.params.id)) {
      return res.status(400).json({ error: "Identifiant invalide" });
    }

    const phone = normalizeFrenchPhone(req.query.phone || "");
    if (!phone) {
      return res.status(400).json({ error: "Numéro invalide" });
    }

    const properties = await fetchProperties();
    const property = findPropertyById(properties, req.params.id);

    if (!property) {
      return res.status(404).json({ error: "Bien introuvable" });
    }

    const payload = buildPropertyPayload(property);
    const whatsappPrefilledLink = generateWhatsAppPrefilledLink(
      phone,
      payload.message
    );

    res.json({
      property: payload.property,
      whatsapp_prefilled_link: whatsappPrefilledLink,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/property/:id/open-whatsapp", async (req, res, next) => {
  try {
    if (!isSafeId(req.params.id)) {
      return res.status(400).send("Identifiant invalide");
    }

    const phone = normalizeFrenchPhone(req.query.phone || "");
    if (!phone) {
      return res.status(400).send("Numéro invalide");
    }

    const properties = await fetchProperties();
    const property = findPropertyById(properties, req.params.id);

    if (!property) {
      return res.status(404).send("Bien introuvable");
    }

    const payload = buildPropertyPayload(property);
    const whatsappPrefilledLink = generateWhatsAppPrefilledLink(
      phone,
      payload.message
    );

    return res.redirect(whatsappPrefilledLink);
  } catch (error) {
    next(error);
  }
});

app.get("/properties/latest", async (_req, res, next) => {
  try {
    const properties = await fetchProperties();
    const latest = findLatestProperty(properties);

    if (!latest) {
      return res.status(404).json({ error: "Aucun bien trouvé" });
    }

    res.json(buildPropertyPayload(latest));
  } catch (error) {
    next(error);
  }
});

// ======================================================
// ADMIN
// ======================================================
app.get("/admin/json", async (req, res, next) => {
  try {
    const auth = checkBasicAuth(req);
    if (!auth.ok) {
      res.set("WWW-Authenticate", 'Basic realm="Admin"');
      return res.status(auth.status).send(auth.message);
    }

    const items = await readContacts();
    res.json({
      count: items.length,
      items,
    });
  } catch (error) {
    next(error);
  }
});
});

app.get("/admin", async (req, res, next) => {
  try {
    const auth = checkBasicAuth(req);
    if (!auth.ok) {
      res.set("WWW-Authenticate", 'Basic realm="Admin"');
      return res.status(auth.status).send(auth.message);
    }

    res.type("html").send(renderAdminPage(await readContacts()));
  } catch (error) {
    next(error);
  }
});

app.get("/admin/audio/:id", async (req, res, next) => {
  try {
    const auth = checkBasicAuth(req);
    if (!auth.ok) {
      res.set("WWW-Authenticate", 'Basic realm="Admin"');
      return res.status(auth.status).send(auth.message);
    }

    const id = String(req.params?.id || "");
    const item = (await readContacts()).find((entry) => entry.id === id);

    if (!item) return res.status(404).send("Enregistrement introuvable");
    if (!item.recordingUrl) return res.status(404).send("Aucun audio disponible");

    const audioBuffer = await fetchTwilioRecordingMp3(item.recordingUrl);
    res.set("Content-Type", "audio/mpeg");
    res.set("Content-Length", String(audioBuffer.length));
    res.set("Cache-Control", "no-store");
    res.send(audioBuffer);
  } catch (error) {
    console.error("Erreur lecture audio Twilio", error);
    next(error);
  }
});

app.post("/admin/mark-read", async (req, res, next) => {
  try {
    const auth = checkBasicAuth(req);
    if (!auth.ok) {
      res.set("WWW-Authenticate", 'Basic realm="Admin"');
      return res.status(auth.status).send(auth.message);
    }

    const id = String(req.body?.id || "");
    if (id) {
      await updateContactEntry(id, (item) => ({
        ...item,
        isRead: !item.isRead,
        updatedAt: new Date().toISOString(),
      }));
    }

    res.redirect("/admin");
  } catch (error) {
    next(error);
  }
});

app.post("/admin/delete", async (req, res, next) => {
  try {
    const auth = checkBasicAuth(req);
    if (!auth.ok) {
      res.set("WWW-Authenticate", 'Basic realm="Admin"');
      return res.status(auth.status).send(auth.message);
    }

    const id = String(req.body?.id || "");
    if (id) await deleteContactEntry(id);

    res.redirect("/admin");
  } catch (error) {
    next(error);
  }
});

// ======================================================
// TWILIO VOICE ENTRY
// ======================================================
app.post(
  "/",
  twilioOnly((_req, res) => {
    const response = vr();
    response.redirect({ method: "POST" }, "/voice");
    res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/voice",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    getSession(callSid);

    businessLog("CALL_STARTED", {
      callSid,
      startedAt: new Date().toISOString(),
    });

    const response = vr();
    sayFr(
      response,
      "Bonjour et bienvenue chez Swixim. Je suis Léa, votre agent digitale."
    );
    response.pause({ length: 1 });
    response.redirect({ method: "POST" }, "/menu");

    res.type("text/xml").send(response.toString());
  })
);

// ======================================================
// MENU / INTENT
// ======================================================
app.post(
  "/menu",
  twilioOnly((_req, res) => {
    const response = vr();

    gatherSpeech(
      response,
      "/intent",
      "Je peux vous aider pour trois besoins. Prendre un rendez vous téléphonique, laisser un message au conseiller, ou recevoir une fiche descriptive du bien. Dites par exemple rendez vous, message, ou fiche descriptive.",
      "rendez vous telephonique, rendez vous, rdv, message au conseiller, fiche descriptive, descriptif, description"
    );

    response.redirect({ method: "POST" }, "/menu-retry");
    res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/menu-retry",
  twilioOnly((_req, res) => {
    const response = vr();

    gatherSpeech(
      response,
      "/intent",
      "Je n ai pas bien entendu. Dites rendez vous téléphonique, message au conseiller, ou fiche descriptive.",
      "rendez vous telephonique, rdv, message au conseiller, fiche descriptive"
    );

    response.redirect({ method: "POST" }, "/goodbye");
    res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/intent",
  twilioOnly(async (req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);

    const speechResult = String(req.body?.SpeechResult || "").trim();
    const text = normalize(speechResult);
    const response = vr();

    businessLog("INTENT_INPUT", {
      callSid,
      speechResult,
      normalizedText: text,
      confidence: req.body?.Confidence,
    });

    const intent = await detectIntent(text);

    if (intent === "fiche") {
      session.flow = "fiche";
      session.unclearCount = 0;

      gatherSpeech(
        response,
        "/fiche-city",
        "Très bien. Pour quelle ville souhaitez vous la fiche ?",
        "strasbourg, mulhouse, guemar, hoerdt, wolfisheim, cernay, erstein, bischheim"
      );
      response.redirect({ method: "POST" }, "/fiche-city-retry");
      return res.type("text/xml").send(response.toString());
    }

    if (intent === "message") {
      session.flow = "message";
      session.unclearCount = 0;

      gatherSpeech(
        response,
        "/msg-type",
        "D accord. Souhaitez vous laisser un message vocal, ou dicter un message ? Dites message vocal, ou message dicté.",
        "message vocal, vocal, message dicté, dicter"
      );
      response.redirect({ method: "POST" }, "/msg-type-retry");
      return res.type("text/xml").send(response.toString());
    }

    if (intent === "rdv") {
      session.flow = "rdv";
      session.unclearCount = 0;

      gatherSpeech(
        response,
        "/rdv-name",
        "Très bien. Quel est votre nom et prénom ?",
        "nom, prénom"
      );
      response.redirect({ method: "POST" }, "/rdv-name-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.unclearCount += 1;

    if (session.unclearCount >= 2) {
      hangupWithMessage(
        response,
        "Je n ai pas bien compris votre besoin. Merci de rappeler. Au revoir."
      );
      resetSession(callSid);
      return res.type("text/xml").send(response.toString());
    }

    gatherSpeech(
      response,
      "/intent",
      "Je n ai pas compris. Dites rendez vous téléphonique, message au conseiller, ou fiche descriptive.",
      "rendez vous telephonique, rdv, message au conseiller, fiche descriptive"
    );
    response.redirect({ method: "POST" }, "/goodbye");
    return res.type("text/xml").send(response.toString());
  })
);

// ======================================================
// FICHE DESCRIPTIVE
// ======================================================
app.post(
  "/fiche-city",
  twilioOnly(async (req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!raw) {
      gatherSpeech(
        response,
        "/fiche-city",
        "Je n ai pas bien entendu la ville. Répétez la ville souhaitée."
      );
      response.redirect({ method: "POST" }, "/fiche-city-retry");
      return res.type("text/xml").send(response.toString());
    }

    const city = await normalizeCity(raw);
    session.data.city = city;

    businessLog("BROCHURE_CITY_PARSED", { callSid, raw, city });

    gatherSpeech(
      response,
      "/fiche-ref",
      `Très bien. Pour ${city}, dites maintenant la référence du bien. Vous pouvez dire les 4 premiers chiffres, ou toute la référence.`,
      "zero, un, deux, trois, quatre, cinq, six, sept, huit, neuf"
    );
    response.redirect({ method: "POST" }, "/fiche-ref-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/fiche-city-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(
      response,
      "Je n ai pas reçu de ville valide. Merci de votre appel. Au revoir."
    );
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/fiche-ref",
  twilioOnly(async (req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    const parsed = await parseRefDigits(raw);

    if (!parsed.ok) {
      gatherSpeech(
        response,
        "/fiche-ref",
        "Je n ai pas bien compris la référence. Répétez les chiffres de la référence."
      );
      response.redirect({ method: "POST" }, "/fiche-ref-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.refDigits = parsed.digits;

    businessLog("BROCHURE_REF_PARSED", {
      callSid,
      raw,
      refDigits: parsed.digits,
    });

    gatherSpeech(
      response,
      "/fiche-phone",
      `Très bien. J ai noté la ville ${session.data.city} et la référence ${spellDigitsForSpeech(parsed.digits)}. Quel numéro de téléphone souhaitez vous utiliser pour recevoir la fiche sur WhatsApp ?`
    );
    response.redirect({ method: "POST" }, "/fiche-phone-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/fiche-ref-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(
      response,
      "Je n ai pas reçu de référence valide. Merci de votre appel. Au revoir."
    );
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/fiche-phone",
  twilioOnly(async (req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!looksLikePhone(raw)) {
      gatherSpeech(
        response,
        "/fiche-phone",
        "Je n ai pas bien compris le numéro. Répétez uniquement votre numéro de téléphone."
      );
      response.redirect({ method: "POST" }, "/fiche-phone-retry");
      return res.type("text/xml").send(response.toString());
    }

    const phone = normalizeFrenchPhone(raw);
    session.data.phone = phone;

    const entry = {
      id: crypto.randomUUID(),
      type: "brochure_request",
      callSid,
      callerName: "",
      callerPhone: phone,
      message: "",
      recordingSid: "",
      recordingUrl: "",
      recordingStatus: "",
      recordingDuration: "",
      brochure: {
        city: session.data.city,
        ref: session.data.refDigits,
        link: "",
        whatsappPrefilledLink: "",
        title: "",
      },
      appointment: {
        date: "",
        time: "",
        calendarEventId: "",
        calendarHtmlLink: "",
      },
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      isRead: false,
      assignedTo: "conseiller",
      source: "twilio_voice",
    };

    saveContactEntry(entry);

    try {
      sayFr(response, "Parfait, je retrouve la fiche. Un instant.");

      const weatherComment = await getWeatherComment(session.data.city);
      if (weatherComment) {
        response.pause({ length: 1 });
        sayFr(response, weatherComment);
      }

      const properties = await fetchProperties();
      const property = findPropertyFromApimoList({
        properties,
        city: session.data.city,
        refDigits: session.data.refDigits,
      });

      if (!property) {
        throw new Error(
          `Aucun bien trouvé pour ${session.data.city} / ${session.data.refDigits}`
        );
      }

      const payload = buildPropertyPayload(property);
      const whatsappLink = generateWhatsAppPrefilledLink(phone, payload.message);

      updateContactEntry(entry.id, (item) => ({
        ...item,
        brochure: {
          ...item.brochure,
          link: payload.property.url || SWIXIM_AGENCY_URL,
          whatsappPrefilledLink: whatsappLink,
          title: payload.property.title,
        },
        updatedAt: new Date().toISOString(),
      }));

      businessLog("BROCHURE_READY", {
        callSid,
        city: session.data.city,
        refDigits: session.data.refDigits,
        phone,
        property: payload.property,
        whatsappLink,
      });

      sayFr(
        response,
        "C est bon. Votre demande de fiche a été enregistrée. Le lien WhatsApp est prêt pour ce numéro. Merci de votre appel. Au revoir."
      );
      response.hangup();

      resetSession(callSid);
      return res.type("text/xml").send(response.toString());
    } catch (error) {
      console.error("Erreur brochure", error);

      updateContactEntry(entry.id, (item) => ({
        ...item,
        updatedAt: new Date().toISOString(),
        message: `Erreur fiche: ${error?.message || "Erreur inconnue"}`,
      }));

      sayFr(
        response,
        "Votre demande de fiche descriptive a bien été enregistrée, mais je n ai pas pu finaliser la recherche automatiquement. Un conseiller vous recontactera rapidement. Merci de votre appel. Au revoir."
      );
      response.hangup();

      resetSession(callSid);
      return res.type("text/xml").send(response.toString());
    }
  })
);

app.post(
  "/fiche-phone-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(
      response,
      "Je n ai pas reçu de numéro. Merci de votre appel. Au revoir."
    );
    return res.type("text/xml").send(response.toString());
  })
);

// ======================================================
// RDV TELEPHONIQUE
// ======================================================
app.post(
  "/rdv-name",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!raw) {
      gatherSpeech(
        response,
        "/rdv-name",
        "Je n ai pas bien entendu. Répétez votre nom et prénom."
      );
      response.redirect({ method: "POST" }, "/rdv-name-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.name = raw;

    gatherSpeech(response, "/rdv-phone", "Merci. Quel est votre numéro de téléphone ?");
    response.redirect({ method: "POST" }, "/rdv-phone-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/rdv-name-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre nom. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/rdv-phone",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!looksLikePhone(raw)) {
      gatherSpeech(
        response,
        "/rdv-phone",
        "Je n ai pas bien compris le numéro. Répétez uniquement votre numéro de téléphone."
      );
      response.redirect({ method: "POST" }, "/rdv-phone-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.phone = normalizeFrenchPhone(raw);

    gatherSpeech(
      response,
      "/rdv-date",
      "Très bien. Quelle date souhaitez vous ? Dites par exemple vingt huit mars deux mille vingt six, ou demain."
    );
    response.redirect({ method: "POST" }, "/rdv-date-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/rdv-phone-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre numéro. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/rdv-date",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    const parsedDate = parseFrenchDate(raw);

    if (!parsedDate.ok) {
      gatherSpeech(
        response,
        "/rdv-date",
        "Je n ai pas bien compris la date. Répétez par exemple vingt huit mars deux mille vingt six, ou demain."
      );
      response.redirect({ method: "POST" }, "/rdv-date-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.dateText = parsedDate.isoDate;

    gatherSpeech(
      response,
      "/rdv-time",
      "Merci. Quelle heure souhaitez vous ? Dites par exemple quatorze heures, quatorze heures trente, ou quatorze trente."
    );
    response.redirect({ method: "POST" }, "/rdv-time-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/rdv-date-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu de date valide. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/rdv-time",
  twilioOnly(async (req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    const parsedTime = parseFrenchTime(raw);

    if (!parsedTime.ok) {
      gatherSpeech(
        response,
        "/rdv-time",
        "Je n ai pas bien compris l heure. Répétez par exemple quatorze heures, quatorze heures trente, ou quatorze trente."
      );
      response.redirect({ method: "POST" }, "/rdv-time-retry");
      return res.type("text/xml").send(response.toString());
    }

    if (!isFutureSlot(session.data.dateText, parsedTime.hhmm)) {
      gatherSpeech(
        response,
        "/rdv-date",
        "La date ou l heure demandée est déjà passée. Merci de redonner une nouvelle date de rendez vous."
      );
      return res.type("text/xml").send(response.toString());
    }

    session.data.timeText = parsedTime.hhmm;

    try {
      const available = await isCalendarSlotAvailable({
        isoDate: session.data.dateText,
        hhmm: session.data.timeText,
        durationMinutes: APPOINTMENT_DURATION_MINUTES,
      });

      if (!available) {
        const alternatives = await findAlternativeSlotsForDay({
          isoDate: session.data.dateText,
          durationMinutes: APPOINTMENT_DURATION_MINUTES,
          excludeHhmm: parsedTime.hhmm,
          maxSuggestions: 3,
        });

        if (alternatives.length > 0) {
          gatherSpeech(
            response,
            "/rdv-time",
            `Ce créneau est déjà pris. Je peux vous proposer ${alternatives.join(", ")}. Merci de dire l heure choisie pour cette même date.`
          );
          response.redirect({ method: "POST" }, "/rdv-time-retry");
          return res.type("text/xml").send(response.toString());
        }

        gatherSpeech(
          response,
          "/rdv-time",
          "Ce créneau est déjà pris. Je n ai pas trouvé d autre disponibilité proche pour cette date. Merci de proposer une autre heure."
        );
        response.redirect({ method: "POST" }, "/rdv-time-retry");
        return res.type("text/xml").send(response.toString());
      }

      const event = await createCalendarEvent({
        name: session.data.name,
        phone: session.data.phone,
        isoDate: session.data.dateText,
        hhmm: session.data.timeText,
      });

      session.data.calendarEventId = event.id || "";
      session.data.calendarHtmlLink = event.htmlLink || "";

      saveContactEntry({
        id: crypto.randomUUID(),
        type: "appointment_request",
        callSid,
        callerName: session.data.name,
        callerPhone: session.data.phone,
        message: "",
        recordingSid: "",
        recordingUrl: "",
        recordingStatus: "",
        recordingDuration: "",
        brochure: {
          city: "",
          ref: "",
          link: "",
          whatsappPrefilledLink: "",
          title: "",
        },
        appointment: {
          date: session.data.dateText,
          time: session.data.timeText,
          calendarEventId: session.data.calendarEventId,
          calendarHtmlLink: session.data.calendarHtmlLink,
        },
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        isRead: false,
        assignedTo: "conseiller",
        source: "twilio_voice",
      });

      hangupWithMessage(
        response,
        "Parfait. Votre rendez vous téléphonique a bien été enregistré dans l agenda. Merci de votre appel. Au revoir."
      );
      resetSession(callSid);
      return res.type("text/xml").send(response.toString());
    } catch (error) {
      console.error("Erreur création Google Calendar", error);

      hangupWithMessage(
        response,
        "Votre demande a bien été prise en compte, mais je rencontre un problème pour l enregistrer dans l agenda. Un conseiller vous recontactera rapidement. Merci de votre appel. Au revoir."
      );
      resetSession(callSid);
      return res.type("text/xml").send(response.toString());
    }
  })
);

app.post(
  "/rdv-time-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu d heure valide. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

// ======================================================
// MESSAGE CHOICE
// ======================================================
app.post(
  "/msg-type",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);

    const raw = String(req.body?.SpeechResult || "").trim();
    const text = normalize(raw);
    const response = vr();

    if (text.includes("vocal")) {
      session.data.messageMode = "voice";

      gatherSpeech(response, "/msg-voice-name", "Très bien. Quel est votre nom et prénom ?");
      response.redirect({ method: "POST" }, "/msg-voice-name-retry");
      return res.type("text/xml").send(response.toString());
    }

    if (
      text.includes("dicte") ||
      text.includes("dicter") ||
      text.includes("dictee") ||
      text.includes("message")
    ) {
      session.data.messageMode = "spoken";

      gatherSpeech(response, "/msg-name", "D accord. Quel est votre nom et prénom ?");
      response.redirect({ method: "POST" }, "/msg-name-retry");
      return res.type("text/xml").send(response.toString());
    }

    gatherSpeech(
      response,
      "/msg-type",
      "Je n ai pas bien compris. Dites message vocal, ou message dicté.",
      "message vocal, vocal, message dicté, dicter"
    );
    response.redirect({ method: "POST" }, "/msg-type-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-type-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre choix. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

// ======================================================
// MESSAGE VOCAL
// ======================================================
app.post(
  "/msg-voice-name",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!raw) {
      gatherSpeech(
        response,
        "/msg-voice-name",
        "Je n ai pas bien entendu. Répétez votre nom et prénom."
      );
      response.redirect({ method: "POST" }, "/msg-voice-name-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.name = raw;

    gatherSpeech(response, "/msg-voice-phone", "Merci. Quel est votre numéro de téléphone ?");
    response.redirect({ method: "POST" }, "/msg-voice-phone-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-voice-name-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre nom. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-voice-phone",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!looksLikePhone(raw)) {
      gatherSpeech(
        response,
        "/msg-voice-phone",
        "Je n ai pas bien compris le numéro. Répétez uniquement votre numéro de téléphone."
      );
      response.redirect({ method: "POST" }, "/msg-voice-phone-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.phone = normalizeFrenchPhone(raw);

    const pendingId = crypto.randomUUID();
    session.data.pendingVoiceMessageId = pendingId;

    saveContactEntry({
      id: pendingId,
      type: "voice_message",
      callSid,
      callerName: session.data.name,
      callerPhone: session.data.phone,
      message: "",
      recordingSid: "",
      recordingUrl: "",
      recordingStatus: "pending",
      recordingDuration: "",
      brochure: {
        city: "",
        ref: "",
        link: "",
        whatsappPrefilledLink: "",
        title: "",
      },
      appointment: {
        date: "",
        time: "",
        calendarEventId: "",
        calendarHtmlLink: "",
      },
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      isRead: false,
      assignedTo: "conseiller",
      source: "twilio_voice",
    });

    sayFr(
      response,
      "Très bien. Après le bip, vous pouvez laisser votre message vocal. Quand vous avez terminé, raccrochez ou appuyez sur la touche dièse."
    );

    response.record({
      action: "/msg-voice-done",
      method: "POST",
      maxLength: 120,
      finishOnKey: "#",
      playBeep: true,
      trim: "trim-silence",
      timeout: 5,
      recordingStatusCallback: "/recording-status",
      recordingStatusCallbackMethod: "POST",
    });

    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-voice-phone-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre numéro. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-voice-done",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const response = vr();

    businessLog("VOICE_MESSAGE_FINISHED", {
      callSid,
      recordingSid: req.body?.RecordingSid || null,
      recordingUrl: req.body?.RecordingUrl || null,
      recordingDuration: req.body?.RecordingDuration || null,
      finishedAt: new Date().toISOString(),
    });

    hangupWithMessage(
      response,
      "Parfait. Votre message vocal a bien été enregistré. Merci de votre appel. Au revoir."
    );

    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/recording-status",
  twilioOnly((req, res) => {
    const callSid = req.body?.CallSid || "unknown";
    const session = sessions.get(callSid);
    const recordingSid = String(req.body?.RecordingSid || "");
    const recordingUrl = String(req.body?.RecordingUrl || "");
    const pendingId = session?.data?.pendingVoiceMessageId;

    if (pendingId) {
      updateContactEntry(pendingId, (item) => ({
        ...item,
        recordingSid,
        recordingUrl,
        recordingStatus: req.body?.RecordingStatus || item.recordingStatus || "",
        recordingDuration: req.body?.RecordingDuration || item.recordingDuration || "",
        updatedAt: new Date().toISOString(),
      }));
    } else {
      upsertContactEntryByPredicate(
        (item) => item.recordingSid && item.recordingSid === recordingSid,
        (existing) => ({
          id: existing?.id || crypto.randomUUID(),
          type: "voice_message",
          callSid,
          callerName: existing?.callerName || "",
          callerPhone: existing?.callerPhone || "",
          message: "",
          recordingSid,
          recordingUrl,
          recordingStatus:
            req.body?.RecordingStatus || existing?.recordingStatus || "",
          recordingDuration:
            req.body?.RecordingDuration || existing?.recordingDuration || "",
          brochure: existing?.brochure || {
            city: "",
            ref: "",
            link: "",
            whatsappPrefilledLink: "",
            title: "",
          },
          appointment: existing?.appointment || {
            date: "",
            time: "",
            calendarEventId: "",
            calendarHtmlLink: "",
          },
          createdAt: existing?.createdAt || new Date().toISOString(),
          updatedAt: new Date().toISOString(),
          isRead: existing?.isRead || false,
          assignedTo: "conseiller",
          source: "twilio_voice",
        })
      );
    }

    resetSession(callSid);
    return res.status(200).send("ok");
  })
);

// ======================================================
// MESSAGE DICTE
// ======================================================
app.post(
  "/msg-name",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!raw) {
      gatherSpeech(
        response,
        "/msg-name",
        "Je n ai pas bien entendu. Répétez votre nom et prénom."
      );
      response.redirect({ method: "POST" }, "/msg-name-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.name = raw;

    gatherSpeech(response, "/msg-phone", "Quel est votre numéro de téléphone ?");
    response.redirect({ method: "POST" }, "/msg-phone-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-name-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre nom. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-phone",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!looksLikePhone(raw)) {
      gatherSpeech(
        response,
        "/msg-phone",
        "Je n ai pas bien compris le numéro. Répétez uniquement votre numéro de téléphone."
      );
      response.redirect({ method: "POST" }, "/msg-phone-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.phone = normalizeFrenchPhone(raw);

    gatherSpeech(response, "/msg-body", "Quel message souhaitez vous laisser ?");
    response.redirect({ method: "POST" }, "/msg-body-retry");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-phone-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre numéro. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-body",
  twilioOnly((req, res) => {
    const callSid = getCallSid(req);
    const session = getSession(callSid);
    const raw = String(req.body?.SpeechResult || "").trim();
    const response = vr();

    if (!raw) {
      gatherSpeech(
        response,
        "/msg-body",
        "Je n ai pas bien entendu. Répétez votre message, s il vous plaît."
      );
      response.redirect({ method: "POST" }, "/msg-body-retry");
      return res.type("text/xml").send(response.toString());
    }

    session.data.message = raw;

    saveContactEntry({
      id: crypto.randomUUID(),
      type: "spoken_message",
      callSid,
      callerName: session.data.name,
      callerPhone: session.data.phone,
      message: session.data.message,
      recordingSid: "",
      recordingUrl: "",
      recordingStatus: "",
      recordingDuration: "",
      brochure: {
        city: "",
        ref: "",
        link: "",
        whatsappPrefilledLink: "",
        title: "",
      },
      appointment: {
        date: "",
        time: "",
        calendarEventId: "",
        calendarHtmlLink: "",
      },
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      isRead: false,
      assignedTo: "conseiller",
      source: "twilio_voice",
    });

    hangupWithMessage(
      response,
      "Parfait. Votre message pour le conseiller a bien été enregistré. Merci de votre appel. Au revoir."
    );
    resetSession(callSid);
    return res.type("text/xml").send(response.toString());
  })
);

app.post(
  "/msg-body-retry",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Je n ai pas reçu votre message. Merci de votre appel. Au revoir.");
    return res.type("text/xml").send(response.toString());
  })
);

// ======================================================
// GOODBYE
// ======================================================
app.post(
  "/goodbye",
  twilioOnly((_req, res) => {
    const response = vr();
    hangupWithMessage(response, "Merci de votre appel. Au revoir.");
    res.type("text/xml").send(response.toString());
  })
);

// ======================================================
// ERROR HANDLING
// ======================================================
app.use((req, res) => {
  res.status(404).json({
    error: "Route introuvable",
  });
});

app.use((error, _req, res, _next) => {
  console.error("UNHANDLED_ERROR", error);
  res.status(500).json({
    error: "Internal Server Error",
    message:
      NODE_ENV === "development"
        ? String(error.message || error)
        : "Une erreur est survenue",
  });
});

process.on("unhandledRejection", (reason) => {
  console.error("UNHANDLED_REJECTION:", reason);
});

process.on("uncaughtException", (error) => {
  console.error("UNCAUGHT_EXCEPTION:", error);
});

// ======================================================
// START
// ======================================================
requireEnvInProduction();
checkEnvAtStartup();

console.log("APIMO token loaded:", !!APIMO_API_TOKEN);
console.log("APIMO provider:", APIMO_PROVIDER_ID);
console.log("APIMO agency:", APIMO_AGENCY_ID);
console.log("APIMO base url:", APIMO_BASE_URL);
console.log("OPENWEATHER enabled:", Boolean(OPENWEATHER_API_KEY));

async function startServer() {
  await initDatabase();

  app.listen(PORT, "0.0.0.0", () => {
    businessLog("SERVER_STARTED", {
      port: PORT,
      env: NODE_ENV,
      timezone: TZ,
      appBaseUrl: APP_BASE_URL,
      apimoCacheTtlMs: APIMO_CACHE_TTL_MS,
      weatherCacheTtlMs: WEATHER_CACHE_TTL_MS,
      appointmentDurationMinutes: APPOINTMENT_DURATION_MINUTES,
      twilioValidateSignature: TWILIO_VALIDATE_SIGNATURE,
      storage: pgPool ? "postgres" : "json",
    });
  });
}

startServer().catch((error) => {
  console.error("STARTUP_ERROR", error);
  process.exit(1);
});
});
>>>>>>> 5e8a3bf (add postgres database + upgrade storage)
