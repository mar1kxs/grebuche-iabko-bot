require("dotenv").config();
const { Telegraf, Markup } = require("telegraf");
const Airtable = require("airtable");
const axios = require("axios");

const bot = new Telegraf(process.env.BOT_TOKEN);

const ADMIN_IDS = process.env.ADMIN_IDS
  ? process.env.ADMIN_IDS.split(",").map((id) => id.trim())
  : [];

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID,
);

// ================== CONFIG ==================
const TABLE_SHIFTS = "Нарахування";
const TABLE_ACQUIRING = "Еквайринг";

const TABLE_OUTLETS = "Заклади";
const TABLE_POSITIONS = "Посади";
const TABLE_EMPLOYEES = "Працівники";

const TABLE_DEDUCTIONS = "Відрахування";
const FIELD_DEDUCTIONS_LINK = "Відрахування(Вибрати)";

// linked tables primary-name fields
const OUTLETS_NAME_FIELD = "Назва закладу";
const POSITIONS_NAME_FIELD = "Скорочена назва";

// employees
const EMP_TG_FIELD = "Telegram ID";

// shifts fields
const FIELD_DATE = "Дата";
const FIELD_OUTLET = "Заклад";
const FIELD_POSITION = "Посада";
const FIELD_EMPLOYEE = "Працівник";
const FIELD_REVENUE = "Виручка";
const FIELD_ENTRANCE_REVENUE = "Виручка Вхід";

// paytype on SHIFT
const SHIFT_PAYTYPE_FIELD = "ЗП для бота";

// ✅ правила
const PAYTYPE_FOR_TOTAL = new Set(["%", "Ставка + %"]);
const PAYTYPE_FOR_ENTRANCE = "Ставка + % вхід";

// acquiring field
const FIELD_ACQ_VALUE = "Еквайринг Poster (API)";

// Poster tokens
const POSTER_TOKENS = {
  Староєврейська: process.env.POSTER_SE,
  Дорошенка: process.env.POSTER_DO,
  Джерельна: process.env.POSTER_DZH,
};

// Poster accounts
const POSTER_ACCOUNTS = {
  Староєврейська: "grebu4e",
  Дорошенка: "grebuche-iabko-kriva-lipa",
  Джерельна: "rayon-gy",
};

const EMPLOYEE_NAME_FIELD = "Ім’я";
const EMPLOYEE_POSITIONS_FIELD = "Посади";
const EMPLOYEE_LIST_PAGE_SIZE = 50;

// Entrance category (ONLY for Джерельна)
const ENTRANCE_OUTLET_NAME = "Джерельна";
const ENTRANCE_CATEGORY_NAME = "БРАСЛЕТИ - ВХОДИ";
const ENTRANCE_CATEGORY_ID = "18";

// ================== LISTS ==================
const OUTLETS = ["Староєврейська", "Дорошенка", "Джерельна"];
const POSITIONS = [
  "СТ Бармен",
  "ПСТ Бармен",
  "МЛ Бармен",
  "СТ Охоронець",
  "МЛ Охоронець",
  "СТ МС",
  "ПСТ МС",
  "МЛ МС",
  "СТ DJ",
  "МЛ DJ",
  "PJ",
];

const isAdmin = (ctx) => ADMIN_IDS.includes(String(ctx.from.id));
const userState = new Map();

// ================== HELPERS ==================
function parseISODate(s) {
  const d = new Date(`${s}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) throw new Error(`Bad date: ${s}`);
  return d;
}

function assertISODate(s) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(s))) throw new Error("bad date");
  parseISODate(s);
}

async function getEmployeeAllowedPositions(ctxOrTgId) {
  const tgId = typeof ctxOrTgId === "object" ? ctxOrTgId.from.id : ctxOrTgId;
  const empRecId = await getEmployeeRecIdByTgId(tgId);
  const empRec = await base(TABLE_EMPLOYEES).find(empRecId);

  const posLinks = empRec?.fields?.[EMPLOYEE_POSITIONS_FIELD] || [];
  if (!Array.isArray(posLinks) || posLinks.length === 0) return [];

  const out = [];
  for (const posRecId of posLinks) {
    const name = await getPositionNameById(posRecId);
    out.push({ id: posRecId, name });
  }

  out.sort((a, b) => a.name.localeCompare(b.name, "uk"));
  return out;
}

function tgIdForAirtable(value) {
  const s = String(value || "").trim();
  if (!/^\d{5,20}$/.test(s)) throw new Error("Bad TG id format");

  const n = Number(s);

  if (!Number.isSafeInteger(n)) {
    throw new Error("TG id is not a safe integer for Airtable Number field");
  }

  return n;
}

function buildAllowedPositionsKeyboard(allowed) {
  const rows = allowed.map((p, i) => [
    Markup.button.callback(p.name, `EMP_POS_A_${i}`),
  ]);
  rows.push([Markup.button.callback("❌ Скасувати", "EMP_CANCEL")]);
  return Markup.inlineKeyboard(rows);
}

function buildPositionsKeyboard(selectedIdxSet) {
  const cols = 2;
  const rows = [];
  for (let i = 0; i < POSITIONS.length; i += cols) {
    const row = [];
    for (let j = 0; j < cols; j++) {
      const idx = i + j;
      if (idx >= POSITIONS.length) break;

      const checked = selectedIdxSet.has(idx);
      const label = `${checked ? "✅ " : ""}${POSITIONS[idx]}`;
      row.push(Markup.button.callback(label, `EMP_POS_T_${idx}`));
    }
    rows.push(row);
  }

  rows.push([
    Markup.button.callback("✅ Готово", "EMP_POS_DONE"),
    Markup.button.callback("🧹 Очистити", "EMP_POS_CLEAR"),
  ]);
  rows.push([Markup.button.callback("❌ Скасувати", "ADM_CANCEL")]);

  return Markup.inlineKeyboard(rows);
}

function extractTelegramIdFromCtxOrText(ctx, text) {
  // 1) forward_from
  const fwdId = ctx.message?.forward_from?.id;
  if (fwdId) return { ok: true, id: String(fwdId) };

  const s = String(text || "").trim();

  // 2) чистое число
  if (/^\d{5,20}$/.test(s)) return { ok: true, id: s };

  // 3) если прислали что-то вроде "id: 123456"
  const m = s.match(/(\d{5,20})/);
  if (m) return { ok: true, id: m[1] };

  // 4) @username — не можем превратить в ID без интеракции
  if (/^@[\w\d_]{3,}$/.test(s)) {
    return {
      ok: false,
      error:
        "Я не можу отримати Telegram ID лише з @username. Перешли повідомлення від людини або надішли її numeric Telegram ID.",
    };
  }

  return {
    ok: false,
    error:
      "Надішли numeric Telegram ID або перешли повідомлення від людини (я сам візьму ID).",
  };
}

function normalizeEmployeeDate(input) {
  const s = String(input).trim();

  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    assertISODate(s);
    return s;
  }

  if (/^\d{2}-\d{2}$/.test(s)) {
    const year = new Date().getFullYear();
    const iso = `${year}-${s}`;
    assertISODate(iso);
    return iso;
  }

  throw new Error("bad date");
}

function formatISODate(d) {
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function datesBetween(startISO, endISO) {
  const start = parseISODate(startISO);
  const end = parseISODate(endISO);
  if (end < start) throw new Error("endDate is earlier than startDate");

  const out = [];
  for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
    out.push(formatISODate(d));
  }
  return out;
}

function seededRand(seed) {
  let x = seed >>> 0;
  return () => {
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    return (x >>> 0) / 4294967296;
  };
}

function hashStrToSeed(str) {
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

function round2(n) {
  return Math.round(n * 100) / 100;
}

async function createInBatches(tableName, records, batchSize = 10) {
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    await base(tableName).create(batch);
  }
}

async function updateInBatches(tableName, records, batchSize = 10) {
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    await base(tableName).update(batch);
  }
}

function toISODateOnly(val) {
  if (!val) return null;
  if (val instanceof Date) return val.toISOString().slice(0, 10);

  const s = String(val);
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  if (/^\d{4}-\d{2}-\d{2}T/.test(s)) return s.slice(0, 10);

  const d = new Date(s);
  if (!Number.isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  return null;
}

function pickTextValue(v) {
  if (typeof v === "string") return v.trim();
  if (Array.isArray(v)) return String(v[0] || "").trim();
  if (v && typeof v === "object" && v.name) return String(v.name).trim();
  return "";
}

function normalizeMoneyToNumber(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 0;
}

function normText(s) {
  return String(s || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function isoToDDMMYYYY(iso) {
  const s = String(iso || "");
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  const [y, m, d] = s.split("-");
  return `${d}.${m}.${y}`;
}

function escapeAirtableStr(s) {
  return String(s).replace(/"/g, '\\"');
}

// ================== LINK RESOLVERS ==================
const linkIdCache = new Map();

async function getLinkedRecordIdByName(tableName, nameField, name) {
  const key = `${tableName}:${nameField}:${name}`;
  if (linkIdCache.has(key)) return linkIdCache.get(key);

  const escaped = String(name).replace(/"/g, '\\"');
  const formula = `{${nameField}} = "${escaped}"`;

  const records = await base(tableName)
    .select({ filterByFormula: formula, maxRecords: 1 })
    .firstPage();

  if (!records || records.length === 0) {
    throw new Error(
      `Не знайдено "${name}" у таблиці "${tableName}" по полю "${nameField}".`,
    );
  }

  const id = records[0].id;
  linkIdCache.set(key, id);
  return id;
}

async function linkOutlet(outletName) {
  const id = await getLinkedRecordIdByName(
    TABLE_OUTLETS,
    OUTLETS_NAME_FIELD,
    outletName,
  );
  return [id];
}

async function linkPosition(positionName) {
  const id = await getLinkedRecordIdByName(
    TABLE_POSITIONS,
    POSITIONS_NAME_FIELD,
    positionName,
  );
  return [id];
}

// ---- Employee by TG ID (linked) ----
const employeeCache = new Map();

async function getEmployeeRecIdByTgId(tgId) {
  const key = String(tgId);
  if (employeeCache.has(key)) return employeeCache.get(key);

  const n = tgIdForAirtable(key);
  const formula = `{${EMP_TG_FIELD}} = ${n}`;

  const records = await base(TABLE_EMPLOYEES)
    .select({ filterByFormula: formula, maxRecords: 1 })
    .firstPage();

  if (!records || records.length === 0) {
    throw new Error(
      `Твого профілю немає в "${TABLE_EMPLOYEES}". Попроси адміна додати тебе (поле "${EMP_TG_FIELD}" = ${key}).`,
    );
  }

  const id = records[0].id;
  employeeCache.set(key, id);
  return id;
}

async function linkEmployeeByTgId(tgId) {
  const id = await getEmployeeRecIdByTgId(tgId);
  return [id];
}

function buildAllowedPositionsKeyboardEdit(allowed) {
  const rows = allowed.map((p, i) => [
    Markup.button.callback(p.name, `EMP_EDIT_POS_A_${i}`),
  ]);
  rows.push([Markup.button.callback("❌ Скасувати", "EMP_CANCEL")]);
  return Markup.inlineKeyboard(rows);
}

// ================== NAME RESOLVERS FOR REPORT ==================
const recNameCache = new Map();

async function getRecordNameById(tableName, recId, nameField) {
  const key = `${tableName}:${recId}:${nameField}`;
  if (recNameCache.has(key)) return recNameCache.get(key);

  const rec = await base(tableName).find(recId);
  const name = pickTextValue(rec?.fields?.[nameField]) || recId;
  recNameCache.set(key, name);
  return name;
}

async function getOutletNameById(outletRecId) {
  return getRecordNameById(TABLE_OUTLETS, outletRecId, OUTLETS_NAME_FIELD);
}

async function getPositionNameById(positionRecId) {
  return getRecordNameById(
    TABLE_POSITIONS,
    positionRecId,
    POSITIONS_NAME_FIELD,
  );
}

// ================== POSTER API ==================
async function posterGetPaymentsByDay({ account, token, startDate, endDate }) {
  const url = `https://${account}.joinposter.com/api/dash.getPaymentsReport`;

  const { data } = await axios.get(url, {
    params: { token, date_from: startDate, date_to: endDate },
    timeout: 20000,
  });

  if (typeof data === "string") {
    throw new Error(`Poster returned non-JSON string for ${account}`);
  }
  if (data?.error) {
    throw new Error(
      `Poster error for ${account}: ${data.error.message || "Unknown error"}`,
    );
  }

  const days = data?.response?.days;
  if (!Array.isArray(days)) {
    throw new Error(`Unexpected Poster API format for ${account}`);
  }

  return days.map((day) => ({
    date: day.date || null,
    cardRevenue: normalizeMoneyToNumber(day.payed_card_sum || 0) / 100,
    totalRevenue:
      normalizeMoneyToNumber(
        day.payed_sum_sum ?? day.total_sum ?? day.sum ?? 0,
      ) / 100,
  }));
}

async function posterGetEntranceRevenueForOneDay({ account, token, dateISO }) {
  const url = `https://${account}.joinposter.com/api/dash.getCategoriesSales`;

  const { data } = await axios.get(url, {
    params: { token, date_from: dateISO, date_to: dateISO },
    timeout: 20000,
  });

  if (typeof data === "string") {
    throw new Error(
      `Poster returned non-JSON string for ${account} (categories)`,
    );
  }
  if (data?.error) {
    throw new Error(
      `Poster error for ${account} (categories): ${
        data.error.message || "Unknown error"
      }`,
    );
  }

  const rows = data?.response;
  if (!Array.isArray(rows)) {
    throw new Error(`Unexpected Poster categories format for ${account}`);
  }

  const targetId = String(ENTRANCE_CATEGORY_ID || "").trim();
  const targetName = normText(ENTRANCE_CATEGORY_NAME);

  let revenue = 0;

  for (const r of rows) {
    const id = String(r.category_id ?? "").trim();
    const name = normText(r.category_name);

    const matchById = targetId && id === targetId;
    const matchByName = targetName && name === targetName;

    if (matchById || matchByName) {
      revenue += normalizeMoneyToNumber(r.revenue || 0) / 100;
    }
  }

  console.log(`[ENTRANCE] ${account} ${dateISO} revenue=${revenue}`);
  return revenue;
}

// ================== FETCH POSTER DATA (demo/real) ==================
async function fetchPosterData(startDate, endDate) {
  const mode = (process.env.POSTER_MODE || "demo").toLowerCase();

  const outletIdByName = {};
  for (const outlet of OUTLETS) {
    const [id] = await linkOutlet(outlet);
    outletIdByName[outlet] = id;
  }

  if (mode === "demo") {
    const days = datesBetween(startDate, endDate);
    const acquiring = [];
    const accruals = [];

    for (const day of days) {
      for (const outlet of OUTLETS) {
        const rand = seededRand(hashStrToSeed(`${day}|${outlet}`));
        const totalRevenue = round2(1200 + rand() * 4800);
        const cardRevenue = round2(totalRevenue * (0.25 + rand() * 0.45));

        acquiring.push({
          date: day,
          outlet,
          outletId: outletIdByName[outlet],
          cardRevenue,
        });

        const a = {
          date: day,
          outlet,
          outletId: outletIdByName[outlet],
          totalRevenue,
        };

        if (outlet === ENTRANCE_OUTLET_NAME) {
          a.entranceRevenue = round2(totalRevenue * 0.08);
        }

        accruals.push(a);
      }
    }

    return { acquiring, accruals };
  }

  const acquiring = [];
  const accruals = [];
  const daysList = datesBetween(startDate, endDate);

  for (const outlet of OUTLETS) {
    const token = POSTER_TOKENS[outlet];
    const account = POSTER_ACCOUNTS[outlet];

    if (!token) throw new Error(`Нема Poster токена для закладу: ${outlet}`);
    if (!account) throw new Error(`Нема Poster account для закладу: ${outlet}`);

    const daysData = await posterGetPaymentsByDay({
      account,
      token,
      startDate,
      endDate,
    });

    const paymentsByDate = new Map();
    for (const d of daysData) {
      if (!d.date) continue;
      paymentsByDate.set(d.date, d);

      acquiring.push({
        date: d.date,
        outlet,
        outletId: outletIdByName[outlet],
        cardRevenue: d.cardRevenue,
      });
    }

    for (const dayISO of daysList) {
      const p = paymentsByDate.get(dayISO);
      if (!p) continue;

      const a = {
        date: dayISO,
        outlet,
        outletId: outletIdByName[outlet],
        totalRevenue: p.totalRevenue,
      };

      if (outlet === ENTRANCE_OUTLET_NAME) {
        const entrance = await posterGetEntranceRevenueForOneDay({
          account,
          token,
          dateISO: dayISO,
        });
        a.entranceRevenue = entrance;
      }

      accruals.push(a);
    }
  }

  return { acquiring, accruals };
}

// ================== SAVE ACQUIRING (ALWAYS CREATE) ==================
async function savePosterToAirtable(posterData) {
  const acquiringRecords = [];

  for (const item of posterData.acquiring) {
    const outletLink = item.outletId
      ? [item.outletId]
      : await linkOutlet(item.outlet);

    acquiringRecords.push({
      fields: {
        [FIELD_DATE]: item.date,
        [FIELD_OUTLET]: outletLink,
        [FIELD_ACQ_VALUE]: item.cardRevenue,
      },
    });
  }

  if (acquiringRecords.length) {
    await createInBatches(TABLE_ACQUIRING, acquiringRecords, 10);
  }

  return { created: acquiringRecords.length };
}

// ================== APPLY REVENUES TO SHIFTS (UPDATE ONLY) ==================
let entranceOutletIdCache = null;
async function getEntranceOutletId() {
  if (entranceOutletIdCache) return entranceOutletIdCache;
  const [id] = await linkOutlet(ENTRANCE_OUTLET_NAME);
  entranceOutletIdCache = id;
  return id;
}

async function applyRevenuesToShifts(accruals, startDate, endDate) {
  if (!accruals || !accruals.length)
    return { updated: 0, totalWrites: 0, entranceWrites: 0 };

  const entranceOutletId = await getEntranceOutletId();

  const totalByKey = new Map();
  const entranceByKey = new Map();

  for (const a of accruals) {
    const d = toISODateOnly(a.date);
    if (!d) continue;

    const outletId = a.outletId;
    if (!outletId) continue;

    const k = `${d}::${outletId}`;
    totalByKey.set(k, normalizeMoneyToNumber(a.totalRevenue || 0));

    if (outletId === entranceOutletId) {
      entranceByKey.set(k, normalizeMoneyToNumber(a.entranceRevenue || 0));
    }
  }

  const formula =
    `AND(` +
    `DATETIME_FORMAT({${FIELD_DATE}}, "YYYY-MM-DD") >= "${startDate}",` +
    `DATETIME_FORMAT({${FIELD_DATE}}, "YYYY-MM-DD") <= "${endDate}"` +
    `)`;

  const shiftRecs = await base(TABLE_SHIFTS)
    .select({
      filterByFormula: formula,
      fields: [FIELD_DATE, FIELD_OUTLET, SHIFT_PAYTYPE_FIELD],
    })
    .all();

  if (!shiftRecs.length)
    return { updated: 0, totalWrites: 0, entranceWrites: 0 };

  const updates = [];
  let totalWrites = 0;
  let entranceWrites = 0;

  for (const r of shiftRecs) {
    const date = toISODateOnly(r.fields?.[FIELD_DATE]);
    const outletLinks = r.fields?.[FIELD_OUTLET];
    if (!date) continue;
    if (!Array.isArray(outletLinks) || !outletLinks[0]) continue;

    const outletId = outletLinks[0];
    const payType = pickTextValue(r.fields?.[SHIFT_PAYTYPE_FIELD]);
    const key = `${date}::${outletId}`;

    if (payType === PAYTYPE_FOR_ENTRANCE) {
      if (outletId !== entranceOutletId) continue;
      const ent = entranceByKey.has(key) ? entranceByKey.get(key) : 0;

      updates.push({
        id: r.id,
        fields: { [FIELD_ENTRANCE_REVENUE]: ent },
      });

      entranceWrites++;
      continue;
    }

    if (PAYTYPE_FOR_TOTAL.has(payType)) {
      const total = totalByKey.get(key);
      if (typeof total !== "number") continue;

      updates.push({
        id: r.id,
        fields: { [FIELD_REVENUE]: total },
      });

      totalWrites++;
      continue;
    }
  }

  if (updates.length) {
    await updateInBatches(TABLE_SHIFTS, updates, 10);
  }

  console.log(`[TOTAL] writes "${FIELD_REVENUE}": ${totalWrites}`);
  console.log(
    `[ENTRANCE] writes "${FIELD_ENTRANCE_REVENUE}": ${entranceWrites}`,
  );

  return { updated: updates.length, totalWrites, entranceWrites };
}

// ================== SYNC "Відрахування" -> "Нарахування" ==================
let syncDeductionsLock = false;

function normalizeAirtableDateToISO(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}
function makeDedKey(employeeId, outletId, dateISO) {
  return `${employeeId}__${outletId}__${dateISO}`;
}

function fmtUndedItem(x) {
  const parts = [];
  parts.push(`• ${x.id}`);
  if (x.reason) parts.push(`— ${x.reason}`);
  if (x.key) parts.push(`(key: ${x.key})`);
  return parts.join(" ");
}

async function syncDeductionsToAccruals() {
  const stats = {
    deductionsTotal: 0,
    deductionsSkippedMissingFields: 0,
    keys: 0,
    accrualsTotal: 0,
    accrualsSkippedMissingFields: 0,
    updatesPlanned: 0,
    updated: 0,
    batches: 0,

    deductionsNotAddedCount: 0,
    deductionsNotAddedSample: [],
  };

  const deductionRecords = await base(TABLE_DEDUCTIONS)
    .select({ fields: [FIELD_EMPLOYEE, FIELD_OUTLET, FIELD_DATE] })
    .all();

  stats.deductionsTotal = deductionRecords.length;

  const map = new Map();

  const invalidDeductionIds = [];

  for (const r of deductionRecords) {
    const emp = r.get(FIELD_EMPLOYEE);
    const outlet = r.get(FIELD_OUTLET);
    const dateISO = normalizeAirtableDateToISO(r.get(FIELD_DATE));

    if (!emp?.[0] || !outlet?.[0] || !dateISO) {
      stats.deductionsSkippedMissingFields++;
      invalidDeductionIds.push({
        id: r.id,
        reason: "немає Працівник/Заклад/Дата",
        key: "",
      });
      continue;
    }

    const key = makeDedKey(emp[0], outlet[0], dateISO);

    if (!map.has(key)) map.set(key, []);
    map.get(key).push(r.id);
  }

  stats.keys = map.size;

  const accrualRecords = await base(TABLE_SHIFTS)
    .select({
      fields: [FIELD_EMPLOYEE, FIELD_OUTLET, FIELD_DATE, FIELD_DEDUCTIONS_LINK],
    })
    .all();

  stats.accrualsTotal = accrualRecords.length;

  const accrualKeySet = new Set();
  const alreadyLinkedDeductionIds = new Set();

  for (const r of accrualRecords) {
    const emp = r.get(FIELD_EMPLOYEE);
    const outlet = r.get(FIELD_OUTLET);
    const dateISO = normalizeAirtableDateToISO(r.get(FIELD_DATE));

    if (!emp?.[0] || !outlet?.[0] || !dateISO) {
      stats.accrualsSkippedMissingFields++;
      continue;
    }

    const key = makeDedKey(emp[0], outlet[0], dateISO);
    accrualKeySet.add(key);

    const existing = r.get(FIELD_DEDUCTIONS_LINK) || [];
    for (const id of existing) alreadyLinkedDeductionIds.add(id);
  }

  const updates = [];

  for (const r of accrualRecords) {
    const emp = r.get(FIELD_EMPLOYEE);
    const outlet = r.get(FIELD_OUTLET);
    const dateISO = normalizeAirtableDateToISO(r.get(FIELD_DATE));

    if (!emp?.[0] || !outlet?.[0] || !dateISO) continue;

    const key = makeDedKey(emp[0], outlet[0], dateISO);
    const needed = map.get(key);
    if (!needed?.length) continue;

    const existing = r.get(FIELD_DEDUCTIONS_LINK) || [];
    const set = new Set(existing);

    let changed = false;
    for (const id of needed) {
      if (!set.has(id)) {
        set.add(id);
        changed = true;
      }
    }

    if (changed) {
      updates.push({
        id: r.id,
        fields: { [FIELD_DEDUCTIONS_LINK]: Array.from(set) },
      });
    }
  }

  stats.updatesPlanned = updates.length;

  const updatesCopyLen = updates.length;
  while (updates.length) {
    stats.batches++;
    const batch = updates.splice(0, 10);
    await base(TABLE_SHIFTS).update(batch);
    stats.updated += batch.length;
  }

  const notAdded = [];

  for (const x of invalidDeductionIds) notAdded.push(x);

  for (const r of deductionRecords) {
    const emp = r.get(FIELD_EMPLOYEE);
    const outlet = r.get(FIELD_OUTLET);
    const dateISO = normalizeAirtableDateToISO(r.get(FIELD_DATE));

    if (!emp?.[0] || !outlet?.[0] || !dateISO) continue;

    const key = makeDedKey(emp[0], outlet[0], dateISO);

    if (!accrualKeySet.has(key)) {
      notAdded.push({
        id: r.id,
        reason:
          "немає відповідного запису у «Нарахування» (Працівник+Заклад+Дата)",
        key,
      });
    }
  }

  const filteredNotAdded = notAdded.filter(
    (x) => !alreadyLinkedDeductionIds.has(x.id),
  );

  stats.deductionsNotAddedCount = filteredNotAdded.length;

  stats.deductionsNotAddedSample = filteredNotAdded
    .slice(0, 20)
    .map(fmtUndedItem);

  console.log(
    `[DEDUCTIONS] planned=${updatesCopyLen} updated=${stats.updated} notAdded=${stats.deductionsNotAddedCount}`,
  );

  return stats;
}

// ================== UI ==================
function mainMenu(ctx) {
  const buttons = [];
  buttons.push([Markup.button.callback("Заповнити зміну", "EMP_FILL_SHIFT")]);
  buttons.push([Markup.button.callback("📄 Звіт по змінах", "EMP_REPORT")]);

  if (isAdmin(ctx)) {
    buttons.push([
      Markup.button.callback("Передати інформацію з POSTER", "ADM_POSTER"),
    ]);
    buttons.push([
      Markup.button.callback(
        "🔁 Синхр. Відрахування → Нарахування",
        "ADM_SYNC_DEDUCTIONS",
      ),
    ]);
    buttons.push([
      Markup.button.callback("➕ Додати працівника", "ADM_ADD_EMPLOYEE"),
    ]);
  }

  return ctx.reply("Головне меню:", Markup.inlineKeyboard(buttons));
}

bot.start((ctx) => mainMenu(ctx));

// ================== EMPLOYEE FLOW ==================
bot.action("EMP_FILL_SHIFT", async (ctx) => {
  await ctx.answerCbQuery();
  userState.set(ctx.from.id, { role: "employee", step: "DATE" });
  return ctx.reply(
    "Введіть дату зміни:\n\n" +
      "• ММ-ДД (рік буде підставлено автоматично)\n" +
      "• або РРРР-ММ-ДД\n\n" +
      "Приклад: 12-08",
  );
});

// ===== EMPLOYEE: REPORT =====
bot.action("EMP_REPORT", async (ctx) => {
  await ctx.answerCbQuery();
  userState.set(ctx.from.id, { role: "employee", step: "REPORT_START_INPUT" });
  return ctx.reply(
    "Звіт по змінах.\n\nВведіть початкову дату:\n• ММ-ДД\n• або РРРР-ММ-ДД",
  );
});

bot.action("EMP_CANCEL", async (ctx) => {
  await ctx.answerCbQuery();
  userState.delete(ctx.from.id);
  return ctx.reply("Операція відмінена.");
});
bot.action(/EMP_POS_A_(\d+)/, async (ctx) => {
  await ctx.answerCbQuery();
  const idx = Number(ctx.match[1]);

  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "employee" || state.step !== "POSITION_ALLOWED")
    return;

  const item = state.allowedPositions?.[idx];
  if (!item) return ctx.reply("Невірна посада. Спробуй ще раз.");

  try {
    const outletLink = await linkOutlet(state.outlet);
    const employeeLink = await linkEmployeeByTgId(ctx.from.id);

    const fields = {
      [FIELD_DATE]: state.date,
      [FIELD_OUTLET]: outletLink,
      [FIELD_POSITION]: [item.id],
      [FIELD_EMPLOYEE]: employeeLink,
    };

    const record = await base(TABLE_SHIFTS).create(fields);
    userState.delete(ctx.from.id);

    return ctx.reply(
      `Зміна збережена:\nДата: ${state.date}\nЗаклад: ${state.outlet}\nПосада: ${item.name}`,
      Markup.inlineKeyboard([
        [Markup.button.callback("✏️ Змінити", `EMP_EDIT_${record.id}`)],
        [Markup.button.callback("Видалити", `EMP_DEL_${record.id}`)],
        [Markup.button.callback("Створити нову", "EMP_FILL_SHIFT")],
      ]),
    );
  } catch (e) {
    console.error(e);
    return ctx.reply("Помилка при збереженні.");
  }
});

bot.on("text", async (ctx) => {
  const state = userState.get(ctx.from.id);
  if (!state) return;

  if (state.role === "employee") return handleEmployeeFlow(ctx, state);
  if (state.role === "admin") return handleAdminFlow(ctx, state);
});

async function handleEmployeeFlow(ctx, state) {
  const text = ctx.message.text.trim();

  if (state.step === "DATE") {
    let isoDate;
    try {
      isoDate = normalizeEmployeeDate(text);
    } catch {
      return ctx.reply(
        "Невірний формат.\n" +
          "Введіть дату як:\n" +
          "• ММ-ДД (наприклад, 12-08)\n" +
          "• або РРРР-ММ-ДД",
      );
    }

    state.date = isoDate;
    state.step = "OUTLET";

    return ctx.reply(
      "Виберіть заклад:",
      Markup.inlineKeyboard(
        OUTLETS.map((o, i) => [Markup.button.callback(o, `EMP_OUTLET_${i}`)]),
      ),
    );
  }

  if (state.step === "EDIT_DATE_INPUT") {
    let isoDate;
    try {
      isoDate = normalizeEmployeeDate(text);
    } catch {
      return ctx.reply("Невірний формат.\nВведіть:\n• ММ-ДД\n• або РРРР-ММ-ДД");
    }

    try {
      await base(TABLE_SHIFTS).update(state.editRecId, {
        [FIELD_DATE]: isoDate,
      });

      const recId = state.editRecId;
      userState.delete(ctx.from.id);

      return ctx.reply(
        `✅ Оновлено! Нова дата: ${isoDate}`,
        Markup.inlineKeyboard([
          [Markup.button.callback("✏️ Змінити ще", `EMP_EDIT_${recId}`)],
        ]),
      );
    } catch (e) {
      console.error(e);
      return ctx.reply("❌ Не вдалося оновити дату.");
    }
  }

  // ===== REPORT FLOW =====
  if (state.step === "REPORT_START_INPUT") {
    let isoDate;
    try {
      isoDate = normalizeEmployeeDate(text);
    } catch {
      return ctx.reply(
        "Невірний формат. Введіть дату як ММ-ДД або РРРР-ММ-ДД.",
      );
    }

    state.reportStart = isoDate;
    state.step = "REPORT_START_CONFIRM";

    return ctx.reply(
      `Початкова дата: ${state.reportStart}`,
      Markup.inlineKeyboard([
        [Markup.button.callback("✅ Підтвердити", "EMP_REP_START_OK")],
        [Markup.button.callback("✏️ Змінити", "EMP_REP_START_EDIT")],
        [Markup.button.callback("❌ Скасувати", "EMP_CANCEL")],
      ]),
    );
  }

  if (state.step === "REPORT_END_INPUT") {
    let isoDate;
    try {
      isoDate = normalizeEmployeeDate(text);
      if (parseISODate(isoDate) < parseISODate(state.reportStart)) {
        return ctx.reply("Кінцева дата не може бути раніше початкової.");
      }
    } catch {
      return ctx.reply(
        "Невірний формат. Введіть дату як ММ-ДД або РРРР-ММ-ДД.",
      );
    }

    state.reportEnd = isoDate;
    state.step = "REPORT_END_CONFIRM";

    return ctx.reply(
      `Кінцева дата: ${state.reportEnd}`,
      Markup.inlineKeyboard([
        [Markup.button.callback("✅ Підтвердити", "EMP_REP_END_OK")],
        [Markup.button.callback("✏️ Змінити", "EMP_REP_END_EDIT")],
        [Markup.button.callback("❌ Скасувати", "EMP_CANCEL")],
      ]),
    );
  }
}

// ===== Employee outlet select (INDEX) =====
bot.action(/EMP_OUTLET_(\d+)/, async (ctx) => {
  await ctx.answerCbQuery();
  const idx = Number(ctx.match[1]);

  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "employee") return;

  const outlet = OUTLETS[idx];
  if (!outlet) return ctx.reply("Невірний заклад. Спробуй ще раз.");

  state.outlet = outlet;
  state.step = "POSITION_ALLOWED";

  const allowed = await getEmployeeAllowedPositions(ctx);
  if (!allowed.length) {
    userState.delete(ctx.from.id);
    return ctx.reply(
      `❌ У тебе не вказані посади в Airtable.\nПопроси адміна заповнити поле "${EMPLOYEE_POSITIONS_FIELD}" у таблиці "${TABLE_EMPLOYEES}".`,
    );
  }

  state.allowedPositions = allowed;

  return ctx.reply("Виберіть посаду:", buildAllowedPositionsKeyboard(allowed));
});

// ===== Employee delete shift (only own) =====
bot.action(/EMP_DEL_(.+)/, async (ctx) => {
  await ctx.answerCbQuery();
  const recId = ctx.match[1];

  try {
    const employeeId = await getEmployeeRecIdByTgId(ctx.from.id);
    const rec = await base(TABLE_SHIFTS).find(recId);
    const recEmp = rec.fields?.[FIELD_EMPLOYEE];

    if (!Array.isArray(recEmp) || recEmp[0] !== employeeId) {
      return ctx.reply("❌ Ти не можеш видаляти цю зміну.");
    }

    await base(TABLE_SHIFTS).destroy(recId);
    return ctx.reply("✅ Запис видалений.");
  } catch (e) {
    console.error(e);
    return ctx.reply("Не вдалося видалити запис.");
  }
});

// ================== EMPLOYEE: EDIT SHIFT ==================
bot.action(/EMP_EDIT_(.+)/, async (ctx) => {
  await ctx.answerCbQuery();
  const recId = ctx.match[1];

  try {
    const employeeId = await getEmployeeRecIdByTgId(ctx.from.id);
    const rec = await base(TABLE_SHIFTS).find(recId);
    const recEmp = rec.fields?.[FIELD_EMPLOYEE];

    if (!Array.isArray(recEmp) || recEmp[0] !== employeeId) {
      return ctx.reply("❌ Ти не можеш змінювати цю зміну.");
    }

    userState.set(ctx.from.id, {
      role: "employee",
      step: "EDIT_WHAT",
      editRecId: recId,
    });

    return ctx.reply(
      "Що змінюємо?",
      Markup.inlineKeyboard([
        [Markup.button.callback("📅 Дата", "EMP_EDIT_DATE")],
        [Markup.button.callback("🏢 Заклад", "EMP_EDIT_OUTLET")],
        [Markup.button.callback("👤 Посада", "EMP_EDIT_POSITION")],
        [Markup.button.callback("❌ Скасувати", "EMP_CANCEL")],
      ]),
    );
  } catch (e) {
    console.error(e);
    return ctx.reply("Не вдалося відкрити редагування.");
  }
});

bot.action("EMP_EDIT_DATE", async (ctx) => {
  await ctx.answerCbQuery();
  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "employee" || state.step !== "EDIT_WHAT") return;

  state.step = "EDIT_DATE_INPUT";
  return ctx.reply("Введіть нову дату:\n• ММ-ДД\n• або РРРР-ММ-ДД");
});

bot.action("EMP_EDIT_OUTLET", async (ctx) => {
  await ctx.answerCbQuery();
  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "employee" || state.step !== "EDIT_WHAT") return;

  state.step = "EDIT_OUTLET_PICK";
  return ctx.reply(
    "Виберіть новий заклад:",
    Markup.inlineKeyboard(
      OUTLETS.map((o, i) => [
        Markup.button.callback(o, `EMP_EDIT_OUTLET_${i}`),
      ]),
    ),
  );
});

bot.action("EMP_EDIT_POSITION", async (ctx) => {
  await ctx.answerCbQuery();
  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "employee" || state.step !== "EDIT_WHAT") return;

  const allowed = await getEmployeeAllowedPositions(ctx);
  if (!allowed.length) {
    userState.delete(ctx.from.id);
    return ctx.reply(
      `❌ У тебе не вказані посади в Airtable.\nПопроси адміна заповнити поле "${EMPLOYEE_POSITIONS_FIELD}".`,
    );
  }

  state.step = "EDIT_POSITION_ALLOWED_PICK";
  state.allowedPositions = allowed;

  return ctx.reply(
    "Виберіть нову посаду:",
    buildAllowedPositionsKeyboardEdit(allowed),
  );
});

bot.action(/EMP_EDIT_OUTLET_(\d+)/, async (ctx) => {
  await ctx.answerCbQuery();
  const idx = Number(ctx.match[1]);

  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "employee" || state.step !== "EDIT_OUTLET_PICK")
    return;

  const outlet = OUTLETS[idx];
  if (!outlet) return ctx.reply("Невірний заклад.");

  try {
    const outletLink = await linkOutlet(outlet);

    await base(TABLE_SHIFTS).update(state.editRecId, {
      [FIELD_OUTLET]: outletLink,
    });

    const recId = state.editRecId;
    userState.delete(ctx.from.id);

    return ctx.reply(
      `✅ Оновлено! Новий заклад: ${outlet}`,
      Markup.inlineKeyboard([
        [Markup.button.callback("✏️ Змінити ще", `EMP_EDIT_${recId}`)],
      ]),
    );
  } catch (e) {
    console.error(e);
    return ctx.reply("❌ Не вдалося оновити заклад.");
  }
});

bot.action(/EMP_EDIT_POS_(\d+)/, async (ctx) => {
  await ctx.answerCbQuery();
  const idx = Number(ctx.match[1]);

  const state = userState.get(ctx.from.id);
  if (
    !state ||
    state.role !== "employee" ||
    state.step !== "EDIT_POSITION_PICK"
  )
    return;

  const position = POSITIONS[idx];
  if (!position) return ctx.reply("Невірна посада.");

  try {
    const positionLink = await linkPosition(position);

    await base(TABLE_SHIFTS).update(state.editRecId, {
      [FIELD_POSITION]: positionLink,
    });

    const recId = state.editRecId;
    userState.delete(ctx.from.id);

    return ctx.reply(
      `✅ Оновлено! Нова посада: ${position}`,
      Markup.inlineKeyboard([
        [Markup.button.callback("✏️ Змінити ще", `EMP_EDIT_${recId}`)],
      ]),
    );
  } catch (e) {
    console.error(e);
    return ctx.reply("❌ Не вдалося оновити посаду.");
  }
});

bot.action(/EMP_EDIT_POS_A_(\d+)/, async (ctx) => {
  await ctx.answerCbQuery();
  const idx = Number(ctx.match[1]);

  const state = userState.get(ctx.from.id);
  if (
    !state ||
    state.role !== "employee" ||
    state.step !== "EDIT_POSITION_ALLOWED_PICK"
  )
    return;

  const item = state.allowedPositions?.[idx];
  if (!item) return ctx.reply("Невірна посада.");

  try {
    await base(TABLE_SHIFTS).update(state.editRecId, {
      [FIELD_POSITION]: [item.id],
    });

    const recId = state.editRecId;
    userState.delete(ctx.from.id);

    return ctx.reply(
      `✅ Оновлено! Нова посада: ${item.name}`,
      Markup.inlineKeyboard([
        [Markup.button.callback("✏️ Змінити ще", `EMP_EDIT_${recId}`)],
      ]),
    );
  } catch (e) {
    console.error(e);
    return ctx.reply("❌ Не вдалося оновити посаду.");
  }
});

// ================== EMPLOYEE: REPORT CONFIRM/BUILD ==================
bot.action("EMP_REP_START_EDIT", async (ctx) => {
  await ctx.answerCbQuery();
  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "employee") return;

  state.step = "REPORT_START_INPUT";
  return ctx.reply("Введіть нову початкову дату:\n• ММ-ДД\n• або РРРР-ММ-ДД");
});

bot.action("EMP_REP_START_OK", async (ctx) => {
  await ctx.answerCbQuery();
  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "employee") return;

  state.step = "REPORT_END_INPUT";
  return ctx.reply("Введіть кінцеву дату:\n• ММ-ДД\n• або РРРР-ММ-ДД");
});

bot.action("EMP_REP_END_EDIT", async (ctx) => {
  await ctx.answerCbQuery();
  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "employee") return;

  state.step = "REPORT_END_INPUT";
  return ctx.reply("Введіть нову кінцеву дату:\n• ММ-ДД\n• або РРРР-ММ-ДД");
});

bot.action("EMP_REP_END_OK", async (ctx) => {
  await ctx.answerCbQuery();
  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "employee") return;

  const startDate = state.reportStart;
  const endDate = state.reportEnd;

  try {
    await ctx.reply("📄 Формую звіт...");

    const employeeRecId = await getEmployeeRecIdByTgId(ctx.from.id);

    // Фильтруем в Airtable ТОЛЬКО по дате (это быстро),
    // а по сотруднику отфильтруем уже в JS (это надежно).
    const formula =
      `AND(` +
      `DATETIME_FORMAT({${FIELD_DATE}}, "YYYY-MM-DD") >= "${escapeAirtableStr(startDate)}",` +
      `DATETIME_FORMAT({${FIELD_DATE}}, "YYYY-MM-DD") <= "${escapeAirtableStr(endDate)}"` +
      `)`;

    const allInRange = await base(TABLE_SHIFTS)
      .select({
        filterByFormula: formula,
        fields: [FIELD_DATE, FIELD_POSITION, FIELD_OUTLET, FIELD_EMPLOYEE],
      })
      .all();

    // JS-фильтр по linked recordId сотрудника
    const shiftRecs = allInRange.filter((r) => {
      const empLinks = r.fields?.[FIELD_EMPLOYEE];
      return Array.isArray(empLinks) && empLinks.includes(employeeRecId);
    });

    if (!shiftRecs.length) {
      userState.delete(ctx.from.id);
      return ctx.reply(
        `Немає змін за період ${startDate} — ${endDate}.`,
        Markup.inlineKeyboard([[Markup.button.callback("🏠 Меню", "GO_MENU")]]),
      );
    }

    const rows = shiftRecs
      .map((r) => {
        const dateISO = toISODateOnly(r.fields?.[FIELD_DATE]);
        const posId = Array.isArray(r.fields?.[FIELD_POSITION])
          ? r.fields[FIELD_POSITION][0]
          : null;
        const outId = Array.isArray(r.fields?.[FIELD_OUTLET])
          ? r.fields[FIELD_OUTLET][0]
          : null;

        return { dateISO, posId, outId };
      })
      .filter((x) => x.dateISO && x.posId && x.outId);

    // сортировка по дате
    rows.sort((a, b) =>
      a.dateISO < b.dateISO ? -1 : a.dateISO > b.dateISO ? 1 : 0,
    );

    const lines = [];
    for (let i = 0; i < rows.length; i++) {
      const x = rows[i];
      const posName = await getPositionNameById(x.posId);
      const outName = await getOutletNameById(x.outId);
      const ddmmyyyy = isoToDDMMYYYY(x.dateISO);

      lines.push(`${i + 1}. ${ddmmyyyy} - ${posName} - ${outName}`);
      if (i !== rows.length - 1) lines.push("- - -");
    }

    const header = `Звіт по змінах (${startDate} — ${endDate})\n\n`;
    const full = header + lines.join("\n");

    // Telegram лимит на сообщение — режем на части если нужно
    const MAX = 3900;
    if (full.length <= MAX) {
      await ctx.reply(full);
    } else {
      let chunk = header;
      for (const line of lines) {
        if (chunk.length + line.length + 1 > MAX) {
          await ctx.reply(chunk);
          chunk = "";
        }
        chunk += (chunk ? "\n" : "") + line;
      }
      if (chunk.trim()) await ctx.reply(chunk);
    }

    userState.delete(ctx.from.id);
    return ctx.reply(
      "✅ Готово.",
      Markup.inlineKeyboard([[Markup.button.callback("🏠 Меню", "GO_MENU")]]),
    );
  } catch (e) {
    console.error(e);
    return ctx.reply(`❌ Помилка звіту: ${e.message || "unknown error"}`);
  }
});

bot.action("GO_MENU", async (ctx) => {
  await ctx.answerCbQuery();
  return mainMenu(ctx);
});

// ================== ADMIN FLOW ==================
bot.action("ADM_POSTER", async (ctx) => {
  await ctx.answerCbQuery();
  if (!isAdmin(ctx)) return ctx.reply("Ви не адміністратор.");

  userState.set(ctx.from.id, { role: "admin", step: "START_DATE_INPUT" });
  return ctx.reply("Введіть початкову дату періода (РРРР-ММ-ДД).");
});

async function handleAdminFlow(ctx, state) {
  const text = ctx.message.text.trim();

  if (state.step === "START_DATE_INPUT") {
    try {
      assertISODate(text);
    } catch {
      return ctx.reply("Невірний формат. Введіть дату як РРРР-ММ-ДД.");
    }

    state.startDate = text;
    state.step = "START_DATE_CONFIRM";
    return ctx.reply(
      `Початкова дата: ${state.startDate}`,
      Markup.inlineKeyboard([
        [Markup.button.callback("✅ Підтвердити", "ADM_START_OK")],
        [Markup.button.callback("✏️ Змінити", "ADM_START_EDIT")],
        [Markup.button.callback("❌ Скасувати", "ADM_CANCEL")],
      ]),
    );
  }
  if (state.step === "EMP_NEW_NAME_INPUT") {
    const name = String(text || "").trim();
    if (name.length < 3)
      return ctx.reply("Введи ім'я та прізвище (мінімум 3 символи).");

    state.newEmpName = name;
    state.step = "EMP_NEW_TGID_INPUT";
    userState.set(ctx.from.id, state);

    return ctx.reply(
      `Працівник: ${name}\nНадішли numeric Telegram ID або перешли повідомлення від людини.`,
    );
  }
  if (state.step === "EMP_SET_TGID_INPUT") {
    const res = extractTelegramIdFromCtxOrText(ctx, text);
    if (!res.ok) return ctx.reply(res.error);

    try {
      await base(TABLE_EMPLOYEES).update(state.employeeRecId, {
        [EMP_TG_FIELD]: tgIdForAirtable(res.id),
      });

      const name = state.employeeName || "Працівник";
      userState.delete(ctx.from.id);

      return ctx.reply(
        `✅ Готово! Записав Telegram ID для:\n${name}\nID: ${res.id}`,
      );
    } catch (e) {
      console.error(e);
      return ctx.reply("❌ Не вдалося записати Telegram ID в Airtable.");
    }
  }
  if (state.step === "EMP_NEW_TGID_INPUT") {
    const res = extractTelegramIdFromCtxOrText(ctx, text);
    if (!res.ok) return ctx.reply(res.error);

    try {
      const posNames = (state.newEmpPositions || [])
        .map((i) => POSITIONS[i])
        .filter(Boolean);

      // linked ids (таблица "Посади", поле POSITIONS_NAME_FIELD)
      const posLinkIds = [];
      for (const pName of posNames) {
        const [id] = await linkPosition(pName);
        posLinkIds.push(id);
      }

      const fields = {
        [EMPLOYEE_NAME_FIELD]: state.newEmpName,
        [EMP_TG_FIELD]: tgIdForAirtable(res.id),
        [EMPLOYEE_POSITIONS_FIELD]: posLinkIds,
      };

      const created = await base(TABLE_EMPLOYEES).create(fields);

      userState.delete(ctx.from.id);

      return ctx.reply(
        `✅ Створено працівника!\n` +
          `Ім'я: ${fields[EMPLOYEE_NAME_FIELD]}\n` +
          `Telegram ID: ${fields[EMP_TG_FIELD]}\n` +
          `Посади: ${posNames.join(", ")}\n` +
          `Record: ${created.id}`,
      );
    } catch (e) {
      console.error(e);
      return ctx.reply(
        "❌ Не вдалося створити працівника.\n" +
          `Перевір:\n` +
          `• EMPLOYEE_NAME_FIELD = "${EMPLOYEE_NAME_FIELD}"\n` +
          `• EMPLOYEE_POSITIONS_FIELD = "${EMPLOYEE_POSITIONS_FIELD}" (linked на таблицю "Посади")\n` +
          `• POSITIONS_NAME_FIELD = "${POSITIONS_NAME_FIELD}" збігається з тим, що збережено в "Посади"`,
      );
    }
  }

  if (state.step === "END_DATE_INPUT") {
    try {
      assertISODate(text);
      if (parseISODate(text) < parseISODate(state.startDate)) {
        return ctx.reply("Кінцева дата не може бути раніше початкової.");
      }
    } catch {
      return ctx.reply("Невірний формат. Введіть дату як РРРР-ММ-ДД.");
    }

    state.endDate = text;
    state.step = "END_DATE_CONFIRM";
    return ctx.reply(
      `Кінцева дата: ${state.endDate}`,
      Markup.inlineKeyboard([
        [Markup.button.callback("✅ Підтвердити", "ADM_END_OK")],
        [Markup.button.callback("✏️ Змінити", "ADM_END_EDIT")],
        [Markup.button.callback("❌ Скасувати", "ADM_CANCEL")],
      ]),
    );
  }
}

bot.action("ADM_ADD_EMPLOYEE", async (ctx) => {
  await ctx.answerCbQuery();
  if (!isAdmin(ctx)) return ctx.reply("Ви не адміністратор.");

  userState.set(ctx.from.id, { role: "admin", step: "EMP_ADD_MODE" });

  return ctx.reply(
    "Додати працівника — обери режим:",
    Markup.inlineKeyboard([
      [
        Markup.button.callback(
          "Додати ID до існуючого",
          "ADM_EMP_MODE_EXISTING",
        ),
      ],
      [
        Markup.button.callback(
          "Створити нового працівника",
          "ADM_EMP_MODE_NEW",
        ),
      ],
      [Markup.button.callback("❌ Скасувати", "ADM_CANCEL")],
    ]),
  );
});

bot.action("ADM_START_EDIT", async (ctx) => {
  await ctx.answerCbQuery();
  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "admin") return;
  state.step = "START_DATE_INPUT";
  return ctx.reply("Введіть нову початкову дату (РРРР-ММ-ДД).");
});

bot.action("ADM_START_OK", async (ctx) => {
  await ctx.answerCbQuery();
  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "admin") return;
  state.step = "END_DATE_INPUT";
  return ctx.reply("Введіть кінцеву дату періода (РРРР-ММ-ДД).");
});

bot.action("ADM_END_EDIT", async (ctx) => {
  await ctx.answerCbQuery();
  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "admin") return;
  state.step = "END_DATE_INPUT";
  return ctx.reply("Введіть нову кінцеву дату періода (РРРР-ММ-ДД).");
});

bot.action("ADM_END_OK", async (ctx) => {
  await ctx.answerCbQuery();
  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "admin") return;

  return ctx.reply(
    `Період:\n${state.startDate} — ${state.endDate}\n\nПередати інформацію з Poster?`,
    Markup.inlineKeyboard([
      [Markup.button.callback("🚀 Передати інформацію", "ADM_SEND_POSTER")],
      [Markup.button.callback("❌ Скасувати", "ADM_CANCEL")],
    ]),
  );
});

bot.action("ADM_CANCEL", async (ctx) => {
  await ctx.answerCbQuery();
  userState.delete(ctx.from.id);
  return ctx.reply("Операція відмінена.");
});

bot.action("ADM_EMP_MODE_EXISTING", async (ctx) => {
  await ctx.answerCbQuery();
  if (!isAdmin(ctx)) return ctx.reply("Ви не адміністратор.");

  const state = userState.get(ctx.from.id) || {};
  state.role = "admin";
  state.step = "EMP_PICK_EXISTING";
  userState.set(ctx.from.id, state);

  try {
    const recs = await base(TABLE_EMPLOYEES)
      .select({
        maxRecords: EMPLOYEE_LIST_PAGE_SIZE,
        fields: [EMPLOYEE_NAME_FIELD, EMP_TG_FIELD],
        sort: EMPLOYEE_NAME_FIELD
          ? [{ field: EMPLOYEE_NAME_FIELD, direction: "asc" }]
          : undefined,
      })
      .firstPage();

    if (!recs.length) return ctx.reply("У таблиці «Працівники» немає записів.");

    state.employeePickList = recs.map((r) => ({
      id: r.id,
      name: String(r.fields?.[EMPLOYEE_NAME_FIELD] || r.id),
    }));
    userState.set(ctx.from.id, state);

    const buttons = state.employeePickList.map((x, i) => [
      Markup.button.callback(x.name, `ADM_EMP_PICK_${i}`),
    ]);

    buttons.push([Markup.button.callback("❌ Скасувати", "ADM_CANCEL")]);

    return ctx.reply("Обери працівника:", Markup.inlineKeyboard(buttons));
  } catch (e) {
    console.error(e);
    return ctx.reply("❌ Не вдалося завантажити список працівників.");
  }
});

bot.action(/ADM_EMP_PICK_(\d+)/, async (ctx) => {
  await ctx.answerCbQuery();
  if (!isAdmin(ctx)) return ctx.reply("Ви не адміністратор.");

  const idx = Number(ctx.match[1]);
  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "admin" || state.step !== "EMP_PICK_EXISTING")
    return;

  const item = state.employeePickList?.[idx];
  if (!item) return ctx.reply("Невірний вибір. Спробуй ще раз.");

  state.step = "EMP_SET_TGID_INPUT";
  state.employeeRecId = item.id;
  state.employeeName = item.name;
  userState.set(ctx.from.id, state);

  return ctx.reply(
    `Працівник: ${item.name}\n\nНадішли:\n• numeric Telegram ID\nабо\n• переслане повідомлення від людини`,
    Markup.inlineKeyboard([
      [Markup.button.callback("❌ Скасувати", "ADM_CANCEL")],
    ]),
  );
});

bot.action("ADM_EMP_MODE_NEW", async (ctx) => {
  await ctx.answerCbQuery();
  if (!isAdmin(ctx)) return ctx.reply("Ви не адміністратор.");

  const state = userState.get(ctx.from.id) || {};
  state.role = "admin";
  state.step = "EMP_NEW_POSITIONS_PICK";
  state.newEmpPositions = []; // массив индексов
  userState.set(ctx.from.id, state);

  const set = new Set(state.newEmpPositions);

  return ctx.reply(
    "Обери посади працівника (можна декілька):",
    buildPositionsKeyboard(set),
  );
});

bot.action(/EMP_POS_T_(\d+)/, async (ctx) => {
  await ctx.answerCbQuery();
  const idx = Number(ctx.match[1]);

  const state = userState.get(ctx.from.id);
  if (
    !state ||
    state.role !== "admin" ||
    state.step !== "EMP_NEW_POSITIONS_PICK"
  )
    return;

  const set = new Set(state.newEmpPositions || []);
  if (set.has(idx)) set.delete(idx);
  else set.add(idx);

  state.newEmpPositions = Array.from(set).sort((a, b) => a - b);
  userState.set(ctx.from.id, state);

  try {
    await ctx.editMessageReplyMarkup(
      buildPositionsKeyboard(new Set(state.newEmpPositions)).reply_markup,
    );
  } catch (e) {}
});

bot.action("EMP_POS_CLEAR", async (ctx) => {
  await ctx.answerCbQuery();

  const state = userState.get(ctx.from.id);
  if (
    !state ||
    state.role !== "admin" ||
    state.step !== "EMP_NEW_POSITIONS_PICK"
  )
    return;

  state.newEmpPositions = [];
  userState.set(ctx.from.id, state);

  try {
    await ctx.editMessageReplyMarkup(
      buildPositionsKeyboard(new Set()).reply_markup,
    );
  } catch (e) {}
});

bot.action("EMP_POS_DONE", async (ctx) => {
  await ctx.answerCbQuery();

  const state = userState.get(ctx.from.id);
  if (
    !state ||
    state.role !== "admin" ||
    state.step !== "EMP_NEW_POSITIONS_PICK"
  )
    return;

  if (!state.newEmpPositions || state.newEmpPositions.length === 0) {
    return ctx.reply("Обери хоча б одну посаду або натисни ❌ Скасувати.");
  }

  state.step = "EMP_NEW_NAME_INPUT";
  userState.set(ctx.from.id, state);

  return ctx.reply(
    "Введи ім'я та прізвище працівника (наприклад: Іван Петренко).",
  );
});

bot.action("ADM_SEND_POSTER", async (ctx) => {
  await ctx.answerCbQuery();
  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "admin") return;

  try {
    await ctx.reply("Отримую дані та відправляю в Airtable...");

    const posterData = await fetchPosterData(state.startDate, state.endDate);

    const acqRes = await savePosterToAirtable(posterData);

    const updRes = await applyRevenuesToShifts(
      posterData.accruals,
      state.startDate,
      state.endDate,
    );

    await ctx.reply(
      `Еквайринг: створено ${acqRes.created}\n` +
        `Нарахування: оновлено ${updRes.updated}\n` +
        `— Виручка: ${updRes.totalWrites}\n` +
        `— Виручка Вхід: ${updRes.entranceWrites}`,
    );

    userState.delete(ctx.from.id);
    return ctx.reply("Дані успішно передані в Airtable.");
  } catch (e) {
    console.error(e);
    return ctx.reply(`Помилка імпорту: ${e.message || "unknown error"}`);
  }
});

// ✅ ADMIN: SYNC DEDUCTIONS BUTTON
bot.action("ADM_SYNC_DEDUCTIONS", async (ctx) => {
  await ctx.answerCbQuery();
  if (!isAdmin(ctx)) return ctx.reply("Ви не адміністратор.");

  if (syncDeductionsLock) {
    return ctx.reply(
      "⏳ Синхронізація вже виконується. Спробуй трохи пізніше.",
    );
  }

  syncDeductionsLock = true;

  try {
    await ctx.reply(
      "🔄 Запускаю синхронізацію «Відрахування → Нарахування»...",
    );

    const s = await syncDeductionsToAccruals();

    let msg =
      `✅ Готово!\n\n` +
      `Відрахування: ${s.deductionsTotal}\n` +
      `Пропущено (немає Працівник/Заклад/Дата): ${s.deductionsSkippedMissingFields}\n` +
      `Унікальних ключів: ${s.keys}\n\n` +
      `Нарахування: ${s.accrualsTotal}\n` +
      `Пропущено (немає Працівник/Заклад/Дата): ${s.accrualsSkippedMissingFields}\n\n` +
      `План оновлень: ${s.updatesPlanned}\n` +
      `Оновлено: ${s.updated}\n` +
      `Батчів: ${s.batches}\n\n` +
      `⚠️ Відрахування НЕ були додані: ${s.deductionsNotAddedCount}`;

    await ctx.reply(msg);
  } catch (e) {
    console.error(e);
    await ctx.reply(
      `❌ Помилка синхронізації: ${e.message || "unknown error"}`,
    );
  } finally {
    syncDeductionsLock = false;
  }
});

// ================== START ==================
bot.launch();
console.log("Bot is running...");

process.once("SIGINT", () => bot.stop("SIGINT"));
process.once("SIGTERM", () => bot.stop("SIGTERM"));
