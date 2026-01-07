require("dotenv").config();
const { Telegraf, Markup } = require("telegraf");
const Airtable = require("airtable");
const axios = require("axios");

const bot = new Telegraf(process.env.BOT_TOKEN);

const ADMIN_IDS = process.env.ADMIN_IDS
  ? process.env.ADMIN_IDS.split(",").map((id) => id.trim())
  : [];

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
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

// paytype on SHIFT (formula / lookup already solved by you)
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
      `Не знайдено "${name}" у таблиці "${tableName}" по полю "${nameField}".`
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
    outletName
  );
  return [id];
}

async function linkPosition(positionName) {
  const id = await getLinkedRecordIdByName(
    TABLE_POSITIONS,
    POSITIONS_NAME_FIELD,
    positionName
  );
  return [id];
}

// ---- Employee by TG ID (linked) ----
const employeeCache = new Map();

async function getEmployeeRecIdByTgId(tgId) {
  const key = String(tgId);
  if (employeeCache.has(key)) return employeeCache.get(key);

  const escaped = key.replace(/"/g, '\\"');
  const formula = `{${EMP_TG_FIELD}} = "${escaped}"`;

  const records = await base(TABLE_EMPLOYEES)
    .select({ filterByFormula: formula, maxRecords: 1 })
    .firstPage();

  if (!records || records.length === 0) {
    throw new Error(
      `Твого профілю немає в "${TABLE_EMPLOYEES}". Попроси адміна додати тебе (поле "${EMP_TG_FIELD}" = ${key}).`
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
      `Poster error for ${account}: ${data.error.message || "Unknown error"}`
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
        day.payed_sum_sum ?? day.total_sum ?? day.sum ?? 0
      ) / 100,
  }));
}

// entrance: dash.getCategoriesSales for ONE day
async function posterGetEntranceRevenueForOneDay({ account, token, dateISO }) {
  const url = `https://${account}.joinposter.com/api/dash.getCategoriesSales`;

  const { data } = await axios.get(url, {
    params: { token, date_from: dateISO, date_to: dateISO },
    timeout: 20000,
  });

  if (typeof data === "string") {
    throw new Error(
      `Poster returned non-JSON string for ${account} (categories)`
    );
  }
  if (data?.error) {
    throw new Error(
      `Poster error for ${account} (categories): ${
        data.error.message || "Unknown error"
      }`
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
  return revenue; // number
}

// ================== FETCH POSTER DATA (demo/real) ==================
async function fetchPosterData(startDate, endDate) {
  const mode = (process.env.POSTER_MODE || "demo").toLowerCase();

  // outletName -> outletRecordId (Airtable)
  const outletIdByName = {};
  for (const outlet of OUTLETS) {
    const [id] = await linkOutlet(outlet);
    outletIdByName[outlet] = id;
  }

  // DEMO
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

  // REAL
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

      // acquiring: всегда создаём
      acquiring.push({
        date: d.date,
        outlet,
        outletId: outletIdByName[outlet],
        cardRevenue: d.cardRevenue,
      });
    }

    // accruals: total + (entrance only for Джерельна)
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
        a.entranceRevenue = entrance; // number
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

  // key = YYYY-MM-DD::outletId
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
    `{${FIELD_DATE}} >= DATETIME_PARSE("${startDate}"),` +
    `{${FIELD_DATE}} <= DATETIME_PARSE("${endDate}")` +
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
    `[ENTRANCE] writes "${FIELD_ENTRANCE_REVENUE}": ${entranceWrites}`
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

  const validDeductionIds = new Set();
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
    validDeductionIds.add(r.id);

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
    (x) => !alreadyLinkedDeductionIds.has(x.id)
  );

  stats.deductionsNotAddedCount = filteredNotAdded.length;

  stats.deductionsNotAddedSample = filteredNotAdded
    .slice(0, 20)
    .map(fmtUndedItem);

  console.log(
    `[DEDUCTIONS] planned=${updatesCopyLen} updated=${stats.updated} notAdded=${stats.deductionsNotAddedCount}`
  );

  return stats;
}

// ================== UI ==================
function mainMenu(ctx) {
  const buttons = [];
  buttons.push([Markup.button.callback("Заповнити зміну", "EMP_FILL_SHIFT")]);

  if (isAdmin(ctx)) {
    buttons.push([
      Markup.button.callback("Передати інформацію з POSTER", "ADM_POSTER"),
    ]);
    buttons.push([
      Markup.button.callback(
        "🔁 Синхр. Відрахування → Нарахування",
        "ADM_SYNC_DEDUCTIONS"
      ),
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
    "Введіть дату зміни в форматі РРРР-ММ-ДД (наприклад, 2025-12-08)."
  );
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
    try {
      assertISODate(text);
    } catch {
      return ctx.reply("Невірний формат. Введіть дату як РРРР-ММ-ДД.");
    }

    state.date = text;
    state.step = "OUTLET";

    return ctx.reply(
      "Виберіть заклад:",
      Markup.inlineKeyboard(
        OUTLETS.map((o) => [Markup.button.callback(o, `EMP_OUTLET_${o}`)])
      )
    );
  }
}

bot.action(/EMP_OUTLET_(.+)/, async (ctx) => {
  await ctx.answerCbQuery();
  const outlet = ctx.match[1];

  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "employee") return;

  state.outlet = outlet;
  state.step = "POSITION";

  return ctx.reply(
    "Виберіть посаду:",
    Markup.inlineKeyboard(
      POSITIONS.map((p) => [Markup.button.callback(p, `EMP_POS_${p}`)])
    )
  );
});

bot.action(/EMP_POS_(.+)/, async (ctx) => {
  await ctx.answerCbQuery();
  const position = ctx.match[1];

  const state = userState.get(ctx.from.id);
  if (!state || state.role !== "employee") return;

  state.position = position;

  try {
    const outletLink = await linkOutlet(state.outlet);
    const positionLink = await linkPosition(state.position);
    const employeeLink = await linkEmployeeByTgId(ctx.from.id);

    const fields = {
      [FIELD_DATE]: state.date,
      [FIELD_OUTLET]: outletLink,
      [FIELD_POSITION]: positionLink,
      [FIELD_EMPLOYEE]: employeeLink,
    };

    const record = await base(TABLE_SHIFTS).create(fields);
    userState.delete(ctx.from.id);

    return ctx.reply(
      `Зміна збережена:\nДата: ${state.date}\nЗаклад: ${state.outlet}\nПосада: ${state.position}`,
      Markup.inlineKeyboard([
        [Markup.button.callback("Видалити", `EMP_DEL_${record.id}`)],
        [Markup.button.callback("Створити нову", "EMP_FILL_SHIFT")],
      ])
    );
  } catch (e) {
    console.error(e);
    return ctx.reply(
      "Помилка при збереженні.\nПеревір:\n1) що ти є в таблиці 'Працівники' з правильним Telegram ID\n2) що 'Працівник/Заклад/Посада' — це linked поля\n3) назви полів збігаються 1-в-1."
    );
  }
});

bot.action(/EMP_DEL_(.+)/, async (ctx) => {
  await ctx.answerCbQuery();
  const recId = ctx.match[1];

  try {
    await base(TABLE_SHIFTS).destroy(recId);
    return ctx.reply("Запис видалений.");
  } catch (e) {
    console.error(e);
    return ctx.reply("Не вдалося видалити запис.");
  }
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
      ])
    );
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
      ])
    );
  }
}

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
  return ctx.reply("Введіть нову кінцеву дату (РРРР-ММ-ДД).");
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
    ])
  );
});

bot.action("ADM_CANCEL", async (ctx) => {
  await ctx.answerCbQuery();
  userState.delete(ctx.from.id);
  return ctx.reply("Операція відмінена.");
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
      state.endDate
    );

    await ctx.reply(
      `Еквайринг: створено ${acqRes.created}\n` +
        `Нарахування: оновлено ${updRes.updated}\n` +
        `— Виручка: ${updRes.totalWrites}\n` +
        `— Виручка Вхід: ${updRes.entranceWrites}`
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
      "⏳ Синхронізація вже виконується. Спробуй трохи пізніше."
    );
  }

  syncDeductionsLock = true;

  try {
    await ctx.reply(
      "🔄 Запускаю синхронізацію «Відрахування → Нарахування»..."
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
      `❌ Помилка синхронізації: ${e.message || "unknown error"}`
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
