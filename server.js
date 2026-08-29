require("dotenv").config();
const express  = require("express");
const path     = require("path");
const cors     = require("cors");
const bcrypt   = require("bcryptjs");
const jwt      = require("jsonwebtoken");
const crypto   = require("crypto");

/* ══════════════════════════════════════════════════════
   CONFIG
══════════════════════════════════════════════════════ */
const PORT       = process.env.PORT || 3000;
const VAPI_KEY   = process.env.VAPI_API_KEY;   // ← YOUR key — customers never see this
const VAPI_URL   = "https://api.vapi.ai";
const APP_URL    = process.env.APP_URL || `http://localhost:${PORT}`;

/* ── Required secrets: fail loudly rather than fall back to a known default.
      A hardcoded fallback here means anyone who has seen this file can forge
      an admin token or log into the admin panel. ── */
function required(name) {
  const v = process.env[name];
  if (!v || v.startsWith("change_this") || v.endsWith("_here")) {
    console.error(`\n✖  ${name} is not set in .env (or is still a placeholder).`);
    console.error(`   Generate one:  node -e "console.log(require('crypto').randomBytes(48).toString('base64url'))"\n`);
    process.exit(1);
  }
  return v;
}

const JWT_SECRET = required("JWT_SECRET");

if (!VAPI_KEY || VAPI_KEY === "vapi_your_key_here") {
  console.warn("⚠️  WARNING: VAPI_API_KEY not set in .env — agents will not be created");
}

/* ══════════════════════════════════════════════════════
   DATABASE — lowdb (pure JavaScript, no compilation needed)
══════════════════════════════════════════════════════ */
const low      = require("lowdb");
const FileSync = require("lowdb/adapters/FileSync");
const adapter  = new FileSync("voiceforge.db.json");
const db       = low(adapter);

// Set defaults
db.defaults({ users: [], agents: [] }).write();

const uid  = () => crypto.randomUUID();
const pj   = (s, d) => { try { return typeof s === "string" ? JSON.parse(s) : s; } catch { return d; } };

/* ── DB helpers ── */
const DB = {
  createUser: (email, password, name) => {
    const user = { id: uid(), email: email.toLowerCase(), password, name, lang: "en",
      plan: "free", agentLimit: 1, stripeId: null, subStatus: "inactive",
      createdAt: new Date().toISOString() };
    db.get("users").push(user).write();
    return { ...user };
  },
  getUserByEmail: e => db.get("users").find(u => u.email === e.toLowerCase()).value() || null,
  getUser:        id => db.get("users").find(u => u.id === id).value() || null,
  updateUser: (id, data) => {
    const allowed = ["name","lang","plan","agentLimit","stripeId","subStatus","minuteLimit","notes","suspended"];
    const update  = {};
    allowed.forEach(k => { if (data[k] !== undefined) update[k] = data[k]; });
    db.get("users").find(u => u.id === id).assign(update).write();
    return DB.getUser(id);
  },
  createAgent: (userId, data) => {
    const agent = {
      id: uid(), userId, vapiId: data.vapiId || null,
      name: data.agentName || "Agent",
      businessName: data.businessName || "", industry: data.industry || "",
      website: data.website || "", agentName: data.agentName || "Agent",
      voice: data.voice || "nova", tone: data.tone || "professional",
      greeting: data.greeting || "", greetingAr: data.greetingAr || "",
      phoneNumber: data.phoneNumber || null, language: data.language || "en",
      products: data.products || [], faqs: data.faqs || [],
      scripts: data.scripts || {},
      createdAt: new Date().toISOString(),
    };
    db.get("agents").push(agent).write();
    return { ...agent };
  },
  getAgent:        id     => db.get("agents").find(a => a.id === id).value() || null,
  getAgentsByUser: userId => db.get("agents").filter(a => a.userId === userId).sortBy("createdAt").reverse().value(),
  countAgents:     userId => db.get("agents").filter(a => a.userId === userId).size().value(),
  updateAgent: (id, userId, data) => {
    const existing = DB.getAgent(id);
    if (!existing || existing.userId !== userId) return null;
    const allowed = ["name","businessName","industry","website","agentName","voice","tone",
      "greeting","greetingAr","phoneNumber","language","vapiId","products","faqs","scripts"];
    const update = {};
    allowed.forEach(k => { if (data[k] !== undefined) update[k] = data[k]; });
    db.get("agents").find(a => a.id === id).assign(update).write();
    return DB.getAgent(id);
  },
  deleteAgent: (id, userId) => {
    db.get("agents").remove(a => a.id === id && a.userId === userId).write();
  },
};

/* ══════════════════════════════════════════════════════
   VAPI HELPER — uses YOUR key, not the customer's
══════════════════════════════════════════════════════ */
async function vapi(path, method = "GET", body = null) {
  const opts = {
    method,
    headers: {
      Authorization: `Bearer ${VAPI_KEY}`,   // ← YOUR Vapi key every time
      "Content-Type": "application/json",
    },
  };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(`${VAPI_URL}${path}`, opts);
  const d = await r.json();
  if (!r.ok) throw new Error(d?.message || `Vapi error ${r.status}`);
  return d;
}

/* ── Build system prompt for GPT-4 inside Vapi ── */
function buildPrompt(data, lang = "en") {
  const isAr = lang === "ar" || lang === "both";
  return `You are ${data.agentName}, an AI sales agent for ${data.businessName}.
${isAr ? "You speak BOTH Arabic and English. Respond in the same language the customer uses. If they speak Arabic, reply in Arabic. If English, reply in English." : "You speak English."}
Industry: ${data.industry || "General"}${data.website ? ` | Website: ${data.website}` : ""}
Tone: ${data.tone || "professional"} — helpful, persuasive, never pushy.
Keep responses SHORT (2-4 sentences) — this is a phone call.

PRODUCTS:
${(data.products||[]).filter(p=>p.name).map(p=>`• ${p.name}${p.price?` — ${p.price}`:""}${p.description?`: ${p.description}`:""}`).join("\n") || "Ask the customer what they need."}

FAQS:
${(data.faqs||[]).filter(f=>f.q).map(f=>`Q: ${f.q}\nA: ${f.a}`).join("\n\n") || "Answer helpfully based on context."}

OPENING: ${data.scripts?.opening || "Greet warmly and ask how you can help."}
CLOSING: ${data.scripts?.closing || "Thank the customer and summarize next steps."}

${(data.scripts?.objections||[]).filter(o=>o.trigger).length ?
  "OBJECTIONS:\n" + data.scripts.objections.filter(o=>o.trigger).map(o=>`If "${o.trigger}" → "${o.response}"`).join("\n") : ""}

Never claim to be human if sincerely asked.`.trim();
}

/* ══════════════════════════════════════════════════════
   EXPRESS APP
══════════════════════════════════════════════════════ */
const app = express();
app.use(cors());

/* Stripe signature verification needs the RAW body. express.json() would consume
   it first (body-parser sets req._body, so the route's express.raw() then skips),
   leaving constructEvent an object instead of a Buffer — every webhook would 400.
   Skip JSON parsing for that one path. */
const STRIPE_WEBHOOK_PATH = "/api/billing/webhook";
const jsonParser = express.json();
app.use((req, res, next) => {
  if (req.path === STRIPE_WEBHOOK_PATH) return next();
  jsonParser(req, res, next);
});

app.use(express.static(path.join(__dirname, "public")));

/* ── Auth middleware ── */
function auth(req, res, next) {
  const t = (req.headers.authorization || "").replace("Bearer ", "");
  if (!t) return res.status(401).json({ error: "No token" });
  try {
    req.user = DB.getUser(jwt.verify(t, JWT_SECRET).sub);
    if (!req.user) return res.status(401).json({ error: "User not found" });
    next();
  } catch { res.status(401).json({ error: "Invalid token" }); }
}

/* ══════════════════════════════════════════════════════
   AUTH ROUTES
══════════════════════════════════════════════════════ */
app.post("/api/auth/signup", async (req, res) => {
  try {
    const { name, email, password } = req.body;
    if (!name?.trim())        return res.status(400).json({ error: "Name required" });
    if (!email?.includes("@")) return res.status(400).json({ error: "Valid email required" });
    if (!password || password.length < 8) return res.status(400).json({ error: "Password min 8 characters" });
    if (DB.getUserByEmail(email)) return res.status(409).json({ error: "Email already registered" });
    const hash = await bcrypt.hash(password, 12);
    const user = DB.createUser(email, hash, name.trim());
    const token = jwt.sign({ sub: user.id }, JWT_SECRET, { expiresIn: "30d" });
    const { password: _, ...safe } = user;
    res.status(201).json({ token, user: safe });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post("/api/auth/login", async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password) return res.status(400).json({ error: "Email and password required" });
    const user = DB.getUserByEmail(email);
    if (!user || !(await bcrypt.compare(password, user.password)))
      return res.status(401).json({ error: "Invalid email or password" });
    const token = jwt.sign({ sub: user.id }, JWT_SECRET, { expiresIn: "30d" });
    const { password: _, ...safe } = user;
    res.json({ token, user: safe });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/auth/me", auth, (req, res) => {
  const { password, ...safe } = req.user;
  res.json({ user: safe });
});

app.put("/api/auth/me", auth, (req, res) => {
  const updated = DB.updateUser(req.user.id, { lang: req.body.lang });
  const { password, ...safe } = updated;
  res.json({ user: safe });
});

/* ══════════════════════════════════════════════════════
   AGENT ROUTES — all Vapi calls use YOUR key
══════════════════════════════════════════════════════ */
app.get("/api/agents", auth, (req, res) => {
  res.json(DB.getAgentsByUser(req.user.id));
});

app.post("/api/agents", auth, async (req, res) => {
  try {
    const user = req.user;
    if (DB.countAgents(user.id) >= user.agentLimit) {
      return res.status(403).json({ error: "Agent limit reached. Upgrade your plan.", code: "LIMIT" });
    }

    const data = req.body;
    const lang = data.language || "en";

    // Choose greeting based on language setting
    const greeting = lang === "ar"
      ? (data.greetingAr || `مرحباً! شكراً لاتصالك بـ ${data.businessName}. أنا ${data.agentName}. كيف يمكنني مساعدتك؟`)
      : lang === "both"
      ? (data.greeting   || `Hello! مرحباً! Thanks for calling ${data.businessName}. This is ${data.agentName}. How can I help you? كيف يمكنني مساعدتك؟`)
      : (data.greeting   || `Hello! Thanks for calling ${data.businessName}. This is ${data.agentName}. How can I help you today?`);

    // Create assistant in Vapi using YOUR key
    const vapiAsst = await vapi("/assistant", "POST", {
      name:         `${data.agentName} — ${data.businessName}`,
      firstMessage: greeting,
      voice: {
        provider: "openai",
        voiceId:  data.voice || "nova",
      },
      model: {
        provider:     "openai",
        model:        "gpt-4",
        systemPrompt: buildPrompt(data, lang),
        temperature:  0.7,
      },
      transcriber: {
        provider: "deepgram",
        model:    "nova-2",
        language: lang === "ar" ? "ar" : "en",   // Arabic STT if needed
      },
      endCallMessage:   data.scripts?.closing || (lang === "ar" ? "شكراً لاتصالك. إلى اللقاء!" : "Thank you for calling. Have a great day!"),
      recordingEnabled: true,
    });

    // Buy phone number under YOUR Vapi account
    let phoneNumber = null;
    try {
      const ph = await vapi("/phone-number/buy", "POST", {
        areaCode:    data.areaCode || "415",
        assistantId: vapiAsst.id,
      });
      phoneNumber = ph?.number || ph?.phoneNumber || null;
    } catch(e) {
      console.warn("Phone provisioning:", e.message);
    }

    // Save to YOUR database
    const agent = DB.createAgent(user.id, { ...data, vapiId: vapiAsst.id, phoneNumber });
    res.status(201).json(agent);
  } catch(e) {
    console.error("create agent:", e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/agents/:id", auth, (req, res) => {
  const agent = DB.getAgent(req.params.id);
  if (!agent || agent.userId !== req.user.id) return res.status(404).json({ error: "Not found" });
  res.json(agent);
});

app.put("/api/agents/:id", auth, async (req, res) => {
  try {
    const agent = DB.getAgent(req.params.id);
    if (!agent || agent.userId !== req.user.id) return res.status(404).json({ error: "Not found" });

    const data = { ...agent, ...req.body };

    // Sync to Vapi using YOUR key
    if (agent.vapiId) {
      await vapi(`/assistant/${agent.vapiId}`, "PATCH", {
        name:         `${data.agentName} — ${data.businessName}`,
        firstMessage: data.greeting || undefined,
        voice:        { provider: "openai", voiceId: data.voice },
        model: {
          provider:     "openai",
          model:        "gpt-4",
          systemPrompt: buildPrompt(data, data.language || "en"),
          temperature:  0.7,
        },
        transcriber: {
          provider: "deepgram",
          model:    "nova-2",
          language: (data.language || "en") === "ar" ? "ar" : "en",
        },
        endCallMessage: data.scripts?.closing || undefined,
      });
    }

    const updated = DB.updateAgent(req.params.id, req.user.id, req.body);
    res.json(updated);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete("/api/agents/:id", auth, async (req, res) => {
  try {
    const agent = DB.getAgent(req.params.id);
    if (!agent || agent.userId !== req.user.id) return res.status(404).json({ error: "Not found" });
    if (agent.vapiId) await vapi(`/assistant/${agent.vapiId}`, "DELETE").catch(() => {});
    DB.deleteAgent(req.params.id, req.user.id);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ── Calls (fetched from Vapi using YOUR key) ── */
app.get("/api/agents/:id/calls", auth, async (req, res) => {
  try {
    const agent = DB.getAgent(req.params.id);
    if (!agent || agent.userId !== req.user.id) return res.status(404).json({ error: "Not found" });
    if (!agent.vapiId) return res.json([]);
    const data = await vapi(`/call?assistantId=${agent.vapiId}&limit=50`);
    res.json(Array.isArray(data) ? data : data?.calls || []);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/agents/:id/calls/:callId", auth, async (req, res) => {
  try {
    const agent = DB.getAgent(req.params.id);
    if (!agent || agent.userId !== req.user.id) return res.status(404).json({ error: "Not found" });
    const call = await vapi(`/call/${req.params.callId}`);
    res.json(call);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ── Outbound call ── */
app.post("/api/agents/:id/outbound", auth, async (req, res) => {
  try {
    const agent = DB.getAgent(req.params.id);
    if (!agent || agent.userId !== req.user.id) return res.status(404).json({ error: "Not found" });
    if (!agent.vapiId) return res.status(400).json({ error: "Agent not initialized" });
    const call = await vapi("/call", "POST", {
      type:        "outboundPhoneCall",
      assistantId: agent.vapiId,
      customer:    { number: req.body.toNumber },
    });
    res.json({ success: true, callId: call.id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ══════════════════════════════════════════════════════
   BILLING ROUTES — Stripe
══════════════════════════════════════════════════════ */
const PLANS = {
  starter: { name:"Starter", price:49,  agentLimit:3,   priceId: process.env.STRIPE_PRICE_STARTER },
  pro:     { name:"Pro",     price:149, agentLimit:10,  priceId: process.env.STRIPE_PRICE_PRO     },
  payg:    { name:"Pay As You Go", price:0, agentLimit:999, priceId: null },
};

app.get("/api/billing/plans", (_, res) => res.json(PLANS));

app.post("/api/billing/checkout", auth, async (req, res) => {
  if (!process.env.STRIPE_SECRET_KEY) return res.status(400).json({ error: "Stripe not configured" });
  const Stripe = require("stripe");
  const stripe = Stripe(process.env.STRIPE_SECRET_KEY);
  try {
    const plan = PLANS[req.body.plan];
    if (!plan) return res.status(400).json({ error: "Invalid plan" });
    let customerId = req.user.stripeId;
    if (!customerId) {
      const c = await stripe.customers.create({ email: req.user.email, name: req.user.name, metadata: { userId: req.user.id } });
      customerId = c.id;
      DB.updateUser(req.user.id, { stripeId: customerId });
    }
    const session = await stripe.checkout.sessions.create({
      customer:   customerId,
      mode:       "subscription",
      line_items: [{ price: plan.priceId, quantity: 1 }],
      success_url:`${APP_URL}/?subscribed=true`,
      cancel_url: `${APP_URL}/?canceled=true`,
      metadata:   { userId: req.user.id, plan: req.body.plan },
      // Session metadata does NOT propagate to the subscription object. The webhook
      // reads sub.metadata.plan, so it must be set here or every plan falls back to
      // "starter" — a Pro subscriber would silently get Starter's agent limit.
      subscription_data: { metadata: { userId: req.user.id, plan: req.body.plan } },
    });
    res.json({ url: session.url });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post("/api/billing/portal", auth, async (req, res) => {
  if (!process.env.STRIPE_SECRET_KEY || !req.user.stripeId)
    return res.status(400).json({ error: "No billing account" });
  const Stripe = require("stripe");
  const stripe = Stripe(process.env.STRIPE_SECRET_KEY);
  try {
    const session = await stripe.billingPortal.sessions.create({ customer: req.user.stripeId, return_url: APP_URL });
    res.json({ url: session.url });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Stripe webhook — activates subscriptions automatically
app.post(STRIPE_WEBHOOK_PATH, express.raw({ type: "application/json" }), async (req, res) => {
  if (!process.env.STRIPE_SECRET_KEY) return res.sendStatus(200);
  const Stripe = require("stripe");
  const stripe = Stripe(process.env.STRIPE_SECRET_KEY);
  let event;
  try {
    event = stripe.webhooks.constructEvent(req.body, req.headers["stripe-signature"], process.env.STRIPE_WEBHOOK_SECRET);
  } catch(e) { return res.status(400).send(`Webhook Error: ${e.message}`); }

  const sub = event.data.object;
  if (event.type === "customer.subscription.created" || event.type === "customer.subscription.updated") {
    const plan = sub.metadata?.plan || "starter";
    const limits = PLANS[plan] || PLANS.starter;
    const user = db.get("users").find(u => u.stripeId === sub.customer).value();
    if (user) DB.updateUser(user.id, { plan, agentLimit: limits.agentLimit, subStatus: "active" });
  }
  if (event.type === "customer.subscription.deleted") {
    const user = db.get("users").find(u => u.stripeId === sub.customer).value();
    if (user) DB.updateUser(user.id, { plan: "free", agentLimit: 1, subStatus: "canceled" });
  }
  res.json({ received: true });
});

/* ══════════════════════════════════════════════════════
   ADMIN MIDDLEWARE — protects all /api/admin/* routes
══════════════════════════════════════════════════════ */
const ADMIN_USER = process.env.ADMIN_USER || "admin";
const ADMIN_PASS = required("ADMIN_PASS");

function adminAuth(req, res, next) {
  const t = (req.headers.authorization || "").replace("Bearer ", "");
  if (!t) return res.status(401).json({ error: "No token" });
  try {
    const p = jwt.verify(t, JWT_SECRET);
    if (p.role !== "admin") return res.status(403).json({ error: "Not admin" });
    next();
  } catch { res.status(401).json({ error: "Invalid token" }); }
}

/* ── Admin login ── */
app.post("/api/admin/login", (req, res) => {
  const { username, password } = req.body;
  if (username !== ADMIN_USER || password !== ADMIN_PASS)
    return res.status(401).json({ error: "Invalid admin credentials" });
  const token = jwt.sign({ role: "admin", sub: "admin" }, JWT_SECRET, { expiresIn: "8h" });
  res.json({ token });
});

/* ── GET /api/admin/stats — dashboard numbers ── */
app.get("/api/admin/stats", adminAuth, (req, res) => {
  const users  = db.get("users").value();
  const agents = db.get("agents").value();
  const plans  = { free: 0, starter: 0, pro: 0, payg: 0 };
  users.forEach(u => { if (plans[u.plan] !== undefined) plans[u.plan]++; else plans.free++; });
  res.json({
    totalUsers:    users.length,
    totalAgents:   agents.length,
    activeSubsCount: users.filter(u => u.subStatus === "active").length,
    planBreakdown: plans,
    mrr: users.filter(u=>u.subStatus==="active").reduce((s,u)=>{
      return s + ({starter:49,pro:149,payg:0}[u.plan]||0);
    },0),
  });
});

/* ── GET /api/admin/users — all users ── */
app.get("/api/admin/users", adminAuth, (req, res) => {
  const users  = db.get("users").value();
  const agents = db.get("agents").value();
  const safe   = users.map(u => {
    const { password, ...rest } = u;
    return { ...rest, agentCount: agents.filter(a => a.userId === u.id).length };
  });
  res.json(safe);
});

/* ── GET /api/admin/users/:id — single user detail ── */
app.get("/api/admin/users/:id", adminAuth, (req, res) => {
  const user = DB.getUser(req.params.id);
  if (!user) return res.status(404).json({ error: "Not found" });
  const { password, ...safe } = user;
  const agents = DB.getAgentsByUser(req.params.id);
  res.json({ user: safe, agents });
});

/* ── PUT /api/admin/users/:id — update user plan/limits/status ── */
app.put("/api/admin/users/:id", adminAuth, (req, res) => {
  const { plan, agentLimit, subStatus, minuteLimit, notes, suspended } = req.body;
  const allowed = ["name","plan","agentLimit","subStatus","minuteLimit","notes","suspended","lang"];
  const update  = {};
  allowed.forEach(k => { if (req.body[k] !== undefined) update[k] = req.body[k]; });
  // Auto-set agentLimit when plan changes (unless manually set)
  if (plan && agentLimit === undefined) {
    const planLimits = { free:1, starter:3, pro:10, payg:999, enterprise:999 };
    update.agentLimit = planLimits[plan] || 1;
  }
  const updated = DB.updateUser(req.params.id, update);
  if (!updated) return res.status(404).json({ error: "Not found" });
  const { password, ...safe } = updated;
  res.json(safe);
});

/* ── DELETE /api/admin/users/:id — delete user + their agents ── */
app.delete("/api/admin/users/:id", adminAuth, async (req, res) => {
  const user   = DB.getUser(req.params.id);
  if (!user) return res.status(404).json({ error: "Not found" });
  // Delete all their agents from Vapi too
  const agents = DB.getAgentsByUser(req.params.id);
  for (const ag of agents) {
    if (ag.vapiId) await vapi(`/assistant/${ag.vapiId}`, "DELETE").catch(() => {});
    db.get("agents").remove(a => a.id === ag.id).write();
  }
  db.get("users").remove(u => u.id === req.params.id).write();
  res.json({ success: true });
});

/* ── POST /api/admin/users/:id/reset-password ── */
app.post("/api/admin/users/:id/reset-password", adminAuth, async (req, res) => {
  const { newPassword } = req.body;
  if (!newPassword || newPassword.length < 8)
    return res.status(400).json({ error: "Password min 8 chars" });
  const hash = await bcrypt.hash(newPassword, 12);
  db.get("users").find(u => u.id === req.params.id).assign({ password: hash }).write();
  res.json({ success: true });
});

/* ── GET /api/admin/agents — all agents across all users ── */
app.get("/api/admin/agents", adminAuth, (req, res) => {
  const agents = db.get("agents").value();
  const users  = db.get("users").value();
  const result = agents.map(ag => {
    const owner = users.find(u => u.id === ag.userId);
    return { ...ag, ownerEmail: owner?.email || "?", ownerName: owner?.name || "?" };
  });
  res.json(result);
});

/* ── GET /api/admin/agents/:id/calls — agent calls via Vapi ── */
app.get("/api/admin/agents/:id/calls", adminAuth, async (req, res) => {
  const ag = DB.getAgent(req.params.id);
  if (!ag || !ag.vapiId) return res.json([]);
  try {
    const data = await vapi(`/call?assistantId=${ag.vapiId}&limit=50`);
    res.json(Array.isArray(data) ? data : data?.calls || []);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ── POST /api/admin/users — create user manually ── */
app.post("/api/admin/users", adminAuth, async (req, res) => {
  try {
    const { name, email, password, plan, agentLimit } = req.body;
    if (!name || !email || !password) return res.status(400).json({ error: "name, email, password required" });
    if (DB.getUserByEmail(email)) return res.status(409).json({ error: "Email already exists" });
    const hash = await bcrypt.hash(password, 12);
    const user = DB.createUser(email, hash, name);
    const planLimits = { free:1, starter:3, pro:10, payg:999 };
    DB.updateUser(user.id, {
      plan:        plan || "free",
      agentLimit:  agentLimit || planLimits[plan] || 1,
      subStatus:   plan && plan !== "free" ? "active" : "inactive",
    });
    const { password: _, ...safe } = DB.getUser(user.id);
    res.status(201).json(safe);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* ── Serve admin panel ── */
app.get("/admin", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "admin.html"));
});

/* ── Serve frontend ── */
app.get("*", (req, res) => {
  if (req.path.startsWith("/api")) return res.status(404).json({ error: "Not found" });
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

/* ── Start ── */
app.listen(PORT, () => {
  console.log(`
╔══════════════════════════════════════════════════╗
║          VoiceForge AI  ·  Ready                 ║
╠══════════════════════════════════════════════════╣
║  Open →  http://localhost:${PORT}                    ║
║  Vapi →  YOUR key used server-side               ║
║  Auth →  Email + Password (JWT)                  ║
║  Lang →  Arabic + English                        ║
╚══════════════════════════════════════════════════╝
`);
});
