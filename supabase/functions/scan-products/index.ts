import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    const RAPIDAPI_KEY = Deno.env.get("RAPIDAPI_KEY");
    const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
    const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    const SLACK_BOT_TOKEN = Deno.env.get("SLACK_BOT_TOKEN") || "";

    if (!RAPIDAPI_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
      return new Response(JSON.stringify({
        error: "Missing env vars",
        has_rapidapi: !!RAPIDAPI_KEY,
        has_url: !!SUPABASE_URL,
        has_key: !!SUPABASE_SERVICE_ROLE_KEY,
      }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }

    const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

    // Parse params
    let clientId: string | null = null;
    let scanAll = false;
    try {
      const body = await req.json();
      clientId = body.client_id || null;
      scanAll = body.scan_all === true;
    } catch { /* defaults */ }

    // Load all clients
    const { data: allClients, error: clientErr } = await sb.from("ct_clients").select("id, marketplace, name, scan_interval_days, track_bullet_order, track_image_order, track_aplus_order, retention_days");
    if (clientErr) {
      return new Response(JSON.stringify({ error: "Failed to load clients", detail: clientErr.message }), {
        status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    // If scan_all: find next client that needs scanning and scan only that one
    // Then trigger self again for the next client
    if (scanAll && !clientId) {
      const now = Date.now();
      // Find clients that need scanning: check their products' last_scanned_at
      const dueClients: string[] = [];
      for (const c of (allClients || [])) {
        const intervalMs = (c.scan_interval_days || 1) * 24 * 60 * 60 * 1000;
        const { data: oldestProduct } = await sb
          .from("ct_products")
          .select("last_scanned_at")
          .eq("client_id", c.id)
          .eq("status", "active")
          .order("last_scanned_at", { ascending: true, nullsFirst: true })
          .limit(1);
        if (oldestProduct && oldestProduct.length > 0) {
          const lastScan = oldestProduct[0].last_scanned_at ? new Date(oldestProduct[0].last_scanned_at).getTime() : 0;
          if ((now - lastScan) >= (intervalMs - 2 * 60 * 60 * 1000)) {
            dueClients.push(c.id);
          }
        }
      }

      if (dueClients.length === 0) {
        return new Response(JSON.stringify({ message: "No clients due for scanning", clients_checked: (allClients || []).length }), {
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        });
      }

      // Scan first due client
      clientId = dueClients[0];
      const remainingClients = dueClients.slice(1);

      // After responding, trigger self for remaining clients (fire-and-forget)
      if (remainingClients.length > 0) {
        // Use EdgeRuntime to call self for next client after this one finishes
        // We do this via fetch at the end, non-blocking
        setTimeout(async () => {
          try {
            await fetch(`${SUPABASE_URL}/functions/v1/scan-products`, {
              method: "POST",
              headers: {
                "Authorization": `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
                "Content-Type": "application/json",
              },
              body: JSON.stringify({ client_id: remainingClients[0], remaining: remainingClients.slice(1) }),
            });
          } catch (e) {
            console.error("Failed to trigger next client scan:", e);
          }
        }, 100);
      }
    }

    // Handle chain: if remaining clients are passed, trigger next after scanning
    let remainingClients: string[] = [];
    try {
      const body = JSON.parse(await req.clone().text());
      remainingClients = body.remaining || [];
    } catch { /* ignore */ }

    // Build client map
    const clientMap: Record<string, any> = {};
    (allClients || []).forEach((c: any) => { clientMap[c.id] = c; });

    // Get active products for this client (or all if no clientId)
    let query = sb.from("ct_products").select("id, asin, client_id, last_scanned_at").eq("status", "active");
    if (clientId) query = query.eq("client_id", clientId);
    const { data: allProducts, error: prodErr } = await query;

    if (prodErr) {
      return new Response(JSON.stringify({ error: "Failed to load products", detail: prodErr.message }), {
        status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    // Filter products based on scan_interval_days
    const now = Date.now();
    const products = (allProducts || []).filter((p: any) => {
      const client = clientMap[p.client_id];
      if (!client) return false;
      const intervalDays = client.scan_interval_days || 1;
      if (!p.last_scanned_at) return true;
      const lastScan = new Date(p.last_scanned_at).getTime();
      const intervalMs = intervalDays * 24 * 60 * 60 * 1000;
      return (now - lastScan) >= (intervalMs - 2 * 60 * 60 * 1000);
    });

    // Helper functions
    function toArray(val: any): any[] {
      if (Array.isArray(val)) return val;
      if (typeof val === "string") { try { const p = JSON.parse(val); return Array.isArray(p) ? p : []; } catch { return []; } }
      return [];
    }
    function normalizeArr(arr: any): string[] {
      return toArray(arr).map((s: any) => String(s).trim());
    }
    function normalizeUrls(arr: any): string[] {
      return toArray(arr).map((s: any) => String(s).trim().split("?")[0]);
    }
    function arraysEqual(a: string[], b: string[]): boolean {
      if (a.length !== b.length) return false;
      return a.every((v, i) => v === b[i]);
    }

    const changesByClient: Record<string, Array<{ asin: string; title: string; field: string }>> = {};
    let scanned = 0;
    let changesFound = 0;
    let errors = 0;

    for (const product of products) {
      try {
        const client = clientMap[product.client_id];
        if (!client) { errors++; continue; }
        const marketplace = (client.marketplace || "DE").toUpperCase();
        const country = marketplace === "UK" ? "GB" : marketplace;

        const apiResp = await fetch(
          `https://real-time-amazon-data.p.rapidapi.com/product-details?asin=${product.asin}&country=${country}`,
          {
            headers: {
              "x-rapidapi-host": "real-time-amazon-data.p.rapidapi.com",
              "x-rapidapi-key": RAPIDAPI_KEY,
            },
          }
        );

        if (!apiResp.ok) {
          console.error(`API error for ${product.asin}: ${apiResp.status}`);
          errors++;
          await new Promise((r) => setTimeout(r, 1000));
          continue;
        }

        const apiData = await apiResp.json();
        const d = apiData.data;
        if (!d) { errors++; continue; }

        const title = d.product_title || "";
        const bulletsArr = d.about_product || [];
        const imagesArr = d.product_photos || [];
        const aPlusArr = d.aplus_images || [];

        // Get previous snapshot
        const { data: prevSnaps } = await sb
          .from("ct_snapshots")
          .select("*")
          .eq("product_id", product.id)
          .order("fetched_at", { ascending: false })
          .limit(1);

        const prev = prevSnaps && prevSnaps.length > 0 ? prevSnaps[0] : null;

        // Insert new snapshot
        const { data: newSnap } = await sb
          .from("ct_snapshots")
          .insert({
            product_id: product.id,
            title,
            bullets: bulletsArr,
            images: imagesArr,
            a_plus_html: JSON.stringify(aPlusArr),
          })
          .select("id")
          .single();

        const snapId = newSnap?.id;

        if (prev && snapId) {
          const trackBulletOrd = !!client.track_bullet_order;
          const trackImgOrd = !!client.track_image_order;
          const trackAplusOrd = !!client.track_aplus_order;
          const oldBullets = trackBulletOrd ? normalizeArr(prev.bullets) : normalizeArr(prev.bullets).sort();
          const newBulletsNorm = trackBulletOrd ? normalizeArr(bulletsArr) : normalizeArr(bulletsArr).sort();
          const oldImages = trackImgOrd ? normalizeUrls(prev.images) : normalizeUrls(prev.images).sort();
          const newImagesNorm = trackImgOrd ? normalizeUrls(imagesArr) : normalizeUrls(imagesArr).sort();
          let oldAplus: string[] = [];
          try { oldAplus = trackAplusOrd ? normalizeUrls(prev.a_plus_html) : normalizeUrls(prev.a_plus_html).sort(); } catch { oldAplus = []; }
          const newAplusNorm = trackAplusOrd ? normalizeUrls(aPlusArr) : normalizeUrls(aPlusArr).sort();

          const checks = [
            { field: "title", changed: (prev.title || "").trim() !== title.trim(), oldVal: prev.title || "", newVal: title },
            { field: "bullets", changed: !arraysEqual(oldBullets, newBulletsNorm), oldVal: JSON.stringify(toArray(prev.bullets)), newVal: JSON.stringify(bulletsArr) },
            { field: "images", changed: !arraysEqual(oldImages, newImagesNorm), oldVal: JSON.stringify(toArray(prev.images)), newVal: JSON.stringify(imagesArr) },
            { field: "a_plus", changed: !arraysEqual(oldAplus, newAplusNorm), oldVal: prev.a_plus_html || "[]", newVal: JSON.stringify(aPlusArr) },
          ];

          for (const check of checks) {
            if (check.changed) {
              await sb.from("ct_changes").insert({
                product_id: product.id,
                snapshot_id: snapId,
                field: check.field,
                old_value: check.oldVal,
                new_value: check.newVal,
              });
              changesFound++;
              if (!changesByClient[product.client_id]) changesByClient[product.client_id] = [];
              changesByClient[product.client_id].push({ asin: product.asin, title, field: check.field });
            }
          }
        }

        await sb.from("ct_products").update({
          title,
          image_url: (d.product_photos || [])[0] || null,
          last_scanned_at: new Date().toISOString(),
        }).eq("id", product.id);

        scanned++;
        await new Promise((r) => setTimeout(r, 300));

      } catch (e) {
        console.error(`Error scanning ${product.asin}:`, e);
        errors++;
      }
    }

    // Send Slack notifications
    if (SLACK_BOT_TOKEN && changesFound > 0) {
      const fieldLabels: Record<string, string> = { title: "Titel", bullets: "Bullet Points", images: "Bilder", a_plus: "A+ Content" };

      for (const [cId, changes] of Object.entries(changesByClient)) {
        const info = clientMap[cId];
        if (!info) continue;
        const channel = info.name.toLowerCase().replace(/\s+/g, "-").replace(/[^a-z0-9-]/g, "");
        if (!channel) continue;

        const byAsin: Record<string, { title: string; fields: string[] }> = {};
        changes.forEach((c) => {
          if (!byAsin[c.asin]) byAsin[c.asin] = { title: c.title, fields: [] };
          if (!byAsin[c.asin].fields.includes(c.field)) byAsin[c.asin].fields.push(c.field);
        });

        const productCount = Object.keys(byAsin).length;
        const lines = Object.entries(byAsin).map(([asin, data]) => {
          const fieldStr = data.fields.map(f => fieldLabels[f] || f).join(", ");
          return `• *${asin}* — ${data.title ? data.title.substring(0, 50) : "N/A"} → ${fieldStr}`;
        });
        const text = `:rotating_light: *Content Tracker: ${productCount} Produkt${productCount > 1 ? "e" : ""} mit Änderungen*\n\n${lines.join("\n")}\n\n<https://adsmasters.github.io/content-tracker/dashboard.html|→ Dashboard öffnen>`;

        try {
          await fetch("https://slack.com/api/chat.postMessage", {
            method: "POST",
            headers: { "Authorization": `Bearer ${SLACK_BOT_TOKEN}`, "Content-Type": "application/json" },
            body: JSON.stringify({ channel: `#${channel}`, text, mrkdwn: true }),
          });
        } catch (e) {
          console.error(`Slack error for #${channel}:`, e);
        }
      }
    }

    // Cleanup old data
    try {
      const scanClient = clientId ? clientMap[clientId] : null;
      const clientsToClean = scanClient ? [scanClient] : (allClients || []);
      for (const c of clientsToClean) {
        const days = c.retention_days || 30;
        const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
        const { data: oldProducts } = await sb.from("ct_products").select("id").eq("client_id", c.id);
        const productIds = (oldProducts || []).map((p: any) => p.id);
        if (productIds.length > 0) {
          await sb.from("ct_changes").delete().in("product_id", productIds).lt("detected_at", cutoff);
          await sb.from("ct_snapshots").delete().in("product_id", productIds).lt("fetched_at", cutoff);
        }
      }
    } catch (e) {
      console.error("Cleanup error:", e);
    }

    // Trigger next client in chain if remaining
    if (remainingClients.length > 0) {
      try {
        await fetch(`${SUPABASE_URL}/functions/v1/scan-products`, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ client_id: remainingClients[0], remaining: remainingClients.slice(1) }),
        });
      } catch (e) {
        console.error("Failed to trigger next client:", e);
      }
    }

    return new Response(
      JSON.stringify({
        client: clientId ? (clientMap[clientId]?.name || clientId) : "all",
        scanned,
        skipped: (allProducts || []).length - products.length,
        changes_found: changesFound,
        errors,
        total: (allProducts || []).length,
        remaining_clients: remainingClients.length,
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );

  } catch (e) {
    return new Response(JSON.stringify({ error: String(e) }), {
      status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
