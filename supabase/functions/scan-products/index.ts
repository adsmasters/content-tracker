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
    const RAPIDAPI_KEY = Deno.env.get("RAPIDAPI_KEY")!;
    const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
    const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

    const SLACK_BOT_TOKEN = Deno.env.get("SLACK_BOT_TOKEN") || "";
    const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

    // Parse params: client_id, batch (0-based), batch_size
    let clientId: string | null = null;
    let batch = 0;
    let batchSize = 10;
    try {
      const body = await req.json();
      clientId = body.client_id || null;
      batch = body.batch || 0;
      batchSize = body.batch_size || 10;
    } catch { /* defaults */ }

    // Get active products (no join — get marketplace separately)
    let query = sb.from("ct_products").select("id, asin, client_id").eq("status", "active");
    if (clientId) query = query.eq("client_id", clientId);
    query = query.range(batch * batchSize, (batch + 1) * batchSize - 1);
    const { data: products, error: prodErr } = await query;

    if (prodErr || !products) {
      return new Response(JSON.stringify({ error: "Failed to load products", detail: prodErr?.message }), {
        status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    // Get total count for pagination info
    let countQuery = sb.from("ct_products").select("id", { count: "exact", head: true }).eq("status", "active");
    if (clientId) countQuery = countQuery.eq("client_id", clientId);
    const { count: totalCount } = await countQuery;

    // Load client info (marketplace + name for Slack channel)
    const clientIds = [...new Set(products.map((p: any) => p.client_id))];
    const { data: clients } = await sb.from("ct_clients").select("id, marketplace, name").in("id", clientIds);
    const clientMap: Record<string, { marketplace: string; name: string }> = {};
    (clients || []).forEach((c: any) => { clientMap[c.id] = { marketplace: c.marketplace || "DE", name: c.name || "" }; });

    // Collect changes per client for Slack notifications
    const changesByClient: Record<string, Array<{ asin: string; title: string; field: string }>> = {};

    let scanned = 0;
    let changesFound = 0;
    let errors = 0;

    for (const product of products) {
      try {
        const clientInfo = clientMap[product.client_id] || { marketplace: "DE", name: "" };
        const marketplace = clientInfo.marketplace.toUpperCase();
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

        if (!apiResp.ok) { errors++; continue; }

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
          const checks = [
            { field: "title", oldVal: prev.title || "", newVal: title },
            { field: "bullets", oldVal: JSON.stringify(prev.bullets || []), newVal: JSON.stringify(bulletsArr) },
            { field: "images", oldVal: JSON.stringify(prev.images || []), newVal: JSON.stringify(imagesArr) },
            { field: "a_plus", oldVal: prev.a_plus_html || "[]", newVal: JSON.stringify(aPlusArr) },
          ];

          for (const check of checks) {
            if (check.oldVal !== check.newVal) {
              await sb.from("ct_changes").insert({
                product_id: product.id,
                snapshot_id: snapId,
                field: check.field,
                old_value: check.oldVal,
                new_value: check.newVal,
              });
              changesFound++;
              // Collect for Slack
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

        // Rate limit: 300ms between requests
        await new Promise((r) => setTimeout(r, 300));

      } catch (e) {
        console.error(`Error scanning ${product.asin}:`, e);
        errors++;
      }
    }

    // Send Slack notifications per client
    if (SLACK_BOT_TOKEN && changesFound > 0) {
      const fieldLabels: Record<string, string> = { title: "Titel", bullets: "Bullet Points", images: "Bilder", a_plus: "A+ Content" };

      for (const [cId, changes] of Object.entries(changesByClient)) {
        const info = clientMap[cId];
        if (!info) continue;

        const channel = info.name.toLowerCase().replace(/\s+/g, "-").replace(/[^a-z0-9-]/g, "");
        if (!channel) continue;

        const lines = changes.map((c) =>
          `• *${c.asin}* — ${c.title ? c.title.substring(0, 60) + "..." : "N/A"} → _${fieldLabels[c.field] || c.field}_ geändert`
        );
        const text = `:rotating_light: *Content Tracker: ${changes.length} Änderung${changes.length > 1 ? "en" : ""} erkannt*\n\n${lines.join("\n")}\n\n<https://adsmasters.github.io/content-tracker/dashboard.html|→ Dashboard öffnen>`;

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

    const hasMore = (batch + 1) * batchSize < (totalCount || 0);

    return new Response(
      JSON.stringify({ scanned, changes_found: changesFound, errors, total: totalCount, batch, has_more: hasMore }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );

  } catch (e) {
    return new Response(JSON.stringify({ error: String(e) }), {
      status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
