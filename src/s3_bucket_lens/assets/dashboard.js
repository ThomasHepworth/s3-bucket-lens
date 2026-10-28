"use strict";

const byId = (id) => document.getElementById(id);
const money = (value) => value == null ? "Not measured" : new Intl.NumberFormat("en-GB", { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(value);
const count = (value) => value == null ? "Not measured" : new Intl.NumberFormat("en-GB").format(value);
const escapeHtml = (value) => String(value ?? "").replace(/[&<>"']/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[character]);
const icon = (name, className = "") => `<i data-lucide="${name}" class="${className}"></i>`;
const refreshIcons = () => window.lucide?.createIcons();
const colours = ["#2675c5", "#168c81", "#c18b32", "#7c6ca3", "#cc6666", "#648b55", "#8097a9"];
let manifest;
let directory;
let page = 1;
let sort = "cost";
let requestNumber = 0;
let selectionRequest = 0;
let selectedPath = null;
const expanded = new Map();
const MAX_EXPANDED_BRANCHES = 20;
const folderNodes = new Map();
const folderChildren = new Map();

function size(bytes) {
  if (bytes == null) return "Not measured";
  if (!bytes) return "0 B";
  const unit = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), 5);
  return `${new Intl.NumberFormat("en-GB", { maximumFractionDigits: unit ? 1 : 0 }).format(bytes / 1024 ** unit)} ${["B", "KiB", "MiB", "GiB", "TiB", "PiB"][unit]}`;
}

function date(value) {
  if (!value) return "Unknown";
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? "Unknown" : parsed.toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric", timeZone: "UTC" });
}

async function pathId(path) {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(path));
  return Array.from(new Uint8Array(digest)).map((byte) => byte.toString(16).padStart(2, "0")).join("").slice(0, 32);
}

function loadReportData(url) {
  return new Promise((resolve, reject) => {
    const script = document.createElement("script");
    const name = url.split("/").pop();
    const cleanup = () => {
      document.removeEventListener("s3-bucket-lens-data", receive);
      script.remove();
    };
    const receive = (event) => {
      if (event.detail.name !== name) return;
      cleanup();
      resolve(event.detail.data);
    };
    script.src = url;
    script.onload = script.onerror = () => {
      cleanup();
      reject(new Error(`Could not load report data: ${url}. Keep the report files together.`));
    };
    document.addEventListener("s3-bucket-lens-data", receive);
    document.head.append(script);
  });
}

function showError(error) {
  byId("error").hidden = false;
  byId("error").textContent = error.message;
}

function rules() { return manifest.bucket.lifecycle?.Rules || []; }
function rulePrefix(rule) { return rule.Filter?.Prefix ?? rule.Filter?.And?.Prefix ?? rule.Prefix ?? ""; }
function conditional(rule) {
  const filter = rule.Filter || {};
  return Object.keys(filter).some((key) => key !== "Prefix") || Boolean(rule.Expiration?.ExpiredObjectDeleteMarker);
}

function lifecycle(path) {
  if (!manifest.bucket.lifecycle) return { label: "Unavailable", className: "" };
  const enabled = rules().filter((rule) => rule.Status === "Enabled");
  if (enabled.some((rule) => path.startsWith(rulePrefix(rule)) && !conditional(rule))) return { label: "Configured", className: "enabled" };
  if (enabled.some((rule) => path.startsWith(rulePrefix(rule)) && conditional(rule))) return { label: "Conditional", className: "conditional" };
  if (enabled.some((rule) => rulePrefix(rule).startsWith(path))) return { label: "Partial", className: "partial" };
  return { label: "No matching rule", className: "" };
}

function lifecycleTag(path) {
  const status = lifecycle(path);
  return `<span class="lifecycle-tag ${status.className}">${status.label}</span>`;
}

function costBadge(node) {
  if (node.unknown_cost_objects && !node.monthly_usd) return '<span class="cost amber">Unknown</span>';
  return `<span class="cost">${money(node.monthly_usd)}</span>${node.unknown_cost_objects ? '<span class="partial" title="Some retained records have unknown storage costs">+ unpriced</span>' : ""}`;
}

function reviewPriority(node) {
  if (node.monthly_usd == null) return { level: "amber", label: "Not assessed", reason: "Folder storage and monthly cost were not measured. A red, amber or green cost assessment cannot be calculated from folder names." };
  if (node.monthly_usd > 20) return { level: "red", label: "Investigate", reason: "Known monthly storage cost is above US$20. Review storage and retention requirements." };
  if (node.unknown_cost_objects) return { level: "amber", label: "Check pricing", reason: `${count(node.unknown_cost_objects)} retained ${node.unknown_cost_objects === 1 ? "record has" : "records have"} unpriced storage. The estimate is incomplete.` };
  if (node.multipart_uploads) return { level: "amber", label: "Review uploads", reason: "Incomplete multipart uploads have storage charges excluded from this estimate." };
  if (node.monthly_usd > 5) return { level: "amber", label: "Review", reason: "Known monthly storage cost is above US$5 and at most US$20." };
  return { level: "green", label: "Routine", reason: "Known monthly storage cost is at most US$5. This is not a deletion recommendation." };
}

function reviewBadge(node) {
  const priority = reviewPriority(node);
  return `<span class="review-status ${priority.level}" title="${escapeHtml(priority.reason)}">${priority.label}</span>`;
}

function renderOverview() {
  if (manifest.folder_only) { renderFolderOverview(); return; }
  const root = manifest.root;
  const whollyUnpriced = root.unknown_cost_objects > 0 && !root.monthly_usd;
  byId("bucket-name").textContent = manifest.bucket.name;
  const encryption = manifest.bucket.encryption?.ServerSideEncryptionConfiguration?.Rules?.[0]?.ApplyServerSideEncryptionByDefault?.SSEAlgorithm || "unknown";
  const encryptionLabel = { "aws:kms": "AWS KMS", "aws:kms:dsse": "dual-layer AWS KMS", "AES256": "S3-managed keys" }[encryption] || encryption;
  byId("bucket-meta").textContent = `${manifest.bucket.region} · Versioning ${(manifest.bucket.versioning || "unknown").toLocaleLowerCase("en-GB")} · Encryption: ${encryptionLabel}${manifest.bucket.prefix_filter ? ` · Scope: ${manifest.bucket.prefix_filter}` : ""}`;
  byId("snapshot-time").textContent = `Snapshot ${date(manifest.captured_at)}`;
  byId("demo-badge").hidden = !manifest.bucket.demo;
  document.title = `${manifest.bucket.name} | S3 Bucket Lens`;
  const metric = (label, value, description, symbol, warning = false) => `<div class="metric"><div class="metric-title">${icon(symbol)}${label}</div><div class="metric-value">${value}</div><div class="metric-sub ${warning ? "warn" : ""}">${description}</div></div>`;
  byId("cost-summary").innerHTML =
    metric("Monthly cost", whollyUnpriced ? "Unknown" : money(root.monthly_usd), root.unknown_cost_objects ? `Partial estimate: ${count(root.unknown_cost_objects)} unpriced ${root.unknown_cost_objects === 1 ? "record" : "records"} excluded` : "USD · estimated storage charges", "wallet", Boolean(root.unknown_cost_objects)) +
    metric("Annual cost", whollyUnpriced ? "Unknown" : money(root.monthly_usd * 12), whollyUnpriced ? "Annual cost unavailable" : "Monthly estimate x 12 · no growth assumed", "calendar-days");
  byId("metrics").innerHTML =
    metric("Stored data", size(root.total_bytes), `${size(root.noncurrent_bytes)} in older versions`, "database") +
    metric("Current files", count(root.objects), `${count(root.versions)} stored versions in total`, "files") +
    metric("Delete markers", count(root.delete_markers), "Deletion records, not file contents", "file-x-2");
  const messages = [...(manifest.warnings || [])];
  if (root.unknown_cost_objects) messages.push(`${count(root.unknown_cost_objects)} retained records (${size(root.unknown_cost_bytes)}) have unpriced storage, including unknown Intelligent-Tiering tiers. These costs are excluded from the estimate.`);
  if (root.multipart_uploads) messages.push(`${count(root.multipart_uploads)} incomplete multipart upload(s), ${size(root.multipart_bytes)} known bytes${root.multipart_unknown_sizes ? `, ${count(root.multipart_unknown_sizes)} unknown sizes` : ""}. Multipart storage charges are excluded from the estimate.`);
  if (manifest.bucket.prefix_filter) messages.push(`Partial bucket scan: only keys starting with ${manifest.bucket.prefix_filter} were included.`);
  if (messages.length) {
    byId("warnings").hidden = false;
    byId("warnings").innerHTML = `<details><summary>${count(messages.length)} snapshot notes${manifest.bucket.demo ? " · Synthetic demonstration data" : ""}${root.unknown_cost_objects ? " · Partial cost estimate" : ""}</summary>${messages.map((message) => `<p>${escapeHtml(message)}</p>`).join("")}</details>`;
  }
  byId("rule-count").textContent = count(rules().length);
  const source = manifest.pricing.source || "Custom prices";
  byId("pricing-source").textContent = source.startsWith("https://") ? `AWS regional public prices · Retrieved ${date(manifest.pricing.retrieved_at)}` : source;
  renderLifecycle();
  renderInsights();
}

function renderFolderOverview() {
  const extraction = manifest.extraction;
  const metric = (label, value, description, symbol) => `<div class="metric"><div class="metric-title">${icon(symbol)}${label}</div><div class="metric-value">${value}</div><div class="metric-sub">${description}</div></div>`;
  byId("bucket-name").textContent = manifest.bucket.name;
  byId("bucket-meta").textContent = `Folder-only snapshot · ${count(manifest.folder_count)} indexed folders · Depth up to ${count(extraction.depth)}`;
  byId("snapshot-time").textContent = `Snapshot ${date(manifest.captured_at)}`;
  byId("demo-badge").hidden = true;
  document.title = `${manifest.bucket.name} | S3 Bucket Lens`;
  document.querySelector(".brand > .badge").textContent = "FOLDER SNAPSHOT";
  document.querySelector(".bucket-heading a").href = "folders.csv";
  byId("cost-summary").innerHTML = metric("Monthly cost", money(null), "USD · storage measurements unavailable", "wallet") + metric("Annual cost", money(null), "Requires a measured monthly estimate", "calendar-days");
  byId("metrics").innerHTML = metric("Stored data", '<span class="unmeasured">Unknown</span>', "Folder discovery does not measure recursive sizes", "database") + metric("Current files", '<span class="unmeasured">Unknown</span>', "Object metadata was not enumerated", "files") + metric("Indexed folders", count(manifest.folder_count), `${count(extraction.api_requests)} listing requests · ${Number(extraction.duration_seconds).toFixed(1)}s`, "folder-tree");
  byId("warnings").hidden = false;
  const coverage = extraction.complete_discovery_to_requested_depth ? `Indexed to the requested depth of ${count(extraction.depth)}.` : `Partial folder index: ${count(extraction.pending_prefix_pages || 0)} prefix pages remain unvisited; ${count(extraction.capped_prefixes || 0)} prefixes reached their page cap.`;
  byId("warnings").innerHTML = `<strong>${escapeHtml(coverage)}</strong><p>Recursive sizes and object counts: not measured. Folder costs and cost-review ratings cannot be calculated from paths alone. Unexpanded folders may contain further children.</p>`;
  byId("pricing-source").textContent = "Folder-only snapshot · No object or storage-price measurements";
  byId("rule-count").textContent = "Unknown";
  renderLifecycle();
  byId("storage-chart").innerHTML = '<p class="empty">Storage-class totals were not collected.</p>';
  byId("review-queue").innerHTML = '<p class="empty">Cost review is not assessed until folder storage and pricing measurements are available.</p>';
  document.querySelector("#insights > .section-heading .muted").textContent = "Measurements unavailable";
  document.querySelector(".review-key").insertAdjacentHTML("beforeend", '<span class="review-status amber">Not assessed: measurements missing</span>');
  sort = "name";
  byId("sort").value = "name";
  byId("sort").querySelector('[value="cost"]').disabled = true;
  byId("search").placeholder = "Search all indexed folders";
  byId("search").setAttribute("aria-label", "Search all indexed folders");
}

function renderBreadcrumb(path) {
  const entries = [{ label: manifest.bucket.name, path: "" }];
  for (let index = 0; index < path.length; index++) {
    if (path[index] === "/") {
      const nextPath = path.slice(0, index + 1);
      const previous = entries[entries.length - 1].path.length;
      entries.push({ label: path.slice(previous, index) || "/", path: nextPath });
    }
  }
  byId("breadcrumb").innerHTML = entries.map((entry, index) => `${index ? '<span aria-hidden="true">/</span>' : ""}${entry.path === path ? `<span>${escapeHtml(entry.label)}</span>` : `<button data-path="${escapeHtml(entry.path)}">${escapeHtml(entry.label)}</button>`}`).join("");
}

function renderRows() {
  const query = byId("search").value.toLocaleLowerCase("en-GB");
  const globalSearch = manifest.folder_only && Boolean(query);
  const matches = globalSearch ? [...folderNodes.values()].filter((node) => node.path && node.path.toLocaleLowerCase("en-GB").includes(query)) : directory.children.filter((node) => node.path.toLocaleLowerCase("en-GB").includes(query));
  const pages = globalSearch ? Math.max(1, Math.ceil(matches.length / manifest.page_size)) : directory.pages;
  page = Math.min(page, pages);
  const children = globalSearch ? matches.slice((page - 1) * manifest.page_size, page * manifest.page_size) : matches;
  byId("directory-rows").innerHTML = children.map((node) => treeRow(node, 0)).join("") || `<tr><td colspan="7" class="empty">${query ? "No matching folders." : manifest.folder_only ? "No further folders are recorded at this level. This does not establish that the folder is empty." : "No child directories. Object metadata is retained in the DuckDB snapshot."}</td></tr>`;
  byId("page-label").textContent = `Page ${count(page)} of ${count(pages)} · ${count(globalSearch ? matches.length : directory.total)} ${globalSearch ? "matching folder paths" : manifest.folder_only ? "indexed child folders" : "child prefixes"}`;
  byId("previous").disabled = page <= 1;
  byId("next").disabled = page >= pages;
  refreshIcons();
}

function treeRow(node, depth) {
  const branch = expanded.get(node.path);
  const globalSearch = manifest.folder_only && Boolean(byId("search").value);
  const label = globalSearch ? node.path : node.path.slice((node.parent_path || "").length);
  const status = lifecycle(node.path);
  const lifecycleIcon = status.className === "enabled" ? "shield-check" : status.className ? "shield-alert" : "shield-x";
  let markup = `<tr role="row" aria-level="${depth + 1}"${node.child_count ? ` aria-expanded="${Boolean(branch)}"` : ""} class="${selectedPath === node.path ? "is-selected" : ""}"><td><div class="tree-name" style="--depth:${depth}"><button class="tree-toggle" data-expand="${escapeHtml(node.path)}" aria-label="${branch ? "Collapse" : "Expand"} ${escapeHtml(label)}" aria-expanded="${Boolean(branch)}"${node.child_count && !globalSearch ? "" : " disabled"}>${icon("chevron-right")}</button><button class="folder-button" data-select="${escapeHtml(node.path)}" title="${escapeHtml(node.path)}"><span class="folder-cube">${icon(branch ? "folder-open" : "folder")}</span><span>${escapeHtml(label)}</span></button><span class="lifecycle-icon ${status.className}" title="${status.label}" aria-label="${status.label}">${icon(lifecycleIcon)}</span></div></td><td>${reviewBadge(node)}</td><td class="number">${costBadge(node)}</td><td class="number">${size(node.total_bytes)}</td><td class="number">${count(node.objects)}</td><td class="number">${count(node.delete_markers)}</td><td>${date(node.last_modified)}</td></tr>`;
  if (branch) {
    markup += branch.children.map((child) => treeRow(child, depth + 1)).join("");
    if (branch.pages > 1) markup += `<tr><td colspan="7"><div class="branch-pagination" style="--depth:${depth + 1}"><span class="muted">${escapeHtml(label)} · Page ${count(branch.page)} of ${count(branch.pages)}</span><button class="icon-button" data-branch="${escapeHtml(node.path)}" data-page="${branch.page - 1}" aria-label="Previous branch page" title="Previous branch page"${branch.page <= 1 ? " disabled" : ""}>${icon("chevron-left")}</button><button class="icon-button" data-branch="${escapeHtml(node.path)}" data-page="${branch.page + 1}" aria-label="Next branch page" title="Next branch page"${branch.page >= branch.pages ? " disabled" : ""}>${icon("chevron-right")}</button></div></td></tr>`;
  }
  return markup;
}

async function loadDirectory(path, nextPage = 1) {
  if (manifest.folder_only) {
    const node = folderNodes.get(path);
    if (!node) throw new Error("This folder is not recorded in the partial index.");
    const children = folderChildren.get(path) || [];
    return { node, children: children.slice((nextPage - 1) * manifest.page_size, nextPage * manifest.page_size), total: children.length, page: nextPage, pages: Math.max(1, Math.ceil(children.length / manifest.page_size)) };
  }
  const id = await pathId(path);
  if (manifest.directory_shard_prefix) {
    const shard = id.slice(0, manifest.directory_shard_prefix);
    const records = await loadReportData(`data/${shard}-${sort}.js`);
    const data = records[`${id}-${sort}-${nextPage}`];
    if (!data) throw new Error("This directory page is missing from the report.");
    const decode = (values) => Object.fromEntries(manifest.directory_columns.map((name, index) => [name, values[index]]));
    return { ...data, node: decode(data.node), children: data.children.map(decode) };
  }
  return loadReportData(`data/${id}-${sort}-${nextPage}.js`);
}

function retainBranch(path, data) {
  if (!expanded.has(path) && expanded.size >= MAX_EXPANDED_BRANCHES) expanded.delete(expanded.keys().next().value);
  expanded.set(path, data);
}

async function toggleBranch(path, nextPage = null) {
  if (expanded.has(path) && nextPage === null) {
    expanded.delete(path);
    renderRows();
    return;
  }
  const navigation = requestNumber;
  try {
    const data = await loadDirectory(path, nextPage || 1);
    if (navigation !== requestNumber) return;
    if (data.total) retainBranch(path, data);
    renderRows();
  } catch (error) { showError(error); }
}

async function selectDirectory(path) {
  const request = ++selectionRequest;
  const navigation = requestNumber;
  try {
    const data = await loadDirectory(path);
    if (request !== selectionRequest || navigation !== requestNumber) return;
    selectedPath = path;
    if (data.total && path !== directory.node.path && !(manifest.folder_only && byId("search").value)) retainBranch(path, data);
    renderRows();
    renderDetail(data.node);
    refreshIcons();
  } catch (error) { showError(error); }
}

function renderDetail(node) {
  if (manifest.folder_only) {
    byId("detail").hidden = false;
    const field = (label, value) => `<div><dt>${label}</dt><dd>${value}</dd></div>`;
    byId("detail").innerHTML = `<div class="detail-title"><div><p class="eyebrow">DIRECTORY DETAILS</p><h2>${escapeHtml(node.path || manifest.bucket.name)}</h2></div>${reviewBadge(node)}<div class="detail-actions"><button class="icon-button" data-path="${escapeHtml(node.path)}" aria-label="Open selected directory" title="Open selected directory">${icon("arrow-right")}</button><button class="icon-button" data-close-detail aria-label="Close directory details" title="Close directory details">${icon("x")}</button></div></div><dl class="detail-list">${field("Monthly cost", money(null))}${field("Annual cost", money(null))}${field("Stored data", size(null))}${field("Current files", count(null))}${field("Indexed child folders", count(node.child_count))}${field("Last modified", date(null))}</dl><p class="access-line">Folder path and hierarchy only. Recursive storage, cost and modification times were not collected. No indexed children does not mean the folder is empty.</p>`;
    return;
  }
  const total = node.current_bytes + node.noncurrent_bytes;
  const currentShare = total ? node.current_bytes / total * 100 : 0;
  const noncurrentShare = total ? node.noncurrent_bytes / total * 100 : 0;
  const field = (label, value) => `<div><dt>${label}</dt><dd>${value}</dd></div>`;
  byId("detail").hidden = false;
  byId("detail").innerHTML = `<div class="detail-title"><div><p class="eyebrow">DIRECTORY DETAILS</p><h2>${escapeHtml(node.path || manifest.bucket.name)}</h2></div>${reviewBadge(node)}${lifecycleTag(node.path)}<div class="detail-actions"><button class="icon-button" data-path="${escapeHtml(node.path)}" aria-label="Open selected directory" title="Open selected directory">${icon("arrow-right")}</button><button class="icon-button" data-close-detail aria-label="Close directory details" title="Close directory details">${icon("x")}</button></div></div><div class="detail-grid"><dl class="detail-list">${field("Monthly cost", costBadge(node))}${field("Stored data", size(node.total_bytes))}${field("Current files", count(node.objects))}${field("Last modified", date(node.last_modified))}${field("Oldest modification", date(node.oldest_modified))}${field("Last observed key GET", date(node.last_accessed))}${field("Stored versions", count(node.versions))}${field("Delete markers", count(node.delete_markers))}${field("Incomplete uploads", count(node.multipart_uploads))}</dl><div><h3>Current files and older versions</h3><div class="stack" role="img" aria-label="${size(node.current_bytes)} current, ${size(node.noncurrent_bytes)} noncurrent"><div class="stack-current" style="width:${currentShare}%"></div><div class="stack-noncurrent" style="width:${noncurrentShare}%"></div></div><div class="legend"><span><i class="swatch" style="background:var(--blue)"></i>Current ${size(node.current_bytes)}</span><span><i class="swatch" style="background:var(--amber)"></i>Older versions ${size(node.noncurrent_bytes)}</span></div><p class="access-line">${count(node.access_observed_objects)} of ${count(node.objects)} current keys have observed GET evidence, not version-specific access times. ${manifest.access_source ? `Source: ${escapeHtml(manifest.access_source)}.` : "No access evidence was supplied."} Modification time is not access time.</p></div></div>`;
}

async function navigate(path, nextPage = 1, updateHistory = true) {
  const request = ++requestNumber;
  byId("error").hidden = true;
  try {
    const next = await loadDirectory(path, nextPage);
    if (request !== requestNumber) return;
    directory = next;
    expanded.clear();
    selectedPath = path;
    page = nextPage;
    byId("search").value = "";
    byId("up").disabled = path === "";
    byId("directory-count").textContent = `${count(next.total)} directories`;
    renderBreadcrumb(path);
    renderRows();
    if (path) renderDetail(next.node);
    else { byId("detail").hidden = true; byId("detail").innerHTML = ""; }
    refreshIcons();
    if (updateHistory) {
      const reportUrl = new URL(location.href);
      reportUrl.hash = encodeURIComponent(path);
      history.pushState(null, "", reportUrl);
    }
  } catch (error) { if (request === requestNumber) showError(error); }
}

function renderLifecycle() {
  byId("lifecycle-rules").innerHTML = rules().map((rule) => {
    const actions = [];
    if (rule.Expiration?.Days) actions.push(`Expire current objects after ${count(rule.Expiration.Days)} days`);
    if (rule.Expiration?.Date) actions.push(`Expire current objects on ${date(rule.Expiration.Date)}`);
    if (rule.Expiration?.ExpiredObjectDeleteMarker) actions.push("Remove expired object delete markers");
    if (rule.NoncurrentVersionExpiration) actions.push(`Expire noncurrent versions after ${count(rule.NoncurrentVersionExpiration.NoncurrentDays)} days${rule.NoncurrentVersionExpiration.NewerNoncurrentVersions ? `; retain ${count(rule.NoncurrentVersionExpiration.NewerNoncurrentVersions)} newer versions` : ""}`);
    if (rule.AbortIncompleteMultipartUpload) actions.push(`Abort incomplete uploads after ${count(rule.AbortIncompleteMultipartUpload.DaysAfterInitiation)} days`);
    for (const transition of rule.Transitions || []) actions.push(`Transition current objects to ${escapeHtml(transition.StorageClass)} ${transition.Date ? `on ${date(transition.Date)}` : `after ${count(transition.Days)} days`}`);
    for (const transition of rule.NoncurrentVersionTransitions || []) actions.push(`Transition noncurrent versions to ${escapeHtml(transition.StorageClass)} after ${count(transition.NoncurrentDays)} days`);
    return `<article class="rule"><div class="rule-heading"><strong>${escapeHtml(rule.ID || "Unnamed rule")}</strong><span class="lifecycle-tag ${rule.Status === "Enabled" ? "enabled" : ""}">${escapeHtml(rule.Status)}</span></div><p>Prefix <code>${escapeHtml(rulePrefix(rule) || "(whole bucket)")}</code>${conditional(rule) ? " · Conditional filter or action" : ""}</p>${actions.map((action) => `<p>${action}</p>`).join("")}<details><summary class="muted">Rule configuration</summary><pre>${escapeHtml(JSON.stringify(rule, null, 2))}</pre></details></article>`;
  }).join("") || `<p class="empty">${manifest.bucket.lifecycle ? "No lifecycle rules were configured." : "Lifecycle configuration was absent or unavailable at extraction."}</p>`;
}

function renderInsights() {
  const root = manifest.root;
  byId("storage-chart").innerHTML = manifest.storage_classes.map((storage, index) => `<div class="storage-row"><div class="storage-label"><span>${escapeHtml(storage.storage_class)}</span><strong>${size(storage.total_bytes)}</strong></div><div class="storage-track"><div class="storage-fill" style="width:${root.total_bytes ? storage.total_bytes / root.total_bytes * 100 : 0}%;background:${colours[index % colours.length]}"></div></div><div class="muted">${count(storage.versions)} versions · ${storage.unknown_cost_objects ? "Cost unknown" : `${money(storage.monthly_usd)} / month`}</div></div>`).join("") || '<p class="empty">No stored object versions.</p>';
  const review = (value, label, detail) => `<div class="review-item"><strong>${value}</strong><p>${label}</p><div class="muted">${detail}</div></div>`;
  byId("review-queue").innerHTML = review(money(root.noncurrent_usd), "Monthly noncurrent-version storage", "Review retention requirements before changing lifecycle policies.") + review(size(root.stale_bytes), "Stored versions last modified over 90 days ago", "Modification age does not show whether data is still being read.") + review(size(root.multipart_bytes), `${count(root.multipart_uploads)} incomplete multipart upload(s)`, `${count(root.multipart_unknown_sizes)} unknown sizes; multipart costs excluded.`) + review(count(root.small_objects), "Retained objects smaller than 128 KiB", "Minimum billable sizes are included for IA and Glacier Instant Retrieval.") + review(`${count(root.access_observed_objects)} / ${count(root.objects)}`, "Current objects with observed GET evidence", "Missing evidence does not mean an object has never been accessed.");
}

document.addEventListener("click", (event) => {
  const toggle = event.target.closest("[data-expand]");
  if (toggle) { toggleBranch(toggle.dataset.expand); return; }
  const branchPage = event.target.closest("[data-branch]");
  if (branchPage) { toggleBranch(branchPage.dataset.branch, Number(branchPage.dataset.page)); return; }
  const selection = event.target.closest("[data-select]");
  if (selection) { selectDirectory(selection.dataset.select); return; }
  if (event.target.closest("[data-close-detail]")) {
    selectionRequest++;
    selectedPath = null;
    byId("detail").hidden = true;
    renderRows();
    return;
  }
  const button = event.target.closest("[data-path]");
  if (button) navigate(button.dataset.path);
  const tab = event.target.closest("[data-tab]");
  if (tab) {
    document.querySelectorAll("[data-tab]").forEach((item) => {
      item.classList.toggle("active", item === tab);
      if (item === tab) item.setAttribute("aria-current", "page");
      else item.removeAttribute("aria-current");
    });
    document.querySelectorAll(".view").forEach((view) => { view.hidden = view.id !== tab.dataset.tab; });
  }
});
byId("search").addEventListener("input", () => { if (!directory) return; if (manifest.folder_only) { page = 1; expanded.clear(); if (!byId("search").value) { navigate(directory.node.path); return; } } renderRows(); });
byId("collapse-all").addEventListener("click", () => { expanded.clear(); if (directory) renderRows(); });
byId("sort").addEventListener("change", () => { sort = byId("sort").value; if (directory) navigate(directory.node.path); });
byId("up").addEventListener("click", () => { if (directory) navigate(directory.node.parent_path || ""); });
byId("previous").addEventListener("click", () => { if (manifest?.folder_only && byId("search").value) { if (page > 1) { page--; renderRows(); } } else if (directory && page > 1) navigate(directory.node.path, page - 1); });
byId("next").addEventListener("click", () => { if (manifest?.folder_only && byId("search").value) { if (!byId("next").disabled) { page++; renderRows(); } } else if (directory && page < directory.pages) navigate(directory.node.path, page + 1); });
window.addEventListener("hashchange", () => { try { navigate(decodeURIComponent(location.hash.slice(1)), 1, false); } catch (error) { showError(error); } });

async function initialise() {
  try {
    manifest = await loadReportData("manifest.js");
    if (manifest.folder_only) {
      const data = await loadReportData("folders.js");
      folderNodes.clear(); folderChildren.clear();
      folderNodes.set("", { ...manifest.root });
      for (const folder of data.folders) {
        const node = { ...manifest.root, ...folder, child_count: 0 };
        folderNodes.set(folder.path, node);
        if (!folderChildren.has(folder.parent_path)) folderChildren.set(folder.parent_path, []);
        folderChildren.get(folder.parent_path).push(node);
      }
      for (const [path, node] of folderNodes) node.child_count = (folderChildren.get(path) || []).length;
    }
    renderOverview();
    refreshIcons();
    await navigate(decodeURIComponent(location.hash.slice(1)), 1, false);
  } catch (error) { showError(error); }
}

initialise();